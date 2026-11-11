"""Long-run incident capture: bounded history, minute trends, no business calls."""
import argparse
from collections import Counter, OrderedDict, deque
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import socket
import time
import uuid

import capture


class WatchTail(capture.LogTail):
    ACTION = re.compile(r'\b(Stored|Retrieved)\s+(\d+)\s+tokens(?:\s+in\s+([\d.]+)\s+seconds)?')
    LOOKUP = re.compile(r'(?:lookup avg latency=|lookup avg latency \(ms\)[\x27\x22]?:\s*)([\d.]+)(?:ms)?')
    TIME = re.compile(r'20\d\d-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:[.,]\d+)?|\b\d\d-\d\d \d\d:\d\d:\d\d')
    def parse_line(self, text):
        values = {}
        if 'server is unhealthy' in text:
            name = 'health.degraded'
        elif 'server is healthy again' in text:
            name = 'health.recovered_log'
        elif any(s in text.lower() for s in ('resident miss', 'some keys not found during retrieve',
                                             'something went wrong when processing the retrieve')):
            name = 'retrieve.source_missing_or_failed'
        elif re.search(r'\bRecovered\s*\d*|\bBYPASS\b', text):
            name = 'retrieve.recompute_or_bypass'
        elif (m := self.ACTION.search(text)):
            name = 'native.store' if m[1] == 'Stored' else 'native.retrieve'
            values['logged_tokens'] = int(m[2])
            if m[3] is not None:
                values['host_ms'] = float(m[3])*1000
        elif (m := self.LOOKUP.search(text)):
            name, values['lookup_log_avg_ms'] = 'native.lookup', float(m[1])
        elif (m := re.search(r'Running:\s*(\d+).*?Waiting:\s*(\d+)', text)):
            name = 'native.queue'
            values.update(running=int(m[1]), waiting=int(m[2]))
            for field, pattern in (('deferred', r'Deferred:\s*(\d+)'),
                                   ('gpu_kv_percent', r'GPU KV cache usage:\s*([\d.]+)%'),
                                   ('prompt_tokens_s', r'Avg prompt throughput:\s*([\d.]+)')):
                found = re.search(pattern, text)
                if found:
                    values[field] = float(found[1])
        else:
            return None
        result = dict(event=name, values=values)
        for field, pattern in (('source_time_text', self.TIME), ('source_pid', re.compile(r'pid=(\d+)')),
                               ('source_engine', re.compile(r'Engine (\d+):'))):
            match = pattern.search(text)
            if match:
                result[field] = match[0] if field == 'source_time_text' else match[1]
        return result


class TraceTail(capture.LogTail):
    ALLOWED = {'span.begin', 'span.end', 'rpc.queue.put.begin', 'rpc.queue.put.return',
               'rpc.queue.dequeued', 'operation.result', 'worker.completion_reported',
               'device.callback.submitted', 'device.callback.completed', 'device.callback.failed',
               'transfer.enqueued_metadata', 'observation.error'}
    FIELDS = ('event', 'run_id', 'node', 'boot_id', 'process_instance', 'pid', 'thread_id',
              'mono_ns', 'monotonic_ns', 'wall_ns', 'request_id', 'generation', 'batch_id',
              'operation', 'operation_id', 'span_id', 'parent_span_id', 'future_id', 'future_call_id',
              'raw_future_id', 'wire_id', 'client_instance', 'ready', 'outcome', 'success',
              'exception_type', 'pp_stage', 'tp_rank', 'transfer_key', 'kind', 'completion_signature',
              'sequence', 'sample_rate')
    def parse_line(self, text):
        try:
            raw = json.loads(text)
            name = raw.get('event')
            natural = {'future.'+m+s for m in ('query', 'result', 'wait', 'set_result', 'set_exception', 'wait_on_stream')
                       for s in ('', '.begin')}
            if name not in self.ALLOWED | natural:
                return None
            row = {k: raw[k] for k in self.FIELDS if k in raw and type(raw[k]) in (str, int, float, bool, type(None))}
            row = {k: v for k, v in row.items() if not isinstance(v, str) or len(v) <= 512}
            if isinstance(raw.get('metadata'), dict):
                row['metadata'] = {k:v for k,v in raw['metadata'].items() if k in
                                   ('transfer_key', 'total_bytes', 'num_tokens', 'stored_count', 'retrieved_count', 'success')
                                   and type(v) in (int, float, bool, str, type(None)) and len(str(v)) <= 512}
            return row
        except (ValueError, TypeError, AttributeError):
            return dict(event='trace.invalid_line', values={})


class Boundaries:
    """Only exact process-instance IDs. Missing ends are observations, not blocking proof."""
    def __init__(self, wait_seconds=30, host_ms=1000, capacity=4096):
        self.pending, self.closed = OrderedDict(), OrderedDict()
        self.wait_seconds, self.host_ms, self.capacity = wait_seconds, host_ms, capacity
        self.loss = Counter()

    def _key(self, e, family, identity):
        if not e.get('process_instance') or identity is None:
            return None
        return (e.get('run_id'), e.get('node'), e['process_instance'], e.get('pid'),
                e.get('generation'), family, identity)

    def _close(self, key, end):
        self.closed[key] = end
        if len(self.closed) > self.capacity:
            self.closed.popitem(last=False)

    def _measure(self, key, start, end):
        a = start.get('mono_ns', start.get('monotonic_ns'))
        b = end.get('mono_ns', end.get('monotonic_ns'))
        if not isinstance(a, int) or not isinstance(b, int) or b < a:
            self.loss['unknown_or_reversed_clock'] += 1
            return []
        elapsed = (b-a)/1e9
        threshold = self.host_ms/1000 if key[-2] in ('span', 'call') else self.wait_seconds
        if elapsed < threshold:
            return []
        return [dict(reason=key[-2]+'.long_observed_interval', seconds=elapsed, begin=start, end=end,
                     meaning='host observation interval; not pure blocking/DMA or full request critical path')]

    def feed(self, e, now):
        name = e.get('event', '')
        edges = []
        if name in ('span.begin', 'span.end'):
            edges.append(('span', e.get('span_id'), name == 'span.begin'))
        if e.get('future_call_id'):
            edges.append(('call', e['future_call_id'], name.endswith('.begin')))
        if name == 'rpc.queue.put.begin':
            edges.append(('rpc', e.get('future_id'), True))
        elif name in ('future.set_result', 'future.set_exception'):
            edges.append(('rpc', e.get('future_id'), False))
        if name == 'future.query' and e.get('ready') is True:
            edges.append(('ready_to_consume', e.get('future_id'), True))
        elif name == 'future.result.begin':
            edges.append(('ready_to_consume', e.get('future_id'), False))
        alerts = []
        for family, identity, begin in edges:
            key = self._key(e, family, identity)
            if key is None:
                continue
            if begin:
                if key in self.closed:
                    if self.closed[key] is not None:
                        alerts.extend(self._measure(key, e, self.closed[key]))
                        self.closed[key] = None
                    continue
                if key in self.pending:
                    if family == 'ready_to_consume':
                        continue  # A repeated ready poll does not restart the clock.
                    self.pending.pop(key)
                    self._close(key, None)
                    self.loss['duplicate_start'] += 1
                    continue
                if len(self.pending) >= self.capacity:
                    self.pending.popitem(last=False)
                    self.loss['tracking_capacity'] += 1
                self.pending[key] = dict(event=e, seen=now, alerted=False)
            else:
                start = self.pending.pop(key, None)
                if start:
                    alerts.extend(self._measure(key, start['event'], e))
                    self._close(key, None)
                elif key not in self.closed:
                    self._close(key, e)
        return alerts

    def poll(self, now):
        alerts = []
        for key, row in self.pending.items():
            if not row['alerted'] and now-row['seen'] >= self.wait_seconds:
                row['alerted'] = True
                alerts.append(dict(reason=key[-2]+'.end_not_observed', observed_seconds=now-row['seen'],
                                   begin=row['event'], meaning='end may be missing, dropped, or outside hook window; not proof of blocking'))
        return alerts


class Engine:
    def __init__(self, out, *, pre=120, post=120, wait=30, host_ms=1000, lookup_ms=10000,
                 poll=5, evidence_bytes=128*1024*1024, trend_bytes=64*1024*1024, ring_size=2048):
        self.out = Path(out)
        if (self.out/'status.json').exists():
            raise ValueError('Use a new output directory')
        self.out.mkdir(parents=True, exist_ok=True)
        (self.out/'incidents').mkdir()
        self.pre, self.post, self.wait, self.host_ms, self.lookup_ms = pre, post, wait, host_ms, lookup_ms
        self.poll_seconds = poll
        self.ring = deque(maxlen=ring_size)
        self.boundaries = Boundaries(wait, host_ms)
        self.limits = dict(evidence=evidence_bytes, trends=trend_bytes)
        self.written, self.loss, self.reasons = Counter(), Counter(), Counter()
        self.minute, self.minute_counts, self.minute_numbers = None, Counter(), {}
        self.queues = {}
        self.incident, self.incidents, self.retained_incidents = None, 0, 0
        self.events, self.started, self.last_now = 0, time.time(), 0
        self.node = socket.gethostname()
        self.sources, self.declared_settings = [], {}

    def write(self, relative, value, category):
        data = (json.dumps(value, ensure_ascii=False, separators=(',', ':'))+'\n').encode('utf-8')
        if len(data) > 65536 or self.written[category]+len(data) > self.limits[category]:
            self.loss[category+'_dropped'] += 1
            return False
        try:
            with (self.out/relative).open('ab') as f:
                f.write(data)
            self.written[category] += len(data)
            return True
        except OSError:
            self.loss['write_errors'] += 1
            return False

    def finish_incident(self, truncated=False):
        if self.incident:
            self.write(self.incident['file'], dict(record='window_end', truncated=truncated,
                                                   triggers=dict(self.incident['triggers'])), 'evidence')
            self.incident = None

    def trigger(self, alert, now, wall):
        self.reasons[alert['reason']] += 1
        if self.incident and now >= self.incident['until']:
            self.finish_incident()
        if self.incident is None:
            self.incidents += 1
            filename = 'incidents/window-%06d.jsonl' % self.incidents
            self.incident = dict(file=filename, until=now+self.post, triggers=Counter())
            saved = self.write(filename, dict(record='window_start', wall_ns=wall, pre_seconds=self.pre,
                                       post_seconds=self.post, ring_limit=self.ring.maxlen,
                                       retained_events=len(self.ring), coverage_losses=dict(self.loss),
                                       source_scope='numeric/sanitized observations only'), 'evidence')
            if saved:
                self.retained_incidents += 1
            self.write('windows.jsonl', dict(window=filename, collector_wall_ns=wall,
                                             utc=datetime.fromtimestamp(wall/1e9, timezone.utc).isoformat(),
                                             first_reason=alert['reason'], retained=saved), 'trends')
            for observed, e in self.ring:
                if now-observed <= self.pre:
                    self.write(filename, dict(record='event', event=e), 'evidence')
            print('ANOMALY_WINDOW '+filename+' '+alert['reason']+' retained='+str(saved), flush=True)
        self.incident['triggers'][alert['reason']] += 1
        self.write(self.incident['file'], dict(record='trigger', wall_ns=wall, **alert), 'evidence')

    def flush_minute(self):
        if self.minute is not None:
            self.write('minutes.jsonl', dict(minute_utc=self.minute,
                                            minute_start_utc=datetime.fromtimestamp(self.minute*60, timezone.utc).isoformat(),
                                            counts=dict(self.minute_counts),
                                            numbers=self.minute_numbers, losses=dict(self.loss)), 'trends')
            self.minute_counts.clear()
            self.minute_numbers = {}

    def remember(self, event, now):
        if len(self.ring) == self.ring.maxlen:
            self.loss['pre_context_evicted'] += 1
        self.ring.append((now, event))
        if self.incident:
            self.write(self.incident['file'], dict(record='event', event=event), 'evidence')

    def feed(self, rows, now, wall, count_poll=True):
        minute = wall//60_000_000_000
        if self.minute is not None and minute != self.minute:
            self.flush_minute()
        self.minute = minute
        if count_poll:
            self.minute_counts['collector.polls'] += 1
        self.last_now = now
        if self.incident and now >= self.incident['until']:
            self.finish_incident()
        alerts, normal_transfers = [], {}
        while self.ring and now-self.ring[0][0] > self.pre:
            self.ring.popleft()
        for raw in rows:
            e = dict(raw, collected_wall_ns=wall, collector_node=self.node)
            self.events += 1
            lane = str((e.get('path'), e.get('endpoint'), e.get('source_pid'), e.get('source_engine')))
            name, values = e.get('event', 'unknown'), dict(e.get('values', {}))
            for series in e.get('series') or []:
                if isinstance(series, dict) and type(series.get('value')) in (int, float):
                    key = 'metric.'+str(series.get('name'))+json.dumps(series.get('labels', {}), sort_keys=True)
                    values[key] = series['value']
            self.minute_counts[name] += 1
            numbers = self.minute_numbers.setdefault(lane, {})
            for field, value in values.items():
                if type(value) in (int, float):
                    stat = numbers.setdefault(name+'.'+field, dict(n=0, sum=0, max=value, last=value))
                    stat.update(n=stat['n']+1, sum=stat['sum']+value, max=max(stat['max'], value), last=value)
            if name in ('native.store', 'native.retrieve') and values.get('host_ms', 0) < self.host_ms:
                key = lane, name
                batch = normal_transfers.setdefault(key, dict(event='context.transfer_log_batch',
                    operation=name, path=e.get('path'), source_pid=e.get('source_pid'), source_engine=e.get('source_engine'),
                    collected_wall_ns=wall, collector_node=self.node,
                    first_read_offset=e.get('read_offset'), last_read_end_offset=e.get('read_end_offset'),
                    values=dict(observed_lines=0, logged_tokens_sum=0, timed_n=0, host_ms_sum=0, host_ms_max=None)))
                batch['last_read_end_offset'] = e.get('read_end_offset')
                b = batch['values']
                b['observed_lines'] += 1
                b['logged_tokens_sum'] += values.get('logged_tokens', 0)
                if 'host_ms' in values:
                    b.update(timed_n=b['timed_n']+1, host_ms_sum=b['host_ms_sum']+values['host_ms'],
                             host_ms_max=max(b['host_ms_max'] or 0, values['host_ms']))
                continue  # Already counted in minute statistics; do not crowd out queue/health evidence.
            self.remember(e, now)
            if name in ('health.degraded', 'retrieve.source_missing_or_failed', 'retrieve.recompute_or_bypass'):
                alerts.append(dict(reason=name, event=e))
            if values.get('lookup_log_avg_ms', 0) >= self.lookup_ms:
                alerts.append(dict(reason='lookup.log_average_high', event=e,
                                   meaning='includes scheduler revisit gaps; not an RPC duration'))
            if values.get('host_ms', 0) >= self.host_ms:
                alerts.append(dict(reason=name+'.host_log_slow', event=e,
                                   meaning='logged host method duration, not pure transfer'))
            if name == 'native.queue':
                prior = self.queues.get(lane)
                blocked = values.get('running') == 0 and values.get('waiting', 0) > 0
                if blocked:
                    fresh = prior and now-prior['last'] <= max(20, 3*self.poll_seconds)
                    self.queues[lane] = dict(start=prior['start'] if fresh else now, last=now,
                                             alerted=prior['alerted'] if fresh else False, event=e)
                else:
                    self.queues.pop(lane, None)
            if name in ('log.error', 'log.rotation', 'trace.invalid_line'):
                self.loss[name] += 1
                self.queues = {k:v for k,v in self.queues.items() if v['event'].get('path') != e.get('path')}
            alerts.extend(self.boundaries.feed(e, now))
        for batch in normal_transfers.values():
            self.remember(batch, now)
        for lane, queue in list(self.queues.items()):
            if now-queue['last'] > max(20, 3*self.poll_seconds):
                self.queues.pop(lane)
            elif not queue['alerted'] and now-queue['start'] >= self.wait and queue['last']-queue['start'] >= self.wait:
                queue['alerted'] = True
                alerts.append(dict(reason='queue.no_running_with_waiting_observed', event=queue['event'],
                                   observed_seconds=queue['last']-queue['start'], meaning='log continuity only; not proof of GPU idle'))
        alerts.extend(self.boundaries.poll(now))
        for alert in alerts:
            self.trigger(alert, now, wall)

    def status(self, active=True):
        value = dict(active=active, at_wall_ns=time.time_ns(), started_wall=self.started, collector_node=self.node,
                     observed_events=self.events, incident_windows=self.incidents, reasons=dict(self.reasons),
                     retained_incident_windows=self.retained_incidents,
                     losses=dict(self.loss), tracker_losses=dict(self.boundaries.loss),
                     bytes_written=dict(self.written), byte_limits=self.limits, sources=self.sources,
                     declared_settings=self.declared_settings, settings_runtime_verified=False,
                     pre_seconds=self.pre, post_seconds=self.post, wait_seconds=self.wait,
                     host_ms=self.host_ms, lookup_ms=self.lookup_ms,
                     device_time=None, full_request_critical_paths=None, device_overhead_verified=False)
        try:
            capture.write_json(self.out/'status.json', value)
        except OSError:
            self.loss['status_write_errors'] += 1
            print('STATUS_WRITE_FAILED: completeness UNKNOWN', flush=True)
        return value

    def close(self):
        self.finish_incident(truncated=True)
        self.flush_minute()
        summary = self.status(False)
        try:
            capture.write_json(self.out/'summary.json', summary)
            (self.out/'report.md').write_text(
                '# 长压异常观察报告\n\n'+json.dumps(summary, ensure_ascii=False, indent=2)+
                '\n\n- minutes.jsonl：预算范围内的分钟趋势；windows.jsonl：异常窗口UTC索引。\n'
                '- incidents/：异常前后有界的数字证据。计数是观察事件数，不是请求数。\n'
                '- 原日志时间保留为字符串；采集机时间不等于日志事件发生时间，未自动跨机校时。\n'
                '- end_not_observed也可能是关闭/丢失hook，不能证明真实阻塞或唯一根因。\n'
                '- 缺逐请求事件时，RPC/TTFT/PP关键路径仍是NA；设备开销未验证。\n', encoding='utf-8')
        except OSError:
            self.loss['summary_write_errors'] += 1
            summary['losses'] = dict(self.loss)
        return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--log', action='append', type=Path, default=[])
    parser.add_argument('--auto-logs', action='store_true')
    parser.add_argument('--trace-dir', action='append', type=Path, default=[])
    parser.add_argument('--config', type=Path)
    parser.add_argument('--duration', type=float, default=0, help='Seconds; 0 means until Ctrl+C')
    parser.add_argument('--poll', type=float, default=5)
    parser.add_argument('--wait-seconds', type=float, default=30)
    parser.add_argument('--host-ms', type=float, default=1000)
    parser.add_argument('--lookup-ms', type=float, default=10000)
    parser.add_argument('--pre-seconds', type=float, default=120)
    parser.add_argument('--post-seconds', type=float, default=120)
    parser.add_argument('--max-evidence-bytes', type=int, default=128*1024*1024)
    parser.add_argument('--max-trend-bytes', type=int, default=64*1024*1024)
    parser.add_argument('--out', type=Path)
    args = parser.parse_args()
    if args.duration < 0 or min(args.poll, args.wait_seconds, args.host_ms, args.lookup_ms, args.pre_seconds,
                                args.post_seconds, args.max_evidence_bytes, args.max_trend_bytes) <= 0:
        parser.error('duration must be >=0; thresholds, intervals and budgets must be positive')
    config = json.loads(args.config.read_text(encoding='utf-8')) if args.config else {}
    logs = args.log+[Path(p) for p in config.get('logs', [])]
    if args.auto_logs:
        from find_logs import discover
        logs += [Path(p) for p in discover()['logs']]
    logs = list(dict.fromkeys(logs))
    if len(logs) > 64:
        parser.error('At most 64 source files are supported')
    if any(not p.is_absolute() or not p.is_file() for p in logs):
        parser.error('Use existing absolute log paths')
    if not (logs or args.trace_dir or config.get('metrics') or config.get('status')):
        parser.error('No sources found; use --log, --trace-dir or existing endpoint configuration')
    out = args.out or Path('runs')/('watch-'+time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8])
    engine = Engine(out, pre=args.pre_seconds, post=args.post_seconds, wait=args.wait_seconds,
                    host_ms=args.host_ms, lookup_ms=args.lookup_ms, poll=args.poll,
                    evidence_bytes=args.max_evidence_bytes, trend_bytes=args.max_trend_bytes)
    engine.declared_settings = {k:v for k,v in config.get('settings', {}).items() if k in capture.SETTING_KEYS}
    tails = {str(p): WatchTail(p) for p in logs}
    start = time.monotonic()
    progress, rediscover = start, start+60
    print('WATCH_STARTED '+str(out)+' duration='+('until Ctrl+C' if args.duration == 0 else str(args.duration)), flush=True)
    try:
        while args.duration == 0 or time.monotonic()-start < args.duration:
            now = time.monotonic()
            if args.auto_logs and now >= rediscover:
                from find_logs import discover
                for name in discover()['logs']:
                    if name not in tails:
                        if len(tails) >= 64:
                            engine.loss['source_capacity'] += 1
                        else:
                            tails[name] = WatchTail(Path(name))
                            engine.loss['new_source_history_not_read_bytes'] += tails[name].offset
                rediscover = now+60
            for directory in args.trace_dir:
                for path in sorted(directory.glob('trace-*.jsonl')):
                    if str(path) not in tails:
                        if len(tails) >= 64:
                            engine.loss['source_capacity'] += 1
                            continue
                        tails[str(path)] = TraceTail(path)  # Existing history is not new live evidence.
            for tail in tails.values():
                old_drops = tail.dropped_bytes
                rows = tail.read()
                if tail.dropped_bytes != old_drops:
                    engine.loss['input_skipped_bytes'] += tail.dropped_bytes-old_drops
                    engine.queues = {k:v for k,v in engine.queues.items() if v['event'].get('path') != str(tail.path)}
                engine.feed(rows, time.monotonic(), time.time_ns(), count_poll=False)
            rows = []
            for kind, reader in (('metrics', capture.metrics), ('status', capture.status_values)):
                for label, url in config.get(kind, {}).items():
                    try:
                        value = reader(url)
                        rows.append(dict(event='snapshot.'+kind, endpoint=label,
                                         values=value if isinstance(value, dict) else {},
                                         series=value[:100] if isinstance(value, list) else None))
                        if isinstance(value, list) and len(value) > 100:
                            engine.loss['metrics_series_omitted'] += len(value)-100
                    except Exception as exc:
                        rows.append(dict(event=kind+'.error', exception_type=type(exc).__name__))
            for directory in args.trace_dir:
                for path in sorted(directory.glob('status-*.json'))[:64]:
                    try:
                        if path.stat().st_size > 65536:
                            engine.loss['hook_status_oversize'] += 1
                            continue
                        raw = json.loads(path.read_text(encoding='utf-8'))
                        allowed = ('active', 'pid', 'process_instance', 'run_id', 'sample_rate', 'at',
                                   'deadline_wall', 'dropped_buffer', 'dropped_budget', 'dropped_write',
                                   'dropped_contention', 'write_errors', 'observation_errors')
                        rows.append(dict(event='snapshot.hook_status', path=str(path),
                                         hook_status={k:raw[k] for k in allowed if k in raw and type(raw[k]) in
                                                      (int, float, str, bool, type(None))}))
                    except (OSError, ValueError):
                        engine.loss['hook_status_unreadable'] += 1
            engine.feed(rows, time.monotonic(), time.time_ns())
            engine.sources = [dict(path=str(t.path), offset=t.offset, skipped_bytes=t.dropped_bytes,
                                   partial_bytes=len(t.fragment), filtered_lines=t.filtered_lines) for t in tails.values()]
            status = engine.status()
            if now >= progress:
                print('PROGRESS '+json.dumps(dict(elapsed_seconds=round(now-start), events=engine.events,
                                                  windows=engine.incidents, losses=status['losses'])), flush=True)
                progress = now+60
            delay = args.poll-(time.monotonic()-now)
            if args.duration:
                delay = min(delay, args.duration-(time.monotonic()-start))
            time.sleep(max(0, delay))
    except KeyboardInterrupt:
        print('MONITOR_STOPPED_BY_USER', flush=True)
    finally:
        print('SUMMARY\n'+json.dumps(engine.close(), ensure_ascii=False, indent=2), flush=True)


if __name__ == '__main__':
    main()
