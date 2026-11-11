"""Conservative offline analysis of native v3 traces and pp2_monitor events."""
import argparse
from collections import Counter, defaultdict
import csv
import json
import math
from pathlib import Path
import statistics


def union_ns(intervals):
    total = 0
    end = None
    for left, right in sorted(intervals):
        if right < left:
            continue
        if end is None or left > end:
            total += right - left
        elif right > end:
            total += right - end
        end = right if end is None else max(end, right)
    return total


def domain(e):
    # Legacy trace files are isolated because PID alone is not a start identity.
    return (e.get('run_id'), e.get('node'), e.get('boot_id'),
            e.get('process_instance', e.get('_source', 'legacy')), e.get('pid'))


def stamp(e):
    return e.get('mono_ns', e.get('monotonic_ns'))


def span_key(e):
    return domain(e) + (e.get('request_id'), e.get('generation'), e.get('span_id'))


def spans_from(events):
    groups = defaultdict(list)
    for e in events:
        if e.get('event') in ('span.begin', 'span.end'):
            groups[span_key(e)].append(e)
    spans, gaps = [], []
    for key, rows in groups.items():
        begin = [e for e in rows if e['event'] == 'span.begin']
        end = [e for e in rows if e['event'] == 'span.end']
        if len(begin) != 1 or len(end) != 1 or stamp(begin[0]) is None or stamp(end[0]) is None or stamp(end[0]) < stamp(begin[0]):
            gaps.append(dict(key=str(key), reason='missing/duplicate/reversed_span'))
            continue
        a, b = begin[0], end[0]
        spans.append(dict(a, start_ns=stamp(a), end_ns=stamp(b),
                          inclusive_ns=stamp(b) - stamp(a), outcome=b.get('outcome'),
                          _key=key, _domain=domain(a)))
    child_index = defaultdict(list)
    for child in spans:
        child_index[(child['_domain'], child.get('parent_span_id'), child.get('thread_id'),
                     child.get('request_id'), child.get('generation'))].append(child)
    for parent in spans:
        children = [(max(parent['start_ns'], s['start_ns']), min(parent['end_ns'], s['end_ns']))
                    for s in child_index[(parent['_domain'], parent['span_id'], parent.get('thread_id'),
                                          parent.get('request_id'), parent.get('generation'))]
                    if s['start_ns'] >= parent['start_ns'] and s['end_ns'] <= parent['end_ns']]
        parent['exclusive_ns'] = parent['inclusive_ns'] - union_ns(children)
        # Synchronous host elapsed is not thread CPU execution time.
        parent['timing_kind'] = 'host_call_elapsed_including_unknown_waits'
    return spans, gaps


def join_observed(start, arrivals, expected):
    missing = sorted(set(expected) - set(arrivals))
    if missing or not expected or any(arrivals[k] < start for k in expected):
        return dict(join_ns=None, last_branch=None, early_wait_ns=None, missing=missing)
    last = max(expected, key=lambda k: arrivals[k])
    return dict(join_ns=arrivals[last] - start, last_branch=last,
                early_wait_ns=arrivals[last] - min(arrivals[k] for k in expected), missing=[])


def native_completions(events):
    """Reuse v3 exact payload digest + transfer_key, reject ambiguous FIFO matches.

    Handler completion is a host boundary, never hardware DMA completion.
    For a transfer all observed native submissions must complete; unknown lost
    submissions are handled by coverage warnings, not invented expected counts.
    """
    callbacks = defaultdict(list)
    metadata = defaultdict(list)
    for e in events:
        name = e.get('event', '')
        if name.startswith('device.callback.'):
            callbacks[domain(e) + (e.get('kind'), e.get('completion_signature'))].append(e)
        elif name == 'transfer.enqueued_metadata':
            key = e.get('metadata', {}).get('transfer_key')
            if key is not None:
                metadata[domain(e) + (key,)].append(e)
    matched = defaultdict(list)
    submitted_by_transfer = Counter()
    ambiguous = 0
    unmatched = 0
    failed_operations = {domain(e)+(e.get('operation_id'),) for e in events
                         if e.get('event') == 'operation.result' and e.get('success') is False and e.get('operation_id') is not None}
    for key, rows in callbacks.items():
        starts = [e for e in rows if e['event'] == 'device.callback.submitted']
        ends = [e for e in rows if e['event'] == 'device.callback.completed']
        failed = [e for e in rows if e['event'] in ('device.callback.failed', 'device.callback.ambiguous')]
        for e in starts:
            transfer = e.get('transfer_key')
            if transfer is not None:
                submitted_by_transfer[domain(e) + (transfer,)] += 1
        if len(starts) != 1 or len(ends) != 1 or failed or key[-1] is None:
            if len(starts) > 1 or len(ends) > 1:
                ambiguous += 1
            else:
                unmatched += 1
            continue
        a, b = starts[0], ends[0]
        if stamp(a) is None or stamp(b) is None or stamp(b) < stamp(a):
            unmatched += 1
            continue
        transfer = a.get('transfer_key')
        if transfer is not None:
            matched[domain(a) + (transfer,)].append((a, b))
    selected = []
    rejected = Counter()
    for key, rows in metadata.items():
        if len(rows) != 1:
            ambiguous += 1
            continue
        e = rows[0]
        meta = e.get('metadata', {})
        callbacks_for_transfer = matched.get(key, [])
        operation = e.get('transfer_kind')
        kinds = {'mp.store.end': {'finish_write_by_owner'},
                 'mp.retrieve.end': {'finish_read_by_owner', 'release_read_borrow', 'finish_read_prefetched'}}.get(operation, set())
        callbacks_for_transfer = [(a, b) for a, b in callbacks_for_transfer if a.get('kind') in kinds]
        size = meta.get('total_bytes')
        if meta.get('success') is False or meta.get('partial_failure') is True or domain(e)+(e.get('operation_id'),) in failed_operations:
            rejected['explicit_failure'] += 1
        elif meta.get('success') is not True and (not isinstance(meta.get('num_tokens'), (int, float)) or meta['num_tokens'] <= 0):
            rejected['zero_tokens_or_unproven_success'] += 1
        elif not isinstance(size, (int, float)) or size <= 0:
            rejected['zero_or_unknown_bytes'] += 1
        elif not callbacks_for_transfer or len(callbacks_for_transfer) != submitted_by_transfer[key]:
            rejected['missing_or_ambiguous_callback'] += 1
        else:
            left = min(stamp(a) for a, b in callbacks_for_transfer)
            right = max(stamp(b) for a, b in callbacks_for_transfer)
            selected.append(dict(operation=operation, request_id=e.get('request_id', e.get('request')),
                                 process_instance=e.get('process_instance'), node=e.get('node'),
                                 transfer_key=key[-1], bytes=size, host_inflight_ns=right-left,
                                 callbacks=len(callbacks_for_transfer), scope='all_observed_native_handlers_completed'))
    return dict(completed_bytes=sum(r['bytes'] for r in selected), positive_transfers=len(selected),
                ambiguous=ambiguous, unmatched_callback_groups=unmatched,
                rejected=dict(rejected), transfers=selected,
                semantics='Metadata bytes associated with completed native handlers; not measured link bytes, DMA duration, or request success. Missing expected device branches remain UNKNOWN.')


def quantiles(values):
    values = sorted(v for v in values if isinstance(v, (float, int)) and math.isfinite(v))
    def q(f):
        return values[max(0, math.ceil(len(values)*f)-1)] if values else None
    return dict(n=len(values), mean=statistics.mean(values) if values else None,
                p50=q(.5), p95=q(.95), p99=q(.99), max=max(values) if values else None)


def rpc_phases(events):
    groups = defaultdict(list)
    for e in events:
        if e.get('future_id'):
            groups[domain(e)+(e['future_id'],)].append(e)
    result = []
    for key, rows in groups.items():
        def one(name):
            matches = [r for r in rows if r['event'] == name and stamp(r) is not None]
            return stamp(matches[0]) if len(matches) == 1 else None
        begin, returned, dequeued = (one(n) for n in ('rpc.queue.put.begin', 'rpc.queue.put.return', 'rpc.queue.dequeued'))
        produced = one('future.set_result')
        consumed = [stamp(r) for r in rows if r['event'] == 'future.result.begin' and stamp(r) is not None]
        lower = max(0, dequeued-returned) if returned is not None and dequeued is not None else None
        upper = dequeued-begin if begin is not None and dequeued is not None and dequeued >= begin else None
        delay = min(consumed)-produced if produced is not None and consumed and min(consumed) >= produced else None
        first = rows[0]
        result.append(dict(run_id=first.get('run_id'), node=first.get('node'),
                           process_instance=first.get('process_instance'), request_id=first.get('request_id'),
                           generation=first.get('generation'), future_id=key[-1], raw_future_id=first.get('raw_future_id'),
                           future_type=first.get('future_type'), wire_id=next((r.get('wire_id') for r in rows if r.get('wire_id') is not None), None),
                           queue_lower_ns=lower, queue_upper_ns=upper,
                           ready_observation_to_result_begin_ns=delay,
                           scope='host_observation_bounds; send/server/device_NA'))
    return result


def load_events(paths):
    events, problems, provenance = [], [], []
    for p in paths:
        p = Path(p)
        provenance.append(str(p.resolve()))
        if p.suffix == '.jsonl':
            def iterator():
                with p.open(encoding='utf-8') as f:
                    for line_no, line in enumerate(f, 1):
                        try:
                            yield json.loads(line), line_no
                        except json.JSONDecodeError:
                            problems.append(dict(source=str(p), line=line_no, reason='invalid_json_or_truncated_line'))
            rows = iterator()
        else:
            raw = json.loads(p.read_text(encoding='utf-8'))
            rows = ((e, i) for i, e in enumerate(raw.get('events', []), 1))
        for e, line in rows:
            if not isinstance(e, dict) or not isinstance(e.get('event'), str):
                problems.append(dict(source=str(p), line=line, reason='not_an_event'))
                continue
            e['_source'], e['_line'] = str(p.resolve()), line
            events.append(e)
    return events, problems, provenance


def write_csv(path, rows, columns):
    with Path(path).open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow({k: 'NA' if row.get(k) is None else
                             json.dumps(row[k], ensure_ascii=False) if isinstance(row[k], (dict, list)) else row[k]
                             for k in columns})


def benchmark_row(label, path):
    raw = json.loads(Path(path).read_text(encoding='utf-8'))
    # Standard vLLM summary; never read/emit prompt, generated_texts or errors.
    fields = ('completed', 'failed', 'duration', 'request_throughput', 'total_input_tokens',
              'total_output_tokens', 'mean_ttft_ms', 'median_ttft_ms', 'p99_ttft_ms')
    result = dict(run=label, source=str(Path(path).resolve()))
    for k in fields:
        v = raw.get(k)
        result[k] = v if type(v) in (float, int) else None
    ttfts = raw.get('ttfts')
    if isinstance(ttfts, list):
        q = quantiles([v * 1000 for v in ttfts if type(v) in (float, int)])
        result.update(ttft_samples=q['n'], p50_ttft_ms=q['p50'], p95_ttft_ms=q['p95'])
    result['scope'] = 'finite_benchmark_summary; correctness and steady_capacity not established'
    return result


def analyze(events, out, *, problems=None, provenance=None, benchmarks=(), statuses=(), manifests=()):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    if (out / 'report.md').exists():
        raise ValueError('Use a new report directory; refusing to overwrite existing report')
    spans, gaps = spans_from(events)
    native = native_completions(events)
    groups = defaultdict(list)
    for s in spans:
        groups[(s.get('run_id'), s.get('node'), s.get('process_instance'),
                s.get('operation'), s.get('pp_stage'), s.get('tp_rank'))].append(s)
    actions = []
    for (run, node, process, op, stage, rank), rows in groups.items():
        q = quantiles([s['inclusive_ns'] / 1e6 for s in rows])
        x = quantiles([s['exclusive_ns'] / 1e6 for s in rows])
        actions.append(dict(run_id=run, node=node, process_instance=process,
                            operation=op, pp_stage=stage, tp_rank=rank, count=len(rows),
                            inclusive_mean_ms=q['mean'], exclusive_mean_ms=x['mean'],
                            p50_ms=q['p50'], p95_ms=q['p95'], p99_ms=q['p99'], max_ms=q['max'],
                            failures=sum(r.get('outcome') == 'exception' for r in rows),
                            timing_kind='host_elapsed_not_CPU_or_DMA'))
    # Old observer has elapsed fields but no span identity. Do not FIFO pair retries.
    legacy = defaultdict(list)
    for e in events:
        if 'elapsed_ns' in e and e.get('schema_version') != 1:
            legacy[(domain(e), e['event'])].append(e['elapsed_ns'] / 1e6)
    for (clock, name), values in legacy.items():
        q = quantiles(values)
        actions.append(dict(run_id=clock[0], node=clock[1], process_instance=clock[3],
                            operation=name, count=q['n'], inclusive_mean_ms=q['mean'],
                            p50_ms=q['p50'], p95_ms=q['p95'], p99_ms=q['p99'], max_ms=q['max'],
                            timing_kind='legacy_host_elapsed; exclusive/blocking/operation_identity_NA'))
    # Each future invocation has a unique ID, so polls/retries/fast replies cannot FIFO match.
    future_groups = defaultdict(list)
    future_actions = defaultdict(list)
    for e in events:
        if e.get('future_call_id'):
            future_groups[domain(e) + (e['future_call_id'],)].append(e)
    for key, rows in future_groups.items():
        begin = [r for r in rows if r['event'].endswith('.begin')]
        end = [r for r in rows if not r['event'].endswith('.begin')]
        if len(begin) == len(end) == 1 and stamp(end[0]) >= stamp(begin[0]):
            name = end[0]['event']
            value = (stamp(end[0]) - stamp(begin[0])) / 1e6
            future_actions[(domain(end[0]), name)].append((value, end[0].get('outcome') == 'exception'))
    for (clock, name), values in future_actions.items():
        q = quantiles([v for v, failed in values])
        actions.append(dict(run_id=clock[0], node=clock[1], process_instance=clock[3],
                            operation=name, count=q['n'], inclusive_mean_ms=q['mean'],
                            p50_ms=q['p50'], p95_ms=q['p95'], p99_ms=q['p99'], max_ms=q['max'],
                            timing_kind='natural_future_call; actual_blocking_NA', failures=sum(f for v, f in values)))
    request_groups = defaultdict(list)
    for e in events:
        rid = e.get('request_id', e.get('request'))
        if isinstance(rid, str):
            request_groups[(domain(e), rid, e.get('generation'))].append(e)
    requests = []
    for (clock, rid, gen), rows in request_groups.items():
        requests.append(dict(run_id=clock[0], node=clock[1], process_instance=clock[3], request_id=rid,
                             generation=gen, events=len(rows),
                             observed_store_spans=len({r.get('operation_id') for r in rows if r.get('event') == 'span.begin' and str(r.get('operation', '')).startswith('STORE')}),
                             observed_retrieve_spans=len({r.get('operation_id') for r in rows if r.get('event') == 'span.begin' and str(r.get('operation', '')).startswith('RETRIEVE')}),
                             evidence='partial; generation_unknown' if gen is None else 'partial; device/scheduler_path_unverified'))
    joins = []
    for e in events:
        if e.get('event') == 'pp.join.observed' and isinstance(e.get('arrivals'), dict):
            result = join_observed(e['start_ns'], e['arrivals'], e.get('expected', []))
            joins.append(dict(request_id=e.get('request_id'), generation=e.get('generation'),
                              timing_kind='single_receiver_clock_host_arrivals', **result))
    windows = defaultdict(Counter)
    for e in events:
        wall = e.get('wall_ns')
        if isinstance(wall, int):
            for seconds in (60, 300):
                windows[(e.get('run_id'), e.get('node'), seconds, wall // (seconds*10**9))][e['event']] += 1
    window_rows = [dict(run_id=k[0], node=k[1], window_seconds=k[2], start_wall_ns=k[3]*k[2]*10**9,
                        events=sum(v.values()), counts=dict(v)) for k, v in sorted(windows.items(), key=lambda kv: str(kv[0]))]
    metric_windows = defaultdict(list)
    for e in events:
        if not isinstance(e.get('wall_ns'), int):
            continue
        values = []
        if e.get('event') == 'metrics':
            values = [(s['name'], s.get('value')) for s in e.get('series', [])]
        elif e.get('event') == 'status':
            values = list(e.get('values', {}).items())
        elif e.get('event') == 'process':
            values = [(k, e.get(k)) for k in ('rss_pages', 'cpu_ticks')]
        for name, value in values:
            if type(value) in (float, int):
                metric_windows[(e.get('run_id'), e.get('node'), e.get('endpoint'), name,
                                e['wall_ns']//(60*10**9), e.get('pid'), e.get('start_ticks'))].append(value)
    metric_rows = []
    for k, values in metric_windows.items():
        q = quantiles(values)
        metric_rows.append(dict(run_id=k[0], node=k[1], endpoint=k[2], metric=k[3],
                                start_wall_ns=k[4]*60*10**9, pid=k[5], start_ticks=k[6],
                                samples=q['n'], mean=q['mean'], min=min(values), max=max(values),
                                scope='raw_values; omitted_labels_prevent_counter_rates'))
    coverage = Counter()
    sequence_gaps = 0
    sequences = defaultdict(set)
    for e in events:
        if isinstance(e.get('sequence'), int):
            sequences[domain(e)].add(e['sequence'])
        if e.get('event') == 'observation.error':
            coverage['observation_errors_in_events'] += 1
    for values in sequences.values():
        sequence_gaps += max(values) - min(values) + 1 - len(values)
    status_rows = []
    for path in statuses:
        raw = json.loads(Path(path).read_text(encoding='utf-8'))
        status_rows.append(raw)
        for key in ('dropped_buffer', 'dropped_contention', 'dropped_budget', 'dropped_write',
                    'write_errors', 'control_errors', 'observation_errors', 'status_errors'):
            coverage[key] += raw.get(key, 0)
    run_rows = [benchmark_row(label, p) for label, p in benchmarks]
    capture_runs = []
    for path in manifests:
        raw = json.loads(Path(path).read_text(encoding='utf-8'))
        allowed = ('run_id', 'node', 'settings', 'start_wall', 'end_wall', 'samples', 'written_bytes',
                   'budget_dropped', 'tail_status', 'interrupted', 'mode')
        capture_runs.append({k: raw[k] for k in allowed if k in raw})
    (out / 'capture_runs.json').write_text(json.dumps(capture_runs, ensure_ascii=False, indent=2), encoding='utf-8')
    # Device overhead is never inferred from unrelated benchmark groups.
    run_columns = ['run', 'source', 'completed', 'failed', 'duration', 'request_throughput',
                   'total_input_tokens', 'total_output_tokens', 'ttft_samples', 'mean_ttft_ms',
                   'p50_ttft_ms', 'p95_ttft_ms', 'p99_ttft_ms', 'scope']
    write_csv(out / 'run_summary.csv', run_rows, run_columns)
    write_csv(out / 'actions.csv', actions, ['run_id', 'node', 'process_instance', 'operation', 'pp_stage', 'tp_rank', 'count',
              'inclusive_mean_ms', 'exclusive_mean_ms', 'p50_ms', 'p95_ms', 'p99_ms', 'max_ms',
              'failures', 'blocking_ms', 'device_ms', 'timing_kind'])
    write_csv(out / 'requests.csv', requests, ['run_id', 'node', 'process_instance', 'request_id',
              'generation', 'events', 'observed_store_spans', 'observed_retrieve_spans', 'ttft_ms',
              'actual_restored_tokens', 'critical_path_ms', 'evidence'])
    write_csv(out / 'stragglers.csv', joins, ['request_id', 'generation', 'join_ns', 'last_branch',
              'early_wait_ns', 'missing', 'timing_kind'])
    write_csv(out / 'windows.csv', window_rows, ['run_id', 'node', 'window_seconds', 'start_wall_ns', 'events', 'counts'])
    write_csv(out / 'metric_windows.csv', metric_rows, ['run_id', 'node', 'endpoint', 'metric',
              'start_wall_ns', 'pid', 'start_ticks', 'samples', 'mean', 'min', 'max', 'counter_rate', 'scope'])
    write_csv(out / 'transfers.csv', native['transfers'], ['operation', 'request_id', 'node',
              'process_instance', 'transfer_key', 'bytes', 'host_inflight_ns', 'callbacks', 'scope'])
    write_csv(out / 'rpc_phases.csv', rpc_phases(events), ['run_id', 'node', 'process_instance',
              'request_id', 'generation', 'future_id', 'raw_future_id', 'future_type', 'wire_id',
              'queue_lower_ns', 'queue_upper_ns', 'ready_observation_to_result_begin_ns',
              'actual_send_ns', 'server_queue_ns', 'device_ns', 'scope'])
    batches = [dict(run_id=e.get('run_id'), node=e.get('node'), process_instance=e.get('process_instance'),
                    batch_id=e.get('batch_id'), selected_members=e.get('members'), total_members=e.get('total_members'),
                    elapsed_ns=e.get('elapsed_ns'), outcome=e.get('outcome')) for e in events
               if e.get('event') == 'scheduler.batch.end']
    write_csv(out / 'batches.csv', batches, ['run_id', 'node', 'process_instance', 'batch_id',
              'selected_members', 'total_members', 'elapsed_ns', 'outcome'])
    # Separate process clock lanes start at zero; no fake synchronized cross-host view.
    origins = {}
    trace = []
    selected_spans = sorted(spans, key=lambda s: s['inclusive_ns'], reverse=True)[:2000]
    for s in selected_spans:
        key = str(s['_domain'])
        origins[key] = min(origins.get(key, s['start_ns']), s['start_ns'])
    for s in selected_spans:
        key = str(s['_domain'])
        if key not in origins or len(trace) >= 2000:
            continue
        trace.append(dict(name=s['operation'], cat='host_elapsed', ph='X',
                          pid=key, tid=s.get('thread_id', 'unknown'),
                          ts=(s['start_ns']-origins[key])/1000, dur=s['inclusive_ns']/1000,
                          args=dict(request_id=s.get('request_id'), generation=s.get('generation'),
                                    operation_id=s.get('operation_id'), device='UNKNOWN', cross_host_clock='UNALIGNED')))
    timeline = dict(traceEvents=trace, displayTimeUnit='ms',
                    warning='Each process has independent zero origin; lanes cannot establish cross-host overlap.')
    (out / 'timeline.json').write_text(json.dumps(timeline, ensure_ascii=False, indent=2), encoding='utf-8')
    summary = dict(event_count=len(events), event_counts=dict(Counter(e['event'] for e in events)),
                   complete_host_spans=len(spans), incomplete_spans=gaps, input_problems=problems or [],
                   coverage=dict(coverage), sequence_gaps=sequence_gaps, statuses=status_rows,
                   native_completion=native, provenance=provenance or [],
                   device_overhead_verified=False, customer_deployed=False,
                   device_overlap=None, actual_blocking_ns=None,
                   complete_critical_paths=0, sample_rates=sorted({e['sample_rate'] for e in events if 'sample_rate' in e}))
    (out / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    report = f'''# PP2 / LMCache 离线观察报告

输入事件 {len(events)}；完整主机调用 span {len(spans)}；缺失/歧义 span {len(gaps)}；输入损坏 {len(problems or [])}。
原件来源见 summary.json 的 provenance（历史组件探针不代表客户正式压测）。

## 可以确认的观察

- STORE/RETRIEVE 与其他主机调用见 actions.csv；exclusive 仅扣除同线程、同请求、同代的已知直接子区间并集。
- 历史 v3 保留 elapsed 原值；缺操作身份时不按 FIFO 配对多次 STATUS/STORE，也不伪造 exclusive。
- 原生回调签名＋transfer_key 关联得到 {native['positive_transfers']} 个正字节观察，元数据字节合计 {native['completed_bytes']}。这是已观察原生 handler 完成所关联的字节，不是链路字节、纯 DMA 或请求成功。
- 回调关联歧义 {native['ambiguous']} 组；未匹配 {native['unmatched_callback_groups']} 组；拒绝项 {json.dumps(native['rejected'], ensure_ascii=False)}。
- 分钟及五分钟事件趋势见 windows.csv；记录的是事件数，不能当作请求吞吐、唯一命中率或设备空泡。
- 指标/资源原值趋势见 metric_windows.csv；缺失标签不合并成 counter 吞吐，PID/start_ticks 不同的 CPU/RSS 不跨重启拼接。

## 覆盖与缺口

采样率 {summary['sample_rates'] or 'NA（历史未记录）'}；序号内部缺口 {sequence_gaps}；观察状态计数 {json.dumps(dict(coverage), ensure_ascii=False)}。
{'没有 writer status，无法确定丢失量及首尾缺口。' if not statuses else 'writer status 是每进程累计值；必须检查更新时间与进程身份，状态未回读不能认为采集完整。'}
出现丢事件、缺 rank、generation 未知、进程身份不明时，完整请求关键路径不输出；本次完整关键路径数量为 0。
PP 汇合只接收同一 receiver 时钟下显式 expected/arrivals；没有完整必要集合就 NA。现有 hooks 未覆盖该 owning runtime 接缝。
真实阻塞、MQ 实际发送/服务端排队、设备 stream 排队、纯设备工作、重新调度、跨机计算/搬运重叠均为 NA。
Future 自然调用仅计主机耗时；未额外 query/result，不因 timeout=60 或函数名含 wait 判定阻塞。
每进程时间线独立归零，跨机未校时不相减；不输出假精确跨机空泡。缺原始请求 ID 的 benchmark 无法与动作强行对齐。

## 下一步最小验证

先在客户实际旧版本计划内接入并回读加载路径，用 60 秒、1% 一致采样核查 STORE/RETRIEVE 与自然完成事件。
当前只有候选等待边，未证明单一根因；不据此修改生产优化逻辑。
取得新设备窗口后，用同版本、同输入、同缓存初态关闭/开启观察对照；吞吐影响不超过约 2% 是待验证目标，超出需降采样/禁用对应 hook。

状态：脚本本地分析；设备开销尚未验证；客户尚未部署。CPU 合同检查和历史组件探针不替代这些验收。
'''
    (out / 'report.md').write_text(report, encoding='utf-8')
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', nargs='*', type=Path, default=[])
    parser.add_argument('--trace-dir', action='append', type=Path, default=[])
    parser.add_argument('--status-dir', action='append', type=Path, default=[])
    parser.add_argument('--benchmark', action='append', default=[], metavar='LABEL=PATH')
    parser.add_argument('--capture-run', action='append', type=Path, default=[])
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--run-id', help='Select a single detailed window; legacy events without run_id excluded')
    args = parser.parse_args()
    paths = list(args.events)
    for root in args.trace_dir:
        paths.extend(sorted(root.rglob('trace-*.jsonl')))
        paths.extend(sorted(root.rglob('capture.jsonl')))
    events, problems, provenance = load_events(paths)
    if args.run_id:
        events = [e for e in events if e.get('run_id') == args.run_id]
    benchmarks = [value.split('=', 1) for value in args.benchmark]
    statuses = [p for root in args.status_dir for p in root.rglob('status-*.json')]
    result = analyze(events, args.out, problems=problems, provenance=provenance,
                     benchmarks=benchmarks, statuses=statuses, manifests=args.capture_run)
    print(json.dumps(dict(events=result['event_count'], host_spans=result['complete_host_spans'],
                         report=str((args.out / 'report.md').resolve())), ensure_ascii=False))


if __name__ == '__main__':
    main()
