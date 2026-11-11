"""One read-only terminal bundle; historical runs and current state stay separate."""
import sys
sys.dont_write_bytecode = True
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import socket
import time

from find_logs import discover
from watch import WatchTail

FIELDS = {
    'lookup_log_avg_max_ms': ('native.lookup.lookup_log_avg_ms', 'max'),
    'lookup_log_samples': ('native.lookup.lookup_log_avg_ms', 'n'),
    'running_last': ('native.queue.running', 'last'),
    'waiting_max': ('native.queue.waiting', 'max'),
    'deferred_max': ('native.queue.deferred', 'max'),
    'store_log_samples': ('native.store.logged_tokens', 'n'),
    'store_host_max_ms': ('native.store.host_ms', 'max'),
    'retrieve_log_samples': ('native.retrieve.logged_tokens', 'n'),
    'retrieve_host_max_ms': ('native.retrieve.host_ms', 'max')}
FLAGS = {'--l1-size-gb', '--l1-init-size-gb', '--l1-read-ttl-seconds', '--l1-write-ttl-seconds',
         '--tensor-parallel-size', '--pipeline-parallel-size', '--node-rank', '--nnodes',
         '--max-num-seqs', '--max-num-batched-tokens', '--max-model-len', '--gpu-memory-utilization',
         '--kv-load-failure-policy', '--distributed-executor-backend', '--num-speculative-tokens'}
BOOLS = {'--enable-prefix-caching', '--no-enable-prefix-caching', '--disable-prefix-caching', '--headless'}
BENCH = ('completed', 'failed', 'duration', 'request_throughput', 'input_throughput', 'output_throughput',
         'total_input_tokens', 'total_output_tokens', 'mean_ttft_ms', 'median_ttft_ms', 'p95_ttft_ms', 'p99_ttft_ms')


def emit(section, data):
    print(section+' '+json.dumps(data, ensure_ascii=True, separators=(',', ':')), flush=True)


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def selected_arguments(arguments):
    result = {}
    for i, arg in enumerate(arguments):
        name, sep, value = arg.partition('=')
        if name in BOOLS:
            result[name] = value if sep else True
        elif name in FLAGS:
            value = value if sep else arguments[i+1] if i+1 < len(arguments) else ''
            if re.fullmatch(r'[A-Za-z0-9_.+-]{1,100}', value):
                result[name] = value
        elif name in ('--speculative-config', '--kv-transfer-config'):
            value = value if sep else arguments[i+1] if i+1 < len(arguments) else ''
            try:
                parsed = json.loads(value)
                allowed = ('method', 'num_speculative_tokens') if name == '--speculative-config' else ('kv_connector', 'kv_role')
                result[name] = {k:parsed[k] for k in allowed if k in parsed and type(parsed[k]) in (str, int, bool)}
            except (ValueError, TypeError):
                result[name] = 'unparsed; original value withheld'
    return result


def process_snapshot(pid):
    result = dict(pid=pid)
    try:
        root = Path('/proc')/str(pid)
        stat = (root/'stat').read_text()
        parts = stat[stat.rfind(')')+2:].split()
        result.update(start_ticks=int(parts[19]), cpu_ticks=int(parts[11])+int(parts[12]),
                      minor_faults=int(parts[7]), major_faults=int(parts[9]))
        for line in (root/'status').read_text().splitlines():
            key, _, value = line.partition(':')
            if key in ('Name', 'VmRSS', 'VmHWM', 'VmLck', 'Threads'):
                result[key] = value.strip()
    except (OSError, ValueError, IndexError):
        result['unavailable'] = True
    return result


def process_parameters(pid):
    result = dict(pid=pid, phase='CURRENT_AFTER_RESTART; not historical configuration')
    try:
        root = Path('/proc')/str(pid)
        arguments = (root/'cmdline').read_bytes().decode('utf-8', errors='replace').split('\0')
        result['selected_arguments'] = selected_arguments(arguments)
        for key in ('exe', 'cwd'):
            try:
                result[key] = os.readlink(root/key)
            except OSError:
                result[key] = None
        environment = dict(item.split(b'=', 1) for item in (root/'environ').read_bytes().split(b'\0') if b'=' in item)
        selected = {}
        for key in ('KS_LMCACHE_MP_READ_POLICY', 'LMCACHE_MP_TRANSFER_MODE', 'LMCACHE_LOG_LEVEL'):
            raw = environment.get(key.encode())
            value = raw.decode('utf-8', errors='replace') if raw else None
            selected[key] = value if value is None or re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', value) else 'withheld'
        result['selected_environment'] = selected
        result['note'] = 'Missing flags/env mean unknown defaults; this does not verify loaded source/version.'
    except OSError:
        result['unavailable'] = True
    return result


def evidence_event(e):
    result = {k:e[k] for k in ('event', 'path', 'source_time_text', 'source_pid', 'source_engine',
                             'read_offset', 'read_end_offset', 'collected_wall_ns', 'values',
                             'request_id', 'generation', 'operation', 'operation_id', 'pp_stage', 'tp_rank') if k in e}
    if isinstance(result.get('values'), dict):
        result['values'] = {k:v for k,v in result['values'].items() if type(v) in (int, float, bool, type(None))}
    return result


def historical(run, scan_bytes=32*1024*1024):
    s = read_json(run/'status.json')
    emit('HISTORICAL_RUN', dict(run=str(run.resolve()), node=s.get('collector_node'), active=s.get('active'),
         monitor_seconds=round(s['at_wall_ns']/1e9-s['started_wall'], 3), reasons=s.get('reasons'),
         losses=s.get('losses'), tracker_losses=s.get('tracker_losses'),
         declared_settings=s.get('declared_settings'), settings_runtime_verified=s.get('settings_runtime_verified')))
    timeline = []
    for line in (run/'minutes.jsonl').read_text(encoding='utf-8').splitlines():
        m = json.loads(line)
        for lane, numbers in m.get('numbers', {}).items():
            values = {label:numbers[key][stat] for label,(key,stat) in FIELDS.items()
                      if key in numbers and stat in numbers[key]}
            if values:
                timeline.append(dict(utc=m.get('minute_start_utc', m.get('minute_utc')), lane=lane, **values))
    indices = set(range(min(2, len(timeline)))) | set(range(max(0, len(timeline)-2), len(timeline)))
    for key in ('lookup_log_avg_max_ms', 'waiting_max', 'store_host_max_ms', 'retrieve_host_max_ms'):
        indices.update(sorted(range(len(timeline)), key=lambda i:timeline[i].get(key, -1), reverse=True)[:4])
    for i in sorted(indices):
        emit('MINUTE', timeline[i])
    emit('MINUTE_SELECTION', dict(total_rows=len(timeline), printed_rows=len(indices),
                                  scope='first/last and largest values; lookup values are log averages, not request maxima'))
    worst, scanned = {}, 0
    files = sorted((run/'incidents').glob('*.jsonl'))
    for path in files:
        remaining = scan_bytes-scanned
        if remaining <= 0:
            break
        with path.open('rb') as f:
            raw = f.read(remaining)
        scanned += len(raw)
        events = []
        for line in raw.splitlines():
            try:
                t = json.loads(line)
            except ValueError:
                continue
            if t.get('record') == 'event':
                events.append(t.get('event', {}))
            elif t.get('record') == 'trigger':
                e = t.get('event') or t.get('end') or t.get('begin') or {}
                v = e.get('values', {})
                score = max(v.get('lookup_log_avg_ms', 0), v.get('host_ms', 0),
                            t.get('seconds', 0)*1000, t.get('observed_seconds', 0)*1000)
                reason = t['reason']
                if reason not in worst or score > worst[reason]['score_ms']:
                    worst[reason] = dict(reason=reason, score_ms=score, file=str(path),
                        trigger_wall_ns=t.get('wall_ns'), event=evidence_event(e),
                        begin=evidence_event(t.get('begin', {})), end=evidence_event(t.get('end', {})))
        queues = [e for e in events if e.get('event') == 'native.queue']
        for value in worst.values():
            if value['file'] != str(path):
                continue
            at = value['trigger_wall_ns'] or 0
            selected = sorted(queues, key=lambda e:abs(e.get('collected_wall_ns', 0)-at))[:3]
            if queues:
                selected += [queues[0], queues[-1], max(queues, key=lambda e:e.get('values', {}).get('waiting', 0))]
            unique = {json.dumps(evidence_event(e), sort_keys=True):evidence_event(e) for e in selected}
            value['queue_context'] = list(unique.values())
    for reason in sorted(worst):
        emit('WORST', worst[reason])
    emit('HISTORY_SCAN', dict(bytes_read=scanned, evidence_bytes_total=sum(p.stat().st_size for p in files),
                              limit_bytes=scan_bytes, all_evidence_scanned=scanned == sum(p.stat().st_size for p in files)))


def memory_snapshot():
    result = dict(phase='CURRENT_AFTER_RESTART; not historical pressure')
    try:
        result['proc_meminfo_host_view'] = {k:v.strip() for line in Path('/proc/meminfo').read_text().splitlines()
            for k,_,v in [line.partition(':')] if k in ('MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree', 'Dirty', 'Writeback')}
    except OSError:
        result['proc_meminfo_host_view'] = None
    for name in ('memory.current', 'memory.max', 'memory.swap.current', 'memory.events'):
        try:
            result[name] = (Path('/sys/fs/cgroup')/name).read_text()[:1024].strip()
        except OSError:
            result[name] = None
    return result


def benchmark_candidates(base):
    paths = set()
    for directory in (base, base/'results', base/'benchmark', base/'benchmarks', base/'output'):
        if directory.is_dir():
            paths.update(directory.glob('*.json'))
            if directory != base:
                paths.update(directory.glob('*/*.json'))
    found = 0
    for p in sorted(paths, key=lambda p:p.stat().st_mtime, reverse=True)[:40]:
        try:
            if p.stat().st_size > 4*1024*1024:
                continue
            data = read_json(p)
            if not isinstance(data, dict) or 'completed' not in data or not any(k in data for k in ('duration', 'request_throughput')):
                continue
            emit('BENCHMARK_CANDIDATE', dict(file=str(p.resolve()), modified_utc=datetime.fromtimestamp(p.stat().st_mtime, timezone.utc).isoformat(),
                 phase='UNASSIGNED: operator must identify the run',
                 values={k:data[k] for k in BENCH if type(data.get(k)) in (int, float)}))
            found += 1
        except (OSError, ValueError):
            continue
    emit('BENCHMARK_SEARCH', dict(candidates_found=found, scope='known result directories, newest 40 small JSON files only',
         missing_action='If absent, paste the final numeric benchmark summary or provide its actual file path.'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='append', type=Path, default=[])
    parser.add_argument('--base', type=Path, default=Path(__file__).resolve().parent.parent)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent
    emit('BEGIN_READ_ONLY_CASE', dict(node=socket.gethostname(), current_utc=datetime.now(timezone.utc).isoformat(),
          note='No service requests, device imports, restarts, config changes or output files. Historical and current data are separate.'))
    runs = args.run or sorted([p for p in (repo/'runs').glob('*') if (p/'status.json').is_file()],
                              key=lambda p:read_json(p/'status.json')['started_wall'], reverse=True)[:3]
    for run in runs:
        try:
            historical(run)
        except (OSError, ValueError, KeyError) as exc:
            emit('HISTORY_UNAVAILABLE', dict(run=str(run), error_type=type(exc).__name__))
    discovery = discover()
    emit('CURRENT_LOG_DESTINATIONS', discovery)
    roles = {p['pid']:p['role'] for p in discovery['processes']}
    pids = sorted(roles)[:64]
    before = {p:process_snapshot(p) for p in pids}
    started = time.monotonic()
    if pids:
        time.sleep(1)
    elapsed = time.monotonic()-started
    for pid in pids:
        row = process_snapshot(pid)
        row['role'] = roles[pid]
        a = before[pid]
        if not a.get('unavailable') and not row.get('unavailable') and a['start_ticks'] == row['start_ticks']:
            row['cpu_pct_one_core_basis'] = round((row['cpu_ticks']-a['cpu_ticks'])/os.sysconf('SC_CLK_TCK')/elapsed*100, 2)
            row['minor_faults_delta'] = row['minor_faults']-a['minor_faults']
            row['major_faults_delta'] = row['major_faults']-a['major_faults']
        emit('CURRENT_PROCESS', row)
        emit('CURRENT_PARAMETERS', process_parameters(pid))
    emit('CURRENT_MEMORY', memory_snapshot())
    for p in discovery['logs'][:8]:
        try:
            path = Path(p)
            size = path.stat().st_size
            with path.open('rb') as f:
                f.seek(max(0, size-128*1024))
                if size > 128*1024:
                    f.readline()
                data = f.read(128*1024)
            tail = WatchTail(path)
            parsed = [v for line in data.decode('utf-8', errors='replace').splitlines() if (v := tail.parse_line(line))]
            emit('CURRENT_LOG_TAIL_NUMERIC_ONLY', dict(path=p, file_size=size, parsed_tail_rows=len(parsed), latest=parsed[-6:]))
        except OSError:
            emit('CURRENT_LOG_UNREADABLE', dict(path=p))
    benchmark_candidates(args.base)
    emit('END_READ_ONLY_CASE', dict(required_operator_note='Current pool size and whether vLLM was restarted; absent benchmark summary still needs operator input.'))


if __name__ == '__main__':
    main()
