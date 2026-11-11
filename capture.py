"""Read-only, bounded metrics/log collector and explicit observation control."""
import argparse
import ast
import importlib.metadata
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.parse
import urllib.request
import uuid

SOURCE_FILES = (
    'integration/vllm/lmcache_mp_connector.py',
    'integration/vllm/vllm_multi_process_adapter.py',
    'v1/multiprocess/futures.py',
    'v1/multiprocess/transport/zmq_impl/mq.py',
    'v1/multiprocess/modules/lookup.py',
    'v1/multiprocess/modules/lmcache_driven_transfer.py',
    'v1/distributed/l1_manager.py',
    'v1/multiprocess/native_completion.py',
)
SETTING_KEYS = {'model', 'tp', 'pp', 'dp', 'layers', 'mtp', 'apc', 'pool_gib',
                'read_policy', 'transfer_mode', 'dataset_manifest', 'request_rate',
                'output_length', 'warmup', 'cache_initial_state', 'has_d', 'version',
                'role', 'concurrency', 'request_count', 'case_origin'}


def inspect_sources(root=None, overrides=None):
    if root is None:
        roots = [Path(p or '.') for p in sys.path if (Path(p or '.') / 'lmcache').is_dir()]
        root = roots[0] if roots else Path('/__lmcache_not_found__')
    root = Path(root)
    package = root if root.name == 'lmcache' else root / 'lmcache'
    files = []
    symbols = set()
    for relative in SOURCE_FILES:
        path = Path((overrides or {}).get(relative, package / relative)).resolve()
        row = dict(path=str(path), exists=path.is_file(), methods=[])
        if path.is_file():
            try:
                tree = ast.parse(path.read_text(encoding='utf-8'))
                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        row['methods'].append(dict(name=node.name, signature=ast.unparse(node.args), line=node.lineno))
                        symbols.add(node.name)
            except (SyntaxError, OSError, UnicodeError) as exc:
                row['error'] = type(exc).__name__
        files.append(row)
    policy = ('both_available_runtime_unknown' if {'borrow_read', 'unsafe_read'} <= symbols else
              'transfer_borrow_candidate' if 'borrow_read' in symbols else
              'legacy_ttl_candidate' if 'unsafe_read' in symbols else 'unknown')
    versions = {}
    for name in ('lmcache', 'vllm', 'lmcache-ascend', 'vllm-ascend'):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return dict(python=sys.executable, versions=versions, source_root=str(root), files=files,
                read_policy=policy, runtime_path_verified=False,
                note='AST only; no framework imports. Live hook capabilities prove actual loaded paths, not this probe.',
                gaps=['wire enqueue/send/receive and server dispatch queue not safely instrumented',
                      'device execution/stream wait require separately authorized profiler',
                      'generation, PP/TP identities, expected rank set require owning runtime evidence',
                      'engine_driven transfer hooks unsupported; metrics/logs still available'])


# Only fixed numeric native messages; never copy arbitrary lines or exceptions.
PATTERNS = (
    ('native.retrieved', re.compile(r'\bRetrieved\s+(\d+)\s+tokens\b'), ('tokens',)),
    ('native.stored', re.compile(r'\bStored\s+(\d+)\s+tokens\s+in\s+([\d.]+)\s+seconds'), ('tokens', 'host_seconds')),
    ('native.queue', re.compile(r'Running:\s*(\d+).*?Waiting:\s*(\d+)'), ('running', 'waiting')),
    ('native.lookup_latency', re.compile(r'lookup avg latency=([\d.]+)ms'), ('elapsed_ms',)),
)


class LogTail:
    def __init__(self, path, from_start=False):
        self.path = Path(path)
        self.offset = 0
        self.identity = None
        self.fragment = b''
        self.dropped_bytes = 0
        self.filtered_lines = 0
        try:
            st = self.path.stat()
            self.identity = (st.st_dev, st.st_ino)
            self.offset = 0 if from_start else st.st_size
        except OSError:
            pass

    def read(self, max_read=256 * 1024):
        rows = []
        try:
            st = self.path.stat()
            identity = (st.st_dev, st.st_ino)
            if identity != self.identity or st.st_size < self.offset:
                rows.append(dict(event='log.rotation', path=str(self.path), offset=self.offset))
                self.identity, self.offset, self.fragment = identity, 0, b''
            with self.path.open('rb') as f:
                # Bound collector lag/memory; explicitly count skipped data.
                if st.st_size - self.offset > max_read:
                    skip = st.st_size - self.offset - max_read
                    self.dropped_bytes += skip
                    self.offset += skip
                    self.fragment = b''
                    f.seek(self.offset)
                    skipped = f.readline(max_read)
                    self.offset += len(skipped)
                    self.dropped_bytes += len(skipped)
                f.seek(self.offset)
                start = self.offset
                data = f.read(max_read)
                self.offset += len(data)
            chunks = (self.fragment + data).split(b'\n')
            self.fragment = chunks.pop()
            if len(self.fragment) > 65536:
                self.dropped_bytes += len(self.fragment)
                self.fragment = b''
            for line in chunks:
                text = line.decode('utf-8', errors='replace')
                for event, pattern, keys in PATTERNS:
                    match = pattern.search(text)
                    if match:
                        rows.append(dict(event=event, path=str(self.path), read_offset=start,
                                         values={k: float(v) for k, v in zip(keys, match.groups())},
                                         time_kind='collector_observation_not_original_timestamp'))
                        break
                else:
                    self.filtered_lines += 1
        except OSError as exc:
            rows.append(dict(event='log.error', path=str(self.path), error_type=type(exc).__name__))
        return rows


def read_url(url, timeout=2, max_bytes=1024 * 1024):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ('http', 'https') or parsed.username or parsed.password or parsed.query:
        raise ValueError('Use an existing read-only metrics URL without credentials/query')
    # No proxy credentials or environment dumps; GET only. No PING/inference RPC.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=timeout) as response:
        raw = response.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError('metrics response exceeds budget')
    return raw


def metrics(url, timeout=2, max_bytes=1024 * 1024):
    raw = read_url(url, timeout, max_bytes)
    values = []
    for line in raw.decode('utf-8', errors='replace').splitlines():
        match = re.match(r'^((?:vllm|lmcache)[\w:]*)(?:\{(.*?)\})?\s+([-+\d.eE]+)(?:\s|$)', line)
        if match:
            labels = dict(re.findall(r'(le|quantile|pp_stage|tp_rank)="([\d.+eE-]+)"', match[2] or ''))
            values.append(dict(name=match[1], value=float(match[3]), labels=labels,
                               other_labels_omitted=bool(match[2])))
    return values


def status_values(url):
    raw = json.loads(read_url(url))
    allowed = {'active_sessions', 'pending_results', 'completed_results', 'pending_stores',
               'pending_retrieves', 'read_locks', 'borrows', 'staging_objects', 'resident_objects',
               'allocated_bytes', 'used_bytes', 'capacity_bytes', 'num_l2_adapters', 'healthy'}
    sections = {'stats', 'status', 'cache', 'storage', 'l1', 'lookup', 'requests', 'memory', 'health'}
    result = {}
    def visit(value, prefix='', depth=0):
        if not isinstance(value, dict) or depth > 4:
            return
        for k, v in value.items():
            if k in allowed and type(v) in (int, float, bool):
                result[prefix+k] = v
            elif k in sections:
                visit(v, prefix+k+'.', depth+1)
    visit(raw)
    return result


def proc_identity(pid):
    result = dict(pid=pid, start_ticks=None, rss_pages=None, cpu_ticks=None)
    try:
        stat = Path('/proc/%d/stat' % pid).read_text()
        parts = stat[stat.rfind(')') + 2:].split()
        result.update(start_ticks=int(parts[19]), rss_pages=int(parts[21]),
                      cpu_ticks=int(parts[11]) + int(parts[12]))
    except (OSError, ValueError, IndexError):
        result['available'] = False
    return result


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(path)


def record(config, out, duration, interval=5, max_bytes=64 * 1024 * 1024):
    if duration <= 0 or interval <= 0 or max_bytes <= 0:
        raise ValueError('duration, interval and max-bytes must be positive')
    if not any(config.get(k) for k in ('metrics', 'status', 'logs', 'pids')):
        raise ValueError('Configure at least one existing metrics/status URL, log path or process PID')
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    if (out / 'run.json').exists():
        raise ValueError('Use a new output directory; refusing to overwrite a run')
    run_id = config.get('run_id', uuid.uuid4().hex)
    manifest = dict(schema_version=1, run_id=run_id, node=config.get('node'),
                    start_wall=time.time(), duration=duration, interval=interval,
                    max_bytes=max_bytes, mode='read_only_metrics_incremental_numeric_logs',
                    settings={k: v for k, v in config.get('settings', {}).items() if k in SETTING_KEYS},
                    sources=config.get('logs', []),
                    device_overhead_verified=False, customer_deployed=False,
                    gaps=['No new detailed hooks installed by record', 'No device profiler or device utilization collected'])
    write_json(out / 'run.json', manifest)
    tails = [LogTail(p) for p in config.get('logs', [])]
    end = time.monotonic() + duration
    written = 0
    samples = 0
    budget_dropped = 0
    interrupted = False
    try:
        with (out / 'capture.jsonl').open('x', encoding='utf-8') as f:
            while time.monotonic() < end:
                now = time.monotonic()
                rows = []
                for name, url in config.get('metrics', {}).items():
                    try:
                        rows.append(dict(event='metrics', endpoint=name, series=metrics(url)))
                    except Exception as exc:
                        rows.append(dict(event='metrics.error', endpoint=name, error_type=type(exc).__name__))
                for name, url in config.get('status', {}).items():
                    try:
                        rows.append(dict(event='status', endpoint=name, values=status_values(url)))
                    except Exception as exc:
                        rows.append(dict(event='status.error', endpoint=name, error_type=type(exc).__name__))
                for tail in tails:
                    rows.extend(tail.read())
                for pid in config.get('pids', []):
                    rows.append(dict(event='process', **proc_identity(int(pid))))
                for row in rows:
                    row.update(run_id=run_id, node=config.get('node'), wall_ns=time.time_ns(),
                               mono_ns=time.monotonic_ns(), sample=samples)
                    data = json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n'
                    size = len(data.encode())
                    if written + size > max_bytes:
                        budget_dropped += 1
                    else:
                        f.write(data)
                        written += size
                f.flush()
                samples += 1
                if written >= max_bytes or budget_dropped:
                    break
                time.sleep(max(0, min(interval - (time.monotonic() - now), end - time.monotonic())))
    except KeyboardInterrupt:
        interrupted = True
    finally:
        manifest.update(end_wall=time.time(), written_bytes=written, samples=samples,
                        budget_dropped=budget_dropped, interrupted=interrupted,
                        tail_status=[dict(path=str(t.path), offset=t.offset, dropped_bytes=t.dropped_bytes,
                                          filtered_lines=t.filtered_lines, partial_bytes=len(t.fragment)) for t in tails])
        write_json(out / 'run.json', manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    inspect_p = sub.add_parser('inspect')
    inspect_p.add_argument('--source-root', type=Path)
    inspect_p.add_argument('--config', type=Path)
    inspect_p.add_argument('--out', type=Path, required=True)
    rec = sub.add_parser('record')
    rec.add_argument('--config', type=Path, required=True)
    rec.add_argument('--out', type=Path, required=True)
    rec.add_argument('--duration', type=float, default=300)
    rec.add_argument('--interval', type=float, default=5)
    rec.add_argument('--max-bytes', type=int, default=67108864)
    arm = sub.add_parser('arm')
    arm.add_argument('--control', type=Path, required=True)
    arm.add_argument('--duration', type=float, default=60)
    arm.add_argument('--sample-rate', type=float, default=.01)
    arm.add_argument('--run-id', required=True)
    arm.add_argument('--seed', default='ks-pp-v1')
    stop = sub.add_parser('disarm')
    stop.add_argument('--control', type=Path, required=True)
    status_p = sub.add_parser('status')
    status_p.add_argument('--dir', type=Path, required=True)
    status_p.add_argument('--max-age', type=float, default=3)
    status_p.add_argument('--run-id')
    args = parser.parse_args()
    if args.command == 'inspect':
        config = json.loads(args.config.read_text(encoding='utf-8')) if args.config else {}
        write_json(args.out, inspect_sources(args.source_root or config.get('source_root'), config.get('source_overrides')))
    elif args.command == 'record':
        config = json.loads(args.config.read_text(encoding='utf-8'))
        record(config, args.out, args.duration, args.interval, args.max_bytes)
    elif args.command == 'arm':
        if not 0 < args.duration <= 3600 or not 0 <= args.sample_rate <= 1:
            parser.error('duration must be (0,3600], sample-rate [0,1]')
        write_json(args.control, dict(enabled=True, deadline_wall=time.time() + args.duration,
                                     sample_rate=args.sample_rate, seed=args.seed, run_id=args.run_id))
    elif args.command == 'disarm':
        try:
            config = json.loads(args.control.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            config = {}
        config.update(enabled=False, deadline_wall=0)
        write_json(args.control, config)
    else:
        rows = []
        for path in args.dir.glob('status-*.json'):
            try:
                row = json.loads(path.read_text(encoding='utf-8'))
                if args.run_id and row.get('run_id') != args.run_id:
                    continue
                row['fresh'] = time.time() - row.get('at', 0) <= args.max_age
                row['status_file'] = str(path)
                rows.append(row)
            except (OSError, ValueError):
                rows.append(dict(status_file=str(path), fresh=False, error='unreadable_status'))
        print(json.dumps(dict(processes=rows, fresh_processes=sum(r['fresh'] for r in rows),
                              warning='Compare expected worker/cache PID inventory; absent processes remain UNKNOWN.'), indent=2))


if __name__ == '__main__':
    main()
