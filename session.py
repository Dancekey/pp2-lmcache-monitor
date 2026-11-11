"""Keep a server-local evidence session alongside the existing read-only watch.

No serving imports, service control, hook injection, or inference requests.
Raw files intentionally retain their original content; do not publish them.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
import uuid

from capture import read_url, write_json
from collect_case import memory_snapshot, process_parameters, process_snapshot
from find_logs import discover

MIB = 1024 * 1024


def stamp():
    return dict(wall_ns=time.time_ns(), mono_ns=time.monotonic_ns())


def small_file(path, limit=MIB):
    try:
        with Path(path).open('rb') as f:
            data = f.read(limit + 1)
        return dict(text=data[:limit].decode('utf-8', errors='replace'),
                    truncated=len(data) > limit)
    except OSError as exc:
        return dict(error=type(exc).__name__)


class Archive:
    """Append-only copies with source offsets; keep renamed open files alive."""
    def __init__(self, out, budget, reserve):
        self.out = Path(out)
        self.out.mkdir()
        self.budget, self.reserve, self.written = budget, reserve, 0
        self.streams, self.current, self.errors = [], {}, []

    def error(self, path, error):
        row = dict(path=str(path), error=error)
        if row not in self.errors:
            self.errors.append(row)

    def add(self, path):
        path = Path(path).resolve()
        key = str(path)
        try:
            s = path.stat()
            if not stat.S_ISREG(s.st_mode):
                self.error(path, 'not_regular_file')
                return
            old = self.current.get(key)
            identity = (s.st_dev, s.st_ino)
            if old and old['identity'] == identity:
                if s.st_size < old['offset']:
                    self.error(path, 'copytruncate_unread_bytes_may_be_lost')
                    old['closed'] = True
                    old['reader'].close()
                else:
                    return
            if len(self.streams) >= 256:
                self.error(path, 'source_generation_limit_256')
                return
            reader = path.open('rb')
            actual = os.fstat(reader.fileno())
            if not stat.S_ISREG(actual.st_mode):
                reader.close()
                self.error(path, 'not_regular_file_after_open')
                return
            item = dict(source=key, identity=(actual.st_dev, actual.st_ino),
                        archive='raw/%06d.bin' % len(self.streams), offset=0,
                        reader=reader, closed=False, opened=stamp(), initial_bytes=actual.st_size)
            self.streams.append(item)
            self.current[key] = item
        except OSError as exc:
            self.error(path, type(exc).__name__)

    def poll(self):
        for item in self.streams:
            if item['closed']:
                continue
            try:
                remaining = min(MIB, self.budget-self.written,
                                shutil.disk_usage(self.out).free-self.reserve)
                if remaining <= 0:
                    continue
                reader = item['reader']
                reader.seek(item['offset'])
                data = reader.read(remaining)
                if data:
                    with (self.out.parent/item['archive']).open('ab') as f:
                        before = f.tell()
                        try:
                            count = f.write(data)
                            f.flush()
                        except OSError:
                            f.seek(before)
                            f.truncate()
                            raise
                    item['offset'] += count
                    self.written += count
                    item['last_copy'] = stamp()
                # The next poll rechecks a renamed descriptor before retirement.
            except OSError as exc:
                self.error(item['source'], type(exc).__name__)

    def state(self):
        rows = []
        for item in self.streams:
            row = {k:v for k,v in item.items() if k not in ('reader', 'closed')}
            try:
                row['observed_source_bytes'] = os.fstat(item['reader'].fileno()).st_size
                row['pending_bytes'] = max(0, row['observed_source_bytes']-item['offset'])
            except (OSError, ValueError):
                row['pending_bytes'] = None
            row['source_start_offset'] = 0
            row['source_end_offset_exclusive'] = item['offset']
            rows.append(row)
        return dict(files=rows, errors=self.errors, written_bytes=self.written,
                    budget_bytes=self.budget, reserve_bytes=self.reserve,
                    budget_exhausted=self.written >= self.budget,
                    free_bytes=shutil.disk_usage(self.out).free,
                    note='Offsets are bytes. Copy time is not event time. Original files are never removed.')

    def close(self):
        for item in self.streams:
            item['reader'].close()


def resources(pids, detailed=False):
    mem = memory_snapshot()
    mem.pop('phase', None)
    rows = []
    for pid in sorted(pids):
        row = process_snapshot(pid)
        if detailed:
            row['cgroup_membership'] = small_file('/proc/%s/cgroup' % pid, 65536)
            row['smaps_rollup'] = small_file('/proc/%s/smaps_rollup' % pid, 65536)
            raw = small_file('/proc/%s/numa_maps' % pid)
            counts = {}
            for line in raw.get('text', '').splitlines():
                page = re.search(r'kernelpagesize_kB=(\d+)', line)
                size = page[1] if page else 'unknown'
                for node, pages in re.findall(r'\b(N\d+)=(\d+)', line):
                    key = node + ':page_kib=' + size
                    counts[key] = counts.get(key, 0) + int(pages)
            row['numa_resident_pages'] = dict(counts=counts,
                                             truncated=raw.get('truncated'), error=raw.get('error'))
        rows.append(row)
    return dict(**stamp(), processes=rows, memory=mem,
                cpu=small_file('/proc/stat', 65536), vm=small_file('/proc/vmstat', 65536),
                network_container_view=small_file('/proc/net/dev', 65536),
                pressure={k:small_file('/proc/pressure/'+k, 65536) for k in ('cpu','memory','io')},
                note='Cumulative counters need same-identity deltas. Host and container views differ. RSS is not pool residency.')


def result_candidates(pids, roots, started_ns, proc_root=Path('/proc')):
    """Search client-declared result dirs and nearby log dirs, never the disk."""
    search = set(Path(p).resolve() for p in roots)
    for pid in pids:
        proc = proc_root/str(pid)
        try:
            args = (proc/'cmdline').read_bytes().decode('utf-8', errors='replace').split('\0')
            if not any('benchmark' in a.lower() or a == 'bench' for a in args):
                continue
            cwd = (proc/'cwd').resolve()
            search.add(cwd)
            for i, arg in enumerate(args):
                key, sep, value = arg.partition('=')
                if key == '--result-dir':
                    value = value if sep else args[i+1]
                    target = Path(value)
                    search.add(target if target.is_absolute() else cwd/target)
        except (OSError, IndexError):
            continue
    paths = set()
    # Bound both discovery work and parsing; scope is printed, never called complete.
    for root in sorted(search)[:24]:
        for relative in ('.', 'results', 'benchmark', 'benchmarks', 'output'):
            folder = root/relative
            try:
                with os.scandir(folder) as entries:
                    for i, entry in enumerate(entries):
                        if i >= 256:
                            break
                        if entry.name.endswith('.json') and entry.is_file():
                            s = entry.stat()
                            if s.st_mtime_ns >= started_ns and s.st_size <= 64*MIB:
                                paths.add((s.st_mtime_ns, entry.path))
            except OSError:
                continue
    matches = []
    for _, name in sorted(paths, reverse=True)[:40]:
        try:
            data = json.loads(Path(name).read_text(encoding='utf-8'))
            if isinstance(data, dict) and 'completed' in data and any(k in data for k in ('duration', 'request_throughput')):
                matches.append(str(Path(name).resolve()))
        except (OSError, ValueError):
            continue
    return matches, sorted(str(p) for p in search)[:24]


def record(args):
    if sys.platform != 'linux':
        raise SystemExit('record requires the actual Linux container; no simulated runtime receipt')
    if args.config:
        config = json.loads(args.config.read_text(encoding='utf-8'))
    else:
        config = dict(run_id=args.run_id or 'customer-night-'+time.strftime('%Y%m%d'),
                      node=args.node or socket.gethostname(), group='LMCache_APC',
                      logs=[], pids=[], metrics={}, status={}, retain_files=[],
                      settings={}, auto_config=True,
                      note='Actual parameters are in processes.jsonl and startup logs; unreadable fields remain unknown.')
        args.auto_logs = True
    for key in ('run_id', 'node', 'group'):
        if not isinstance(config.get(key), str) or not config[key].strip():
            raise SystemExit('Fill '+key+' in the session config')
    if not (args.auto_logs or config.get('logs') or config.get('retain_files') or
            config.get('metrics') or config.get('status')):
        raise SystemExit('No evidence sources configured')
    paths = config.get('logs', []) + config.get('retain_files', [])
    if any(not Path(p).is_absolute() for p in paths):
        raise SystemExit('Source paths must be absolute')
    if len(paths) > 64:
        raise SystemExit('At most 64 explicitly selected files')
    out = (args.out or Path(__file__).resolve().parent/'runs'/(
        'session-'+time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8])).resolve()
    if any(Path(p).resolve().is_relative_to(out) for p in paths):
        raise SystemExit('Evidence sources must be outside this new output directory')
    os.umask(0o077)
    out.mkdir(parents=True, exist_ok=False)
    write_json(out/'session.json', dict(config=config, started=stamp(), python=sys.executable,
                                      boot_id=small_file('/proc/sys/kernel/random/boot_id'),
                                      duration=args.duration, pid=os.getpid(),
                                      status='COLLECTING; not proof of full probe coverage'))
    archive = Archive(out/'raw', args.raw_mib*MIB, args.reserve_mib*MIB)
    logs = set(config.get('logs', []))
    pids = set(int(p) for p in config.get('pids', []))
    seen = set()
    child = None
    console = (out/'watch-console.log').open('wb')
    started = time.monotonic()
    started_wall_ns = time.time_ns()
    result_roots, retained_results = set(), set()
    last_discovery = last_sample = last_detail = -float('inf')
    metadata_bytes, metadata_dropped = 0, 0
    stop_reason = 'duration'

    def append(name, row):
        nonlocal metadata_bytes, metadata_dropped
        raw = (json.dumps(row, ensure_ascii=True)+'\n').encode()
        if metadata_bytes+len(raw) > args.metadata_mib*MIB or shutil.disk_usage(out).free < args.reserve_mib*MIB:
            metadata_dropped += 1
            return
        with (out/name).open('ab') as f:
            f.write(raw)
        metadata_bytes += len(raw)

    try:
        print('SESSION_STARTED '+str(out), flush=True)
        while args.duration == 0 or time.monotonic()-started < args.duration:
            now = time.monotonic()
            if now-last_discovery >= 60:
                if args.auto_logs:
                    found = discover()
                    logs.update(found['logs'])
                    pids.update(p['pid'] for p in found['processes'])
                    append('discovery.jsonl', dict(**stamp(), **found))
                    result_roots.update(str(Path(p).parent) for p in found['logs'])
                    candidates, scanned = result_candidates(pids, result_roots, started_wall_ns)
                    result_roots.update(scanned)
                    retained_results.update(candidates)
                    append('benchmark-candidates.jsonl', dict(**stamp(), candidates=candidates, roots=scanned,
                           association='CANDIDATES_MODIFIED_DURING_CAPTURE; verify client/run identity'))
                last_discovery = now
            selected = sorted(logs | set(config.get('retain_files', [])) | retained_results)
            for path in selected[64:]:
                archive.error(path, 'source_limit_64')
            for path in selected[:64]:
                archive.add(path)
            archive.poll()
            if child is None and any(Path(p).is_file() for p in logs):
                # One HTTP polling owner: this session saves complete raw responses.
                watch_config = dict(config, logs=[p for p in sorted(logs) if Path(p).is_file()], metrics={}, status={})
                write_json(out/'watch-config.json', watch_config)
                command = [sys.executable, '-B', '-S', str(Path(__file__).with_name('watch.py')),
                           '--config', str(out/'watch-config.json'), '--out', str(out/'watch'), '--duration', '0']
                if args.auto_logs:
                    command.append('--auto-logs')
                child = subprocess.Popen(command, stdout=console, stderr=console, start_new_session=True)
            if now-last_sample >= 5:
                detail = now-last_detail >= 60
                snapshot = resources(pids, detail)
                append('resources.jsonl', snapshot)
                for row in snapshot['processes']:
                    identity = (row['pid'], row.get('start_ticks'))
                    if identity not in seen:
                        params = process_parameters(row['pid'])
                        params.pop('phase', None)
                        append('processes.jsonl', dict(**stamp(), identity=identity, parameters=params))
                        seen.add(identity)
                for kind in ('metrics', 'status'):
                    for label, url in config.get(kind, {}).items():
                        before = stamp()
                        try:
                            response = read_url(url, timeout=1, max_bytes=MIB).decode('utf-8', errors='replace')
                            append('endpoints.jsonl', dict(kind=kind, label=label, start=before, end=stamp(), raw=response))
                        except Exception as exc:
                            append('endpoints.jsonl', dict(kind=kind, label=label, start=before, end=stamp(), error=type(exc).__name__))
                last_sample = now
                if detail:
                    last_detail = now
                    print('SESSION_PROGRESS '+json.dumps(dict(seconds=int(now-started), raw_bytes=archive.written,
                                                              metadata_dropped=metadata_dropped,
                                                              log_files=len(logs), result_candidates=len(retained_results))), flush=True)
                    if not logs:
                        print('NO_FILE_LOGS: service output may be a pipe/terminal; raw service logs are not covered.', flush=True)
                write_json(out/'status.json', dict(**stamp(), active=True, archive=archive.state(),
                            metadata_dropped=metadata_dropped, watch_pid=child.pid if child else None,
                            watch_exit=child.poll() if child else None))
            time.sleep(1)
    except KeyboardInterrupt:
        stop_reason = 'operator_interrupt'
    except BaseException:
        stop_reason = 'collector_error'
        raise
    finally:
        if child is not None and child.poll() is None:
            child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.terminate()
                child.wait(timeout=5)
        console.close()
        if args.auto_logs:
            candidates, scanned = result_candidates(pids, result_roots, started_wall_ns)
            retained_results.update(candidates)
            append('benchmark-candidates.jsonl', dict(**stamp(), candidates=candidates, roots=scanned,
                   association='FINAL_CANDIDATES; verify client/run identity'))
            for path in candidates:
                archive.add(path)
            # Finished result JSON is bounded at 64 MiB; copy it before final status.
            for _ in range(64):
                if all(x.get('pending_bytes') == 0 for x in archive.state()['files']):
                    break
                if archive.written >= archive.budget or shutil.disk_usage(out).free <= archive.reserve:
                    break
                archive.poll()
        archive.poll()
        state = dict(**stamp(), active=False, stop_reason=stop_reason, archive=archive.state(),
                     metadata_dropped=metadata_dropped, watch_pid=child.pid if child else None,
                     watch_exit=child.returncode if child else None,
                     coverage='CHECK_PENDING_BYTES_ERRORS_AND_PROBE_MATRIX; never infer all-data-complete')
        write_json(out/'status.json', state)
        archive.close()
        print('SESSION_SAVED '+str(out/'status.json'), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    commands = p.add_subparsers(dest='action', required=True)
    r = commands.add_parser('record')
    r.add_argument('--config', type=Path)
    r.add_argument('--out', type=Path)
    r.add_argument('--run-id')
    r.add_argument('--node')
    r.add_argument('--auto-logs', action='store_true')
    r.add_argument('--duration', type=float, default=0)
    r.add_argument('--raw-mib', type=int, default=8192)
    r.add_argument('--metadata-mib', type=int, default=256)
    r.add_argument('--reserve-mib', type=int, default=1024)
    i = commands.add_parser('index')
    i.add_argument('--run', type=Path, required=True)
    e = commands.add_parser('extract')
    e.add_argument('--run', type=Path, required=True)
    e.add_argument('--file', required=True, help='Relative path from index')
    e.add_argument('--offset', type=int, default=0)
    e.add_argument('--bytes', type=int, default=16384)
    s = commands.add_parser('summary')
    s.add_argument('--run', type=Path)
    args = p.parse_args()
    if args.action == 'record':
        if args.duration < 0 or min(args.raw_mib, args.metadata_mib, args.reserve_mib) <= 0:
            p.error('duration must be nonnegative; budgets must be positive')
        record(args)
    elif args.action == 'summary':
        run = args.run
        if run is None:
            choices = [x for x in (Path(__file__).resolve().parent/'runs').glob('session-*') if (x/'session.json').is_file()]
            if not choices:
                p.error('No session found; supply --run with the printed output directory')
            run = max(choices, key=lambda x:(x/'session.json').stat().st_mtime_ns)
        print('SESSION_DIRECTORY '+str(run.resolve()), flush=True)
        print((run/'status.json').read_text(encoding='utf-8'), flush=True)
        if (run/'benchmark-candidates.jsonl').is_file():
            lines = (run/'benchmark-candidates.jsonl').read_text(encoding='utf-8').splitlines()
            if lines:
                print('LATEST_BENCHMARK_SEARCH '+lines[-1], flush=True)
        if (run/'watch/status.json').is_file():
            subprocess.run([sys.executable, '-B', '-S', str(Path(__file__).with_name('collect_case.py')),
                            '--run', str(run/'watch')], check=True)
    elif args.action == 'index':
        for name in ('session.json', 'status.json'):
            path = args.run/name
            print(name+' '+path.read_text(encoding='utf-8'))
        for path in sorted(args.run.rglob('*')):
            if path.is_file() and not path.is_symlink():
                print(json.dumps(dict(file=str(path.relative_to(args.run)), bytes=path.stat().st_size)))
    else:
        root = args.run.resolve()
        path = (root/args.file).resolve()
        if not path.is_relative_to(root) or args.offset < 0 or not 0 < args.bytes <= MIB:
            p.error('Choose a run-local file, nonnegative offset and 1..1048576 bytes')
        with path.open('rb') as f:
            f.seek(args.offset)
            data = f.read(args.bytes)
        print(json.dumps(dict(file=str(path), offset=args.offset, bytes=len(data))))
        print(data.decode('utf-8', errors='replace'))


if __name__ == '__main__':
    main()
