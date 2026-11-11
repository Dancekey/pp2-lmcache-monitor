"""One-command read-only entry: existing logs -> capture -> offline report."""
import argparse
from collections import Counter
import json
from pathlib import Path
import socket
import time
import uuid

import analyze
import capture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--log', action='append', type=Path, default=[])
    parser.add_argument('--auto-logs', action='store_true', help='Discover existing vLLM/LMCache stdout/stderr file logs')
    parser.add_argument('--duration', type=float, default=60)
    parser.add_argument('--max-bytes', type=int, default=8*1024*1024,
                        help='Capture output budget in bytes; default 8 MiB')
    parser.add_argument('--out', type=Path)
    args = parser.parse_args()
    logs = list(dict.fromkeys(args.log))
    if args.auto_logs:
        from find_logs import discover
        discovery = discover()
        logs = list(dict.fromkeys(logs + [Path(p) for p in discovery['logs']]))
        print('DISCOVERED_LOGS\n' + json.dumps(discovery, ensure_ascii=False, indent=2), flush=True)
    if not logs:
        parser.error('No existing log files found. Use --log with a real file path; service output may be a terminal/pipe.')
    if any(not p.is_absolute() or not p.is_file() for p in logs):
        parser.error('--log must name an existing absolute log path')
    if not 0 < args.duration <= 3600:
        parser.error('--duration must be (0, 3600] seconds')
    if args.max_bytes <= 0:
        parser.error('--max-bytes must be positive')
    run_id = 'readonly-%s-%s' % (time.strftime('%Y%m%d-%H%M%S'), uuid.uuid4().hex[:8])
    out = args.out or Path(__file__).resolve().parent / 'runs' / run_id
    config = dict(run_id=run_id, node=socket.gethostname(), logs=[str(p) for p in logs])
    print('READ_ONLY_STARTED seconds=%s output=%s' % (args.duration, out), flush=True)
    manifest = capture.record(config, out, args.duration, interval=5, max_bytes=args.max_bytes)
    events, problems, sources = analyze.load_events([out / 'capture.jsonl'])
    analyze.analyze(events, out / 'report', problems=problems, provenance=sources,
                    manifests=[out / 'run.json'])
    counts = dict(Counter(e['event'] for e in events))
    by_source = {}
    for event in events:
        source = event.get('path')
        values = event.get('values', {})
        if source is None:
            continue
        row = by_source.setdefault(source, dict(store_logged_samples=0, store_host_ms_sum=0,
                                                store_host_ms_max=None, retrieve_host_ms=None,
                                                last_queue=None, last_reported_lookup_avg_ms=None))
        if event['event'] == 'native.stored' and 'host_seconds' in values:
            ms = values['host_seconds'] * 1000
            row['store_logged_samples'] += 1
            row['store_host_ms_sum'] += ms
            row['store_host_ms_max'] = max(row['store_host_ms_max'] or 0, ms)
        elif event['event'] == 'native.queue':
            row['last_queue'] = values
        elif event['event'] == 'native.lookup_latency':
            row['last_reported_lookup_avg_ms'] = values.get('elapsed_ms')
    for row in by_source.values():
        row['store_host_ms_mean'] = (row['store_host_ms_sum']/row['store_logged_samples']
                                     if row['store_logged_samples'] else None)
    result = dict(output=str(out), report=str(out / 'report/report.md'),
                  samples=manifest['samples'], event_counts=counts,
                  budget_dropped=manifest['budget_dropped'], tail_status=manifest['tail_status'],
                  log_observations_by_source=by_source,
                  scope='existing incremental numeric logs; counts are observations, not unique requests',
                  rpc_breakdown=None, device_time=None, device_overhead_verified=False)
    print('SUMMARY\n' + json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if not events:
        print('NO_MATCHING_INCREMENTAL_EVENTS: this does not prove absence of STORE/RETRIEVE.', flush=True)


if __name__ == '__main__':
    main()
