"""Associate batches and draw host/device evidence without inferring missing causes."""
import argparse
import collections
import csv
import html
import json
from pathlib import Path


def union(intervals):
    merged = []
    for start, end in sorted(intervals):
        if end < start:
            raise ValueError('Reversed interval')
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(end, merged[-1][1])
        else:
            merged.append([start, end])
    return merged


def gaps(intervals, start, end):
    result, cursor = [], start
    for left, right in union([(max(a, start), min(b, end)) for a, b in intervals if b >= start and a <= end]):
        if left > cursor:
            result.append([cursor, left])
        cursor = max(cursor, right)
    if cursor < end:
        result.append([cursor, end])
    return result


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    pos = (len(s)-1)*p/100
    lo = int(pos)
    return s[lo] + (s[min(lo+1, len(s)-1)]-s[lo])*(pos-lo)


def load(paths):
    rows, errors = [], []
    for path in paths:
        for lineno, line in enumerate(path.read_text(encoding='utf-8').splitlines(), 1):
            try:
                row = json.loads(line)
                row.setdefault('evidence', f'{path}:{lineno}')
                rows.append(row)
            except (json.JSONDecodeError, TypeError) as exc:
                errors.append({'path': str(path), 'line': lineno, 'error': str(exc)})
    return rows, errors


def analyze(rows, devices, out):
    out.mkdir(parents=True, exist_ok=False)
    maps = collections.defaultdict(list)
    for r in rows:
        if r['event'] == 'batch.map':
            maps[(r.get('run'), r.get('group'), r['batch'])].append(r)
    batches = []
    for (run, group, batch), records in maps.items():
        execs = [r for r in records if r.get('role') == 'executor']
        workers = [r for r in records if r.get('role') == 'worker']
        ranks = collections.Counter((r.get('pp_rank'), r.get('tp_rank')) for r in workers)
        expected = {(pp, tp) for pp in range(2) for tp in range(16)}
        shapes = {json.dumps(r.get('scheduled_tokens'), sort_keys=True) for r in records}
        valid = len(execs) == 1 and set(ranks) == expected and all(n == 1 for n in ranks.values()) and len(shapes) == 1
        base = records[0]
        batches.append({'run': run, 'group': group, 'batch': batch,
                        'tokens': base['tokens'], 'request_count': base['request_count'],
                        'batch_type':base.get('batch_type','unknown'),
                        'external_restore_tokens':json.dumps(base.get('external_restore_tokens',{})),
                        'requests': json.dumps(base['requests']), 'scheduled_tokens': json.dumps(base['scheduled_tokens']),
                        'association': 'SEQUENCE_MATCHED_DEVICE_LINK_STILL_REQUIRED' if valid else 'INCOMPLETE_OR_AMBIGUOUS',
                        'executor_records': len(execs), 'worker_records': len(workers),
                        'missing_ranks': json.dumps(sorted(expected-set(ranks))),
                        'duplicate_ranks': json.dumps([list(k) for k, n in ranks.items() if n > 1])})
    with (out / 'batches.csv').open('w', newline='', encoding='utf-8-sig') as f:
        names = list(batches[0]) if batches else ['run', 'group', 'batch', 'association']
        writer = csv.DictWriter(f, fieldnames=names)
        writer.writeheader()
        writer.writerows(batches)
    # Host axes stay separate by node. No subtraction of cross-node raw timestamps.
    def axis(row):
        return (row.get('run'), row.get('group'),
                'reference='+row['reference_node'] if 'aligned_mono_ns' in row and ('start_ns' not in row or 'aligned_start_ns' in row) else 'node='+str(row.get('node')))
    origins = {}
    for r in rows:
        node = axis(r)
        ts = r.get('aligned_mono_ns',r['mono_ns']) if node[-1].startswith('reference=') else r['mono_ns']
        origins[node] = min(origins.get(node, ts), ts)
    for d in devices:
        if 'aligned_start_ns' in d:
            node = (d['run'],d['group'],'reference='+d['reference_node'])
            ts = d['aligned_capture_start_ns']
            origins[node] = min(origins.get(node,ts),ts)
    chrome, visual = [], []
    for r in rows:
        node = axis(r)
        if 'start_ns' in r and 'duration_ns' in r:
            start = r.get('aligned_start_ns',r['start_ns']) if node[-1].startswith('reference=') else r['start_ns']
            duration = r['duration_ns']
            event = r['event'].removesuffix('.end')
            pid = '/'.join(str(x) for x in node) + f"/{r.get('node')}/host/{r['pid']}"
            chrome.append({'name': event, 'cat': 'HOST_ONLY', 'ph': 'X', 'pid': pid,
                           'tid': r['thread'], 'ts': (start-origins[node])/1000, 'dur': duration/1000,
                           'args': r})
            visual.append({'lane': pid + '/t' + str(r['thread']), 'start': (start-origins[node])/1e6,
                           'duration': duration/1e6, 'label': event + ' batch=' + str(r.get('batch')),
                           'kind': 'host', 'evidence': r.get('evidence')})
    device_stats = []
    lanes = collections.defaultdict(list)
    for d in devices:
        if d['end_ns'] < d['start_ns'] or d['kind'] not in ('compute', 'communication', 'transfer', 'unknown'):
            raise ValueError('Invalid normalized device interval')
        key = (d['run'], d['group'], d['node'], d['pp_rank'], d['tp_rank'], d['clock_domain'])
        lanes[key].append(d)
    for key, events in lanes.items():
        # A lane must declare the actual complete capture bounds, including idle edges.
        starts = {e['capture_start_ns'] for e in events}
        ends = {e['capture_end_ns'] for e in events}
        if len(starts) != 1 or len(ends) != 1:
            raise ValueError('Inconsistent capture bounds')
        left, right = next(iter(starts)), next(iter(ends))
        if right <= left:
            raise ValueError('Invalid capture bounds')
        compute = [(e['start_ns'], e['end_ns']) for e in events if e['kind'] == 'compute']
        known_compute = all(e.get('compute_coverage_verified', False) for e in events)
        holes = gaps(compute, left, right) if known_compute else []
        device_stats.append({'lane': key, 'capture_ns': right-left,
                             'compute_union_ns': sum(b-a for a,b in union(compute)),
                             'all_activity_union_ns': sum(b-a for a,b in union([(e['start_ns'],e['end_ns']) for e in events])),
                             'compute_gaps': [{'start_ns': a, 'end_ns': b, 'category': 8,
                                               'reason': 'NEEDS_DEPENDENCY_AND_RESOURCE_EVIDENCE',
                                               'capture_edge': a == left or b == right} for a,b in holes],
                             'clock_domain': key[-1], 'compute_coverage_verified': known_compute})
        for e in events:
            label = '/'.join(str(x) for x in key)
            if 'aligned_start_ns' in e:
                node = (e['run'],e['group'],'reference='+e['reference_node'])
                label += '/reference='+e['reference_node']
                start = (e['aligned_start_ns']-origins[node])/1e6
            else:
                label += '/UNALIGNED'
                start = (e['start_ns']-left)/1e6
            duration = (e['end_ns']-e['start_ns'])/1e6
            visual.append({'lane': label+'/'+e['kind'], 'start': start, 'duration': duration,
                           'label': e.get('name', e['kind']) + ' batch=' + str(e.get('batch')),
                           'kind': e['kind'], 'evidence': e.get('evidence')})
            chrome.append({'name': e.get('name', e['kind']), 'cat': 'DEVICE_'+e['kind'], 'ph': 'X',
                           'pid': label, 'tid': e['kind'], 'ts': start*1000, 'dur': duration*1000, 'args': e})
    dispatch = collections.defaultdict(list)
    clients = collections.defaultdict(dict)
    for r in rows:
        if r['event'] == 'core.dispatch.begin':
            dispatch[(r.get('run'), r.get('group'), r.get('node'), r['pid'])].append(r['mono_ns'])
        if r['event'].startswith('client.') and r.get('request'):
            # request IDs contain cold/warm prefix; never pool their percentiles.
            rid = r['request']
            phase = 'cold' if '-cold-' in rid else 'warm' if '-warm-' in rid else 'unknown'
            clients[(r.get('run'), r.get('group'), phase)].setdefault(rid, {})[r['event']] = r
    client_stats = []
    for key, requests in clients.items():
        complete, failed, incomplete, ttfts = [], [], [], []
        for rid, events in requests.items():
            result = events.get('client.complete')
            sent = events.get('client.send')
            if result is None:
                incomplete.append(rid)
            elif result.get('success') and sent and result.get('output_tokens') == sent.get('output_len'):
                complete.append(result)
                ttfts.append(result['ttft_s']*1000)
            else:
                failed.append(rid)
        sent_times = [x['client.send']['mono_ns'] for x in requests.values() if 'client.send' in x]
        end_times = [x['client.complete']['completed_ns'] for x in requests.values() if 'client.complete' in x]
        elapsed = (max(end_times)-min(sent_times))/1e9 if sent_times and end_times else None
        client_stats.append({'group_round': key, 'sent': len(sent_times), 'complete_length_success': len(complete),
                             'failed_or_length_unverified': failed, 'incomplete': incomplete,
                             'ttft_p50_ms': percentile(ttfts, 50), 'ttft_p90_ms': percentile(ttfts, 90),
                             'ttft_p99_ms': percentile(ttfts, 99),
                             'successful_completion_req_s': len(complete)/elapsed if elapsed and not incomplete else None,
                             'measured_elapsed_s': elapsed, 'output_quality': 'NOT_EVALUATED'})
    result = {'state': 'ANALYZED_INPUTS_NOT_DEVICE_ACCEPTANCE', 'batch_count': len(batches),
              'ambiguous_or_incomplete_batches': sum(b['association'] == 'INCOMPLETE_OR_AMBIGUOUS' for b in batches),
              'dispatch': [{'lane': k, 'interval_p50_ms': percentile([(b-a)/1e6 for a,b in zip(sorted(v),sorted(v)[1:])],50),
                            'interval_p99_ms': percentile([(b-a)/1e6 for a,b in zip(sorted(v),sorted(v)[1:])],99)} for k,v in dispatch.items()],
              'device_lanes': device_stats, 'clients': client_stats,
              'cross_node_subtraction': 'ALIGNED_ROWS_HAVE_EXPLICIT_UNCERTAINTY' if any('aligned_mono_ns' in r for r in rows) else 'DISABLED_NO_VERIFIED_ALIGNMENT',
              'causality': 'All automatic gap categories remain 8 until reviewed with exact dependency evidence',
              'device_evidence': 'PRESENT_REVIEW_COVERAGE' if devices else 'NOT_CAPTURED'}
    (out / 'summary.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    (out / 'timeline.chrome.json').write_text(json.dumps({'traceEvents': chrome}, ensure_ascii=False), encoding='utf-8')
    lane_names = sorted({v['lane'] for v in visual})
    scale = max((v['start']+v['duration'] for v in visual), default=1) or 1
    svg = []
    colors = {'host': '#74839c', 'compute': '#21886a', 'communication': '#b77927', 'transfer': '#596fc2', 'unknown': '#777'}
    for i, lane in enumerate(lane_names):
        y = i*30+25
        svg.append(f'<text x="4" y="{y+10}" font-size="10">{html.escape(lane)}</text>')
        for v in (x for x in visual if x['lane'] == lane):
            x = 420+v['start']/scale*1100
            width = max(1,v['duration']/scale*1100)
            title = html.escape(f"{v['label']} | {v['start']:.3f}ms +{v['duration']:.3f}ms | {v['evidence']}")
            svg.append(f'<rect x="{x:.3f}" y="{y}" width="{width:.3f}" height="16" fill="{colors[v["kind"]]}"><title>{title}</title></rect>')
    warning = 'Lanes marked reference share a calibrated axis with error in event metadata. UNALIGNED/node lanes have independent origins. Ordering within combined clock error is unresolved.'
    page = '<!doctype html><meta charset="utf-8"><title>PP diagnostic evidence</title><h1>PP diagnostic evidence</h1><p>'+warning+'</p><p>Hover for batch and raw evidence. Green=device compute; amber=communication; blue=transfer; grey=host only.</p>'
    page += f'<svg width="1560" height="{max(150,len(lane_names)*30+60)}">'+''.join(svg)+'</svg>'
    (out / 'timeline.html').write_text(page, encoding='utf-8')
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--events', type=Path, nargs='+', required=True)
    p.add_argument('--device-intervals', type=Path)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    rows, errors = load(a.events)
    devices = json.loads(a.device_intervals.read_text(encoding='utf-8')) if a.device_intervals else []
    result = analyze(rows, devices, a.out)
    (a.out/'parse-errors.json').write_text(json.dumps(errors, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({'batch_count': result['batch_count'], 'device_evidence': result['device_evidence'], 'parse_errors': len(errors)}))
