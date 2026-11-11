"""Align calibrated local monotonic axes; retain raw clocks and uncertainty."""
import argparse
import collections
import json
from pathlib import Path


def calibration(samples):
    by_node = collections.defaultdict(list)
    references = {x['reference'] for x in samples}
    if len(references) != 1:
        raise ValueError('One common reference node is required')
    for row in samples:
        by_node[row['node']].append(row['anchor'])
    result = {}
    for node, anchors in by_node.items():
        if len({a['boot_id'] for a in anchors}) != 1:
            raise ValueError('Node rebooted between clock samples: '+node)
        anchors.sort(key=lambda a:a['node_mono_ns'])
        if len(anchors) < 2 or anchors[-1]['node_mono_ns'] <= anchors[0]['node_mono_ns']:
            raise ValueError('Need distinct pre/post samples for '+node)
        result[node] = anchors
    return next(iter(references)), result


def convert(node, ns, reference, anchors):
    if node == reference:
        return ns, 0
    pair = anchors.get(node)
    if pair is None or ns < pair[0]['node_mono_ns'] or ns > pair[-1]['node_mono_ns']:
        return None, None
    for a,b in zip(pair,pair[1:]):
        if a['node_mono_ns'] <= ns <= b['node_mono_ns']:
            local_delta = b['node_mono_ns']-a['node_mono_ns']
            ref_delta = b['reference_mono_ns']-a['reference_mono_ns']
            if ref_delta <= 0:
                raise ValueError('Reference clock is not monotonic')
            value = a['reference_mono_ns'] + (ns-a['node_mono_ns'])*ref_delta/local_delta
            # Conservative allowance for observed endpoint drift, not a claim
            # that asymmetric network delay or nonlinear drift was eliminated.
            error = max(a['error_ns'], b['error_ns']) + abs(ref_delta-local_delta)
            return round(value), error
    return None, None


def align(rows, devices, samples):
    reference, anchors = calibration(samples)
    events = []
    for row in rows:
        row = dict(row)
        ts, error = convert(row.get('node'), row['mono_ns'], reference, anchors)
        if ts is not None:
            row.update(aligned_mono_ns=ts, alignment_error_ns=error, reference_node=reference)
            if 'start_ns' in row:
                start, start_error = convert(row.get('node'), row['start_ns'], reference, anchors)
                if start is not None:
                    row['aligned_start_ns'] = start
                    row['alignment_error_ns'] = max(error, start_error)
        events.append(row)
    intervals = []
    for row in devices:
        row = dict(row)
        # Device importer has already converted this trace to host monotonic.
        if row['clock_domain'] == 'node_monotonic':
            converted = {k: convert(row['node'], row[k], reference, anchors)
                         for k in ('start_ns','end_ns','capture_start_ns','capture_end_ns')}
            if all(v[0] is not None for v in converted.values()):
                row.update({'aligned_'+k:v[0] for k,v in converted.items()})
                row.update(reference_node=reference, alignment_error_ns=max(v[1] for v in converted.values())+row.get('clock_error_ns',0))
        intervals.append(row)
    return events, intervals


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--clocks', type=Path, nargs='+', required=True)
    p.add_argument('--events', type=Path, nargs='+', required=True)
    p.add_argument('--device-intervals', type=Path)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    samples = [json.loads(f.read_text(encoding='utf-8')) for f in a.clocks]
    rows = []
    for f in a.events:
        for n,line in enumerate(f.read_text(encoding='utf-8').split('\n'),1):
            if line.strip():
                row = json.loads(line)
                row.setdefault('evidence',f'{f}:{n}')
                rows.append(row)
    devices = json.loads(a.device_intervals.read_text(encoding='utf-8')) if a.device_intervals else []
    events, intervals = align(rows, devices, samples)
    a.out.mkdir(parents=True, exist_ok=False)
    (a.out/'events.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in events),encoding='utf-8')
    (a.out/'device-intervals.json').write_text(json.dumps(intervals,indent=2)+'\n',encoding='utf-8')
    (a.out/'alignment.json').write_text(json.dumps({'calibrations':samples,
         'aligned_event_count':sum('aligned_mono_ns' in x for x in events),
         'unaligned_event_count':sum('aligned_mono_ns' not in x for x in events),
         'limitations':'RTT/2 plus observed drift allowance; raw clocks preserved; ordering within combined error is unresolved'},indent=2)+'\n',encoding='utf-8')
