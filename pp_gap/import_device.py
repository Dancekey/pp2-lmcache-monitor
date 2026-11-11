"""Convert explicitly verified profiler lanes; never guess kernel categories."""
import argparse
import json
from pathlib import Path


def convert(trace, mapping):
    if not mapping.get('lane_classification_verified') or not mapping.get('host_device_anchor_verified'):
        raise ValueError('Actual lane classification and host/device correlation must be verified first')
    rows = trace['traceEvents'] if isinstance(trace, dict) else trace
    rules = mapping['lanes']
    anchors = mapping['anchors']
    scale = anchors['mono_ns_per_trace_us']
    if scale <= 0:
        raise ValueError('Invalid clock scale')
    def mono(ts):
        return round(anchors['mono_ns'] + (ts-anchors['trace_us'])*scale)
    result = []
    for i, event in enumerate(rows):
        if event.get('ph') != 'X' or 'dur' not in event:
            continue
        matches = [x for x in rules if str(x['pid']) == str(event.get('pid')) and str(x['tid']) == str(event.get('tid'))
                   and ('event_names' not in x or event.get('name') in x['event_names'])]
        if len(matches) > 1:
            raise ValueError('Ambiguous profiler lane/name rules')
        rule = matches[0] if matches else None
        if rule is None:
            continue
        # Explicit lane classification must not include CPU launch/API lanes.
        if rule['kind'] not in ('compute', 'communication', 'transfer', 'unknown'):
            raise ValueError('Unrecognized device lane kind')
        start = max(event['ts'], mapping['capture_start_trace_us'])
        end = min(event['ts']+event['dur'], mapping['capture_end_trace_us'])
        if end <= start:
            continue
        result.append({k: mapping[k] for k in ('run', 'group', 'node', 'clock_domain')} | {
            'pp_rank': rule['pp_rank'], 'tp_rank': rule['tp_rank'], 'kind': rule['kind'],
            'name': event.get('name', ''), 'start_ns': mono(start), 'end_ns': mono(end),
            'capture_start_ns': mono(mapping['capture_start_trace_us']),
            'capture_end_ns': mono(mapping['capture_end_trace_us']),
            'classification_verified': rule['kind'] != 'unknown', 'clock_error_ns': anchors['error_ns'],
            'compute_coverage_verified': mapping.get('compute_coverage_verified', False),
            'batch': mapping.get('task_batches', {}).get(str(i)),
            'evidence': f"{mapping['raw_trace_path']}#traceEvents[{i}]",
            'scope': mapping.get('scope', 'MODEL_PROCESS_ONLY_CACHE_NOT_CAPTURED')})
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--trace', type=Path, required=True)
    p.add_argument('--mapping', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    r = convert(json.loads(a.trace.read_text(encoding='utf-8')), json.loads(a.mapping.read_text(encoding='utf-8')))
    a.out.write_text(json.dumps(r, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({'device_intervals': len(r), 'batch_linked': sum(x['batch'] is not None for x in r)}))
