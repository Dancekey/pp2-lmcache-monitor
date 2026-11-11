"""Read process mappings and distribution records; never import serving packages."""
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def inventory(pids, binaries):
    distributions = {}
    for name in ('lmcache', 'lmcache-ascend', 'vllm', 'vllm-ascend', 'torch', 'torch-npu', 'mooncake-transfer-engine'):
        try:
            d = importlib.metadata.distribution(name)
            distributions[name] = {'version': d.version, 'distribution_root': str(d.locate_file('')),
                                   'direct_url': d.read_text('direct_url.json'),
                                   'warning': 'Distribution metadata is not proof of a running process import path'}
        except importlib.metadata.PackageNotFoundError:
            distributions[name] = None
    processes = []
    for pid in pids:
        proc = Path('/proc') / str(pid)
        fields = (proc / 'stat').read_text(encoding='utf-8').rsplit(')', 1)[1].split()
        maps = (proc / 'maps').read_text(encoding='utf-8')
        paths = sorted({l.split(None, 5)[5] for l in maps.splitlines() if len(l.split(None, 5)) == 6 and '.so' in l.split(None, 5)[5]})
        processes.append({'pid': pid, 'state': fields[0], 'start_ticks': int(fields[19]),
                          'executable': os.readlink(proc / 'exe'), 'mapped_binaries': paths})
    build_ids = {}
    for binary in binaries:
        # Only explicitly selected loaded native artifacts; no directory scans/hashing.
        r = subprocess.run(['readelf', '-n', binary], capture_output=True, text=True, timeout=10)
        build_ids[binary] = {'returncode': r.returncode,
                             'build_id_lines': [l.strip() for l in r.stdout.splitlines() if 'Build ID:' in l],
                             'build_source': 'NOT_VERIFIED_UNTIL_MATCHED_TO_WHEEL_OR_BUILD_RECEIPT'}
    return {'state': 'READ_ONLY_RUNTIME_INVENTORY', 'wall_ns': time.time_ns(),
            'mono_ns': time.monotonic_ns(), 'python': sys.executable,
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'distributions': distributions, 'processes': processes, 'native_build_ids': build_ids,
            'required_companion': 'observer module.loaded/hook.method paths, runtime CLI and config, binary build/wheel provenance'}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pid', type=int, action='append', required=True)
    p.add_argument('--binary', action='append', default=[])
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    a.out.write_text(json.dumps(inventory(a.pid, a.binary), indent=2) + '\n', encoding='utf-8')
