"""CPU-only clock handshake for a newly authorized private container window."""
import argparse
import json
from pathlib import Path
import socket
import time


def serve(bind, port, seconds):
    deadline = time.monotonic()+seconds
    with socket.socket() as s:
        s.bind((bind, port))
        s.listen(4)
        s.settimeout(1)
        while time.monotonic() < deadline:
            try:
                c, _ = s.accept()
            except TimeoutError:
                continue
            with c:
                c.settimeout(2)
                c.recv(1)
                t1 = time.monotonic_ns()
                data = {'t1': t1, 't2': time.monotonic_ns(),
                        'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
                c.sendall(json.dumps(data).encode()+b'\n')


def sample(host, port, count, node, reference):
    rows = []
    for _ in range(count):
        with socket.create_connection((host, port), timeout=2) as c:
            t0 = time.monotonic_ns()
            c.sendall(b'x')
            payload = b''
            while not payload.endswith(b'\n'):
                block = c.recv(4096)
                if not block:
                    raise ValueError('Incomplete clock response')
                payload += block
            t3 = time.monotonic_ns()
        data = json.loads(payload)
        t1, t2 = data['t1'], data['t2']
        rtt = (t3-t0)-(t2-t1)
        rows.append(dict(data, t0=t0, t3=t3, rtt_ns=rtt,
                         node_mono_ns=(t1+t2)//2, reference_mono_ns=(t0+t3)//2,
                         error_ns=max(0, rtt)//2))
    best = min(rows, key=lambda r: r['rtt_ns'])
    return {'node': node, 'reference': reference, 'samples': rows, 'anchor': best,
            'warning': 'Uncertainty assumes monotonic clock rate stability; repeat before/after capture and record drift'}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='action', required=True)
    s = sub.add_parser('serve')
    s.add_argument('--bind', required=True)
    s.add_argument('--port', type=int, required=True)
    s.add_argument('--seconds', type=float, required=True)
    c = sub.add_parser('sample')
    c.add_argument('--host', required=True)
    c.add_argument('--port', type=int, required=True)
    c.add_argument('--count', type=int, default=9)
    c.add_argument('--node', required=True)
    c.add_argument('--reference', required=True)
    c.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    if a.action == 'serve':
        if a.seconds <= 0:
            p.error('--seconds must be positive')
        serve(a.bind, a.port, a.seconds)
    else:
        if a.count <= 0:
            p.error('--count must be positive')
        a.out.write_text(json.dumps(sample(a.host, a.port, a.count, a.node, a.reference), indent=2)+'\n', encoding='utf-8')
