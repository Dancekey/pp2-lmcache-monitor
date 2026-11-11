"""Discover existing regular stdout/stderr log files; never read pipe output."""
import argparse
import json
import os
from pathlib import Path


def role_for(arguments):
    for value in arguments:
        name = value.replace('\\', '/').lower()
        base = name.rsplit('/', 1)[-1]
        if base.startswith('vllm') or name.startswith('vllm.') or '/vllm/' in name:
            return 'vllm'
        if base.startswith('lmcache') or name.startswith('lmcache.') or '/lmcache/' in name:
            return 'lmcache'
    return None


def discover(proc_root=Path('/proc')):
    found, processes = set(), []
    try:
        entries = sorted(proc_root.iterdir())
    except OSError:
        entries = []
    for entry in entries:
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            arguments = (entry/'cmdline').read_bytes().decode('utf-8', errors='replace').split('\0')
            role = role_for(arguments)
            if role is None:
                continue
            for fd in ('1', '2'):
                try:
                    destination = os.readlink(entry/'fd'/fd)
                except OSError:
                    continue
                regular = os.path.isabs(destination) and os.path.isfile(destination)
                processes.append(dict(pid=int(entry.name), role=role, fd=int(fd),
                                      output=destination, regular_file=regular))
                if regular:
                    found.add(destination)
        except (OSError, ValueError):
            continue
    return dict(logs=sorted(found), processes=processes,
                note='Process argv is not printed. Pipes, terminals and unavailable files are not read.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    print(json.dumps(discover(), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
