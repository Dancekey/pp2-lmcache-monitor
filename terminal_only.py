"""Standalone stdin-compatible observation; no files written, no services touched."""
import json, os, re, time

LOGS = ['/填写vLLM日志绝对路径', '/填写LMCache日志绝对路径']
DURATION = 60


def main(logs=LOGS, duration=DURATION):
    paths = list(dict.fromkeys(p for p in logs if p))
    if not paths or any(not os.path.isabs(p) or not os.path.isfile(p) for p in paths):
        raise SystemExit('请填写真实日志绝对路径；只有一个日志时，删除另一个路径。')
    action = re.compile(r'\b(Stored|Retrieved)\s+(\d+)\s+tokens(?:\s+in\s+([\d.]+)\s+seconds)?')
    queue = re.compile(r'Running:\s*(\d+).*?Waiting:\s*(\d+)')
    lookup = re.compile(r'lookup avg latency=([\d.]+)ms')
    states, report = {}, {}
    for path in paths:
        st = os.stat(path)
        states[path] = [(st.st_dev, st.st_ino), st.st_size, b'']
        report[path] = dict(STORE=dict(n=0, logged_tokens=0, timed_n=0, host_ms_sum=0, host_ms_max=None),
                            RETRIEVE=dict(n=0, logged_tokens=0, timed_n=0, host_ms_sum=0, host_ms_max=None),
                            last_queue=None, waiting_peak=None, last_lookup_log_avg_ms=None,
                            skipped_bytes=0, read_errors=0, rotations=0)
    deadline = time.monotonic()+duration
    print('开始只读观察 %s 秒；Ctrl+C 仅停止本命令。' % duration, flush=True)
    try:
        while time.monotonic() < deadline:
            for path, state in states.items():
                row = report[path]
                try:
                    st = os.stat(path)
                    if (st.st_dev, st.st_ino) != state[0] or st.st_size < state[1]:
                        row['rotations'] += 1
                        row['skipped_bytes'] += len(state[2])
                        state[:] = [(st.st_dev, st.st_ino), 0, b'']
                    with open(path, 'rb') as src:
                        if st.st_size-state[1] > 262144:
                            skip = st.st_size-state[1]-262144
                            row['skipped_bytes'] += skip+len(state[2])
                            state[1] += skip
                            state[2] = b''
                            src.seek(state[1])
                            skip = len(src.readline(262144))
                            state[1] += skip
                            row['skipped_bytes'] += skip
                        src.seek(state[1])
                        data = src.read(262144)
                        state[1] += len(data)
                    lines = (state[2]+data).split(b'\n')
                    state[2] = lines.pop()
                    if len(state[2]) > 65536:
                        row['skipped_bytes'] += len(state[2])
                        state[2] = b''
                    for raw in lines:
                        text = raw.decode('utf-8', errors='replace')
                        m = action.search(text)
                        if m:
                            item = row['STORE' if m[1] == 'Stored' else 'RETRIEVE']
                            item['n'] += 1
                            item['logged_tokens'] += int(m[2])
                            if m[3] is not None:
                                ms = float(m[3])*1000
                                item['timed_n'] += 1
                                item['host_ms_sum'] += ms
                                item['host_ms_max'] = max(item['host_ms_max'] or 0, ms)
                        m = queue.search(text)
                        if m:
                            row['last_queue'] = dict(running=int(m[1]), waiting=int(m[2]))
                            row['waiting_peak'] = max(row['waiting_peak'] or 0, int(m[2]))
                        m = lookup.search(text)
                        if m:
                            row['last_lookup_log_avg_ms'] = float(m[1])
                except OSError:
                    row['read_errors'] += 1
            time.sleep(max(0, min(1, deadline-time.monotonic())))
    except KeyboardInterrupt:
        print('\n已停止本次观察。')
    finally:
        for path, row in report.items():
            row['partial_bytes'] = len(states[path][2])
            for kind in ('STORE', 'RETRIEVE'):
                item = row[kind]
                item['host_ms_mean'] = item.pop('host_ms_sum')/item['timed_n'] if item['timed_n'] else None
        print(json.dumps(dict(scope='仅本窗口新增日志；不是唯一请求数、成功搬运或纯DMA',
                              rpc_breakdown=None, device_time=None, pp_overlap=None,
                              device_overhead_verified=False, logs=report), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
