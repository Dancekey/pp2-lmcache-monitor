import json
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from watch import Boundaries, Engine, TraceTail, WatchTail


def trace(name, t, **fields):
    return dict(event=name, mono_ns=int(t*1e9), node='synthetic', process_instance='p', pid=1,
                run_id='test', generation=None, request_id='r', **fields)


class WatchTests(unittest.TestCase):
    def setUp(self):
        self.stdout = contextlib.redirect_stdout(io.StringIO())
        self.stdout.__enter__()

    def tearDown(self):
        self.stdout.__exit__(None, None, None)

    def test_native_formats_and_privacy(self):
        tail = WatchTail(Path('absent'))
        e = tail.parse_line("INFO 01-01 01:02:03 LMCacheMPConnector={'LMCache MP lookup avg latency (ms)': 21506.674}")
        self.assertEqual(e['values']['lookup_log_avg_ms'], 21506.674)
        self.assertEqual(tail.parse_line('lookup avg latency=500000ms')['values']['lookup_log_avg_ms'], 500000)
        self.assertEqual(tail.parse_line('Retrieved 32 tokens in 0.002 seconds')['values']['host_ms'], 2)
        self.assertEqual(tail.parse_line('LMCache server is unhealthy — entering degraded mode')['event'], 'health.degraded')
        self.assertEqual(tail.parse_line('server is healthy again, triggering recovery callback')['event'], 'health.recovered_log')
        self.assertIsNone(tail.parse_line('prompt=NEVER_LOG'))

    def test_exact_byte_indexes_with_fragment(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'log'
            p.write_bytes(b'')
            tail = WatchTail(p)
            p.write_bytes(b'Retrieved 32 to')
            self.assertEqual(tail.read(), [])
            with p.open('ab') as f:
                f.write(b'kens in 0.002 seconds\nStored 16 tokens in 0.001 seconds\n')
            rows = tail.read()
            self.assertEqual(rows[0]['read_offset'], 0)
            self.assertEqual(rows[1]['read_offset'], len(b'Retrieved 32 tokens in 0.002 seconds\n'))

    def test_five_hours_and_late_incident_keep_trends(self):
        with tempfile.TemporaryDirectory() as d:
            engine = Engine(d, pre=120, post=120)
            for minute in range(301):
                engine.feed([dict(event='native.queue', path='synthetic.log', values=dict(running=1, waiting=1))],
                            minute*60, minute*60*10**9)
            engine.feed([dict(event='native.lookup', path='synthetic.log', values=dict(lookup_log_avg_ms=502000))],
                        18005, 18005*10**9)
            summary = engine.close()
            self.assertEqual(summary['reasons']['lookup.log_average_high'], 1)
            self.assertGreaterEqual(len((Path(d)/'minutes.jsonl').read_text().splitlines()), 301)
            rows = [json.loads(s) for s in (Path(d)/'incidents/window-000001.jsonl').read_text().splitlines()]
            self.assertTrue(any(e.get('event', {}).get('event') == 'native.queue' for e in rows))

    def test_budget_exhaustion_does_not_stop_observation(self):
        with tempfile.TemporaryDirectory() as d:
            engine = Engine(d, evidence_bytes=1, trend_bytes=1)
            for minute in range(301):
                engine.feed([dict(event='health.degraded', path='synthetic.log', values={})],
                            minute*60, minute*60*10**9)
            summary = engine.close()
            self.assertEqual(summary['observed_events'], 301)
            self.assertEqual(summary['reasons']['health.degraded'], 301)
            self.assertGreater(summary['losses']['evidence_dropped'], 0)
            self.assertGreater(summary['losses']['trends_dropped'], 0)

    def test_idle_queue_requires_continuous_same_lane_samples(self):
        with tempfile.TemporaryDirectory() as d:
            engine = Engine(d, wait=30, poll=5)
            q = dict(event='native.queue', path='log', source_pid='1', values=dict(running=0, waiting=10))
            for t in (0, 10, 20, 30):
                engine.feed([q], t, int(t*1e9))
            self.assertEqual(engine.reasons['queue.no_running_with_waiting_observed'], 1)
            q2 = dict(q, source_pid='2')
            engine.feed([q2], 40, 40*10**9)
            engine.feed([q2], 100, 100*10**9)  # A gap cannot count as continuously waiting.
            self.assertEqual(engine.reasons['queue.no_running_with_waiting_observed'], 1)
            engine.close()

    def test_ready_revisits_do_not_reset_and_late_consumption_is_measured(self):
        tracker = Boundaries(wait_seconds=30)
        ready = trace('future.query', 0, future_id='f', ready=True)
        tracker.feed(ready, 0)
        tracker.feed(trace('future.query', 20, future_id='f', ready=True), 20)
        self.assertEqual(len(tracker.poll(31)), 1)
        alerts = tracker.feed(trace('future.result.begin', 40, future_id='f'), 40)
        self.assertEqual(alerts[0]['seconds'], 40)
        tracker.feed(trace('future.query', 50, future_id='f', ready=True), 50)
        self.assertEqual(tracker.poll(100), [])

    def test_fast_replies_out_of_order_and_restarts(self):
        tracker = Boundaries(wait_seconds=1)
        tracker.feed(trace('future.set_result', 2, future_id='f'), 0)
        self.assertEqual(tracker.feed(trace('rpc.queue.put.begin', 1.9, future_id='f'), 0), [])
        self.assertEqual(tracker.poll(100), [])
        tracker.feed(trace('rpc.queue.put.begin', 3, future_id='g'), 3)
        tracker.feed(dict(trace('future.set_result', 4, future_id='g'), process_instance='restarted'), 4)
        self.assertEqual(tracker.poll(100)[0]['reason'], 'rpc.end_not_observed')

    def test_trace_filter_and_callback_begins_retained_in_trigger(self):
        tail = TraceTail(Path('absent'))
        row = tail.parse_line(json.dumps(dict(trace('span.begin', 0, span_id='s'), prompt='NEVER_LOG')))
        self.assertNotIn('NEVER_LOG', json.dumps(row))
        tracker = Boundaries(wait_seconds=30)
        tracker.feed(row, 0)
        self.assertEqual(tracker.poll(500)[0]['begin']['span_id'], 's')

    def test_endpoint_status_lanes_do_not_merge(self):
        with tempfile.TemporaryDirectory() as d:
            engine = Engine(d)
            engine.feed([dict(event='snapshot.status', endpoint='stage0', values=dict(completed_results=1)),
                         dict(event='snapshot.status', endpoint='stage1', values=dict(completed_results=9))], 0, 0)
            engine.close()
            minute = json.loads((Path(d)/'minutes.jsonl').read_text().splitlines()[0])
            self.assertEqual(len(minute['numbers']), 2)

    def test_more_than_five_hours_is_valid_cli_and_manual_stop_finalizes(self):
        import watch
        with tempfile.TemporaryDirectory() as d:
            log = Path(d)/'log'
            log.write_bytes(b'')
            out = Path(d)/'output'
            arguments = ['watch.py', '--log', str(log.resolve()), '--duration', '21600', '--out', str(out)]
            with patch('sys.argv', arguments), patch.object(watch.time, 'sleep', side_effect=KeyboardInterrupt):
                watch.main()
            self.assertFalse(json.loads((out/'summary.json').read_text())['active'])

    def test_unwritable_evidence_records_loss_and_continues(self):
        with tempfile.TemporaryDirectory() as d:
            engine = Engine(d)
            (Path(d)/'incidents/window-000001.jsonl').mkdir()
            engine.feed([dict(event='health.degraded', values={})], 0, 0)
            engine.feed([dict(event='native.lookup', values=dict(lookup_log_avg_ms=50000))], 300, 300*10**9)
            summary = engine.close()
            self.assertEqual(summary['observed_events'], 2)
            self.assertGreater(summary['losses']['write_errors'], 0)


if __name__ == '__main__':
    unittest.main()
