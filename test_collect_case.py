import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import collect_case as case


class CaseTests(unittest.TestCase):
    def test_only_selected_arguments_and_connector_fields_are_printed(self):
        args = ['python', '--token', 'NEVER_PRINT', '--l1-size-gb=256', '--node-rank', '1', '--headless',
                '--kv-transfer-config', '{"kv_connector":"LMCacheMPConnector","password":"NEVER_PRINT"}']
        result = case.selected_arguments(args)
        self.assertEqual(result['--l1-size-gb'], '256')
        self.assertEqual(result['--node-rank'], '1')
        self.assertNotIn('NEVER_PRINT', json.dumps(result))

    def test_historical_peak_and_context_are_reported_without_raw_payload(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root/'incidents').mkdir()
            (root/'status.json').write_text(json.dumps(dict(at_wall_ns=60*10**9, started_wall=0, collector_node='synthetic')))
            (root/'minutes.jsonl').write_text(json.dumps(dict(minute_start_utc='synthetic', numbers={
                'source': {'native.lookup.lookup_log_avg_ms':dict(n=2,max=20000)}}))+'\n')
            queue = dict(record='event', event=dict(event='native.queue', collected_wall_ns=1, values=dict(waiting=5)))
            slow = dict(record='trigger', reason='native.store.host_log_slow', wall_ns=2,
                        event=dict(path='synthetic.log', values=dict(host_ms=2000, prompt='NEVER_PRINT')))
            (root/'incidents/one.jsonl').write_text(json.dumps(queue)+'\n'+json.dumps(slow)+'\n')
            with contextlib.redirect_stdout(io.StringIO()) as stream:
                case.historical(root)
            value = stream.getvalue()
            self.assertIn('MINUTE', value)
            self.assertIn('queue_context', value)
            self.assertIn('2000', value)
            self.assertNotIn('NEVER_PRINT', value)

    def test_benchmark_reports_numeric_summary_only(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root/'benchmark.json').write_text(json.dumps(dict(completed=2,duration=3,mean_ttft_ms=4,
                                                               generated_texts=['NEVER_PRINT'])))
            with contextlib.redirect_stdout(io.StringIO()) as stream:
                case.benchmark_candidates(root)
            self.assertIn('BENCHMARK_CANDIDATE', stream.getvalue())
            self.assertNotIn('NEVER_PRINT', stream.getvalue())

    def test_proc_stat_indexes_and_process_exit(self):
        parts = ['0']*22
        for index,value in ((7,'5'), (9,'6'), (11,'7'), (12,'8'), (19,'99')):
            parts[index] = value
        def read(path, *a, **k):
            return '123 (name with spaces) '+' '.join(parts) if path.name == 'stat' else 'VmRSS:\t12 kB\n'
        with patch.object(Path, 'read_text', read):
            value = case.process_snapshot(123)
        self.assertEqual((value['cpu_ticks'], value['start_ticks'], value['major_faults']), (15,99,6))
        with patch.object(Path, 'read_text', side_effect=FileNotFoundError):
            self.assertTrue(case.process_snapshot(123)['unavailable'])


if __name__ == '__main__':
    unittest.main()
