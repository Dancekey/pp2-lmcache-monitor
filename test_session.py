"""CPU-only checks for evidence retention, not device/probe acceptance."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch, Mock

import session


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.archive = session.Archive(self.root/'raw', 100000, 1)

    def tearDown(self):
        self.archive.close()
        self.temp.cleanup()

    def raw(self, n=0):
        return (self.root/'raw'/('%06d.bin' % n)).read_bytes()

    def test_append_preserves_bytes_and_source_offsets(self):
        source = self.root/'service.log'
        source.write_bytes(b'first\n\xffraw\n')
        self.archive.add(source)
        self.archive.poll()
        with source.open('ab') as f:
            f.write(b'next\n')
        self.archive.add(source)
        self.archive.poll()
        self.assertEqual(self.raw(), source.read_bytes())
        state = self.archive.state()['files'][0]
        self.assertEqual(state['source_end_offset_exclusive'], source.stat().st_size)
        self.assertEqual(state['pending_bytes'], 0)

    def test_budget_does_not_advance_past_retained_bytes(self):
        source = self.root/'service.log'
        source.write_bytes(b'0123456789')
        self.archive.budget = 4
        self.archive.add(source)
        self.archive.poll()
        self.archive.poll()
        self.assertEqual(self.raw(), b'0123')
        self.assertEqual(self.archive.state()['files'][0]['pending_bytes'], 6)
        self.assertTrue(self.archive.state()['budget_exhausted'])

    def test_truncation_opens_new_generation_and_reports_gap(self):
        source = self.root/'service.log'
        source.write_bytes(b'old content\n')
        self.archive.add(source)
        self.archive.poll()
        source.write_bytes(b'new\n')
        self.archive.add(source)
        self.archive.poll()
        self.assertEqual(self.raw(), b'old content\n')
        self.assertEqual(self.raw(1), b'new\n')
        self.assertIn('copytruncate', self.archive.errors[0]['error'])

    def test_late_created_source_is_retained(self):
        source = self.root/'later.json'
        self.archive.add(source)
        source.write_bytes(b'{"completed": 2}')
        self.archive.add(source)
        self.archive.poll()
        self.assertEqual(self.raw(), source.read_bytes())
        self.assertEqual(self.archive.errors[0]['error'], 'FileNotFoundError')

    def test_extract_exact_byte_range_with_identity_header(self):
        source = self.root/'result.log'
        source.write_bytes(b'0123456789')
        output = io.StringIO()
        with patch('sys.argv', ['session.py', 'extract', '--run', str(self.root),
                               '--file', 'result.log', '--offset', '3', '--bytes', '4']), contextlib.redirect_stdout(output):
            session.main()
        header, excerpt = output.getvalue().splitlines()
        self.assertEqual(json.loads(header)['offset'], 3)
        self.assertEqual(excerpt, '3456')

    def test_extract_refuses_parent_escape(self):
        with patch('sys.argv', ['session.py', 'extract', '--run', str(self.root),
                               '--file', '../outside']), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                session.main()
        self.assertEqual(error.exception.code, 2)

    def test_low_disk_preserves_source_and_exposes_backlog(self):
        source = self.root/'service.log'
        source.write_bytes(b'keep source')
        self.archive.add(source)
        self.archive.reserve = session.shutil.disk_usage(self.root).free+1
        self.archive.poll()
        self.assertEqual(source.read_bytes(), b'keep source')
        self.assertEqual(self.archive.state()['files'][0]['pending_bytes'], 11)

    def test_record_orchestration_saves_raw_metrics_and_child_failure(self):
        source = self.root/'model.log'
        source.write_bytes(b'Running: 1 Waiting: 2\n')
        config = self.root/'config.json'
        config.write_text(json.dumps(dict(run_id='cpu-only', node='fixture', group='ON',
                           logs=[str(source)], metrics={'model':'http://localhost/metrics'})))
        out = self.root/'run'
        child = Mock(pid=12345, returncode=3)
        child.poll.return_value = 3
        args = SimpleNamespace(config=config, out=out, auto_logs=False, duration=0,
                               raw_mib=1, metadata_mib=1, reserve_mib=1)
        response = b'vllm:cache_hits{model="fixture",engine="0"} 42\n'
        # Only OS/process/HTTP boundaries are substituted. The actual collector,
        # archive, resource-unavailable path and finalization execute normally.
        with patch.object(session.sys, 'platform', 'linux'), \
             patch.object(session.subprocess, 'Popen', return_value=child), \
             patch.object(session, 'read_url', return_value=response), \
             patch.object(session.time, 'sleep', side_effect=KeyboardInterrupt), \
             contextlib.redirect_stdout(io.StringIO()):
            session.record(args)
        state = json.loads((out/'status.json').read_text())
        self.assertFalse(state['active'])
        self.assertEqual(state['stop_reason'], 'operator_interrupt')
        self.assertEqual(state['watch_exit'], 3)
        self.assertEqual((out/'raw/000000.bin').read_bytes(), source.read_bytes())
        metric = json.loads((out/'endpoints.jsonl').read_text().splitlines()[0])
        self.assertEqual(metric['raw'], response.decode())
        child.send_signal.assert_not_called()

    def test_result_discovery_keeps_only_new_benchmark_json(self):
        import time
        old = self.root/'old.json'
        old.write_text('{"completed": 1, "duration": 2}')
        boundary = time.time_ns()
        result = self.root/'new.json'
        result.write_text('{"completed": 10, "duration": 5, "output": "kept locally"}')
        import os
        os.utime(old, ns=(boundary-1000000000, boundary-1000000000))
        os.utime(result, ns=(boundary+1000000000, boundary+1000000000))
        (self.root/'config.json').write_text('{"model": "fixture"}')
        matches, _ = session.result_candidates([], [self.root], boundary)
        self.assertEqual(matches, [str(result.resolve())])

    def test_result_discovery_uses_client_result_dir(self):
        client = self.root/'proc/42'
        client.mkdir(parents=True)
        results = self.root/'client-output'
        results.mkdir()
        result = results/'result.json'
        result.write_text('{"completed": 2, "request_throughput": 1.2}')
        (client/'cmdline').write_bytes(('vllm\0bench\0serve\0--result-dir\0'+str(results)+'\0').encode())
        matches, roots = session.result_candidates([42], [], 0, self.root/'proc')
        self.assertIn(str(result.resolve()), matches)
        self.assertIn(str(results), roots)

    def test_record_without_config_discovers_sources(self):
        source = self.root/'auto.log'
        source.write_bytes(b'Running: 1 Waiting: 2\n')
        out = self.root/'auto-run'
        child = Mock(pid=23456, returncode=0)
        child.poll.return_value = 0
        args = SimpleNamespace(config=None, out=out, node='pp0', run_id='auto-cpu',
                               auto_logs=False, duration=0, raw_mib=1, metadata_mib=1, reserve_mib=1)
        with patch.object(session.sys, 'platform', 'linux'), \
             patch.object(session.subprocess, 'Popen', return_value=child), \
             patch.object(session, 'discover', return_value=dict(logs=[str(source)], processes=[])), \
             patch.object(session.time, 'sleep', side_effect=KeyboardInterrupt), \
             contextlib.redirect_stdout(io.StringIO()):
            session.record(args)
        config = json.loads((out/'session.json').read_text())['config']
        self.assertEqual(config['node'], 'pp0')
        self.assertTrue(config['auto_config'])
        self.assertEqual((out/'raw/000000.bin').read_bytes(), source.read_bytes())


if __name__ == '__main__':
    unittest.main()
