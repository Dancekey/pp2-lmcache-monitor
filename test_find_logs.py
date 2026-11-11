import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import find_logs


class LogDiscoveryTests(unittest.TestCase):
    def test_existing_file_is_unique_and_command_secrets_are_not_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root/'service.log'
            log.write_text('synthetic local case\n', encoding='utf-8')
            proc = root/'123'
            proc.mkdir()
            (proc/'cmdline').write_bytes(b'python\0-m\0lmcache.v1.multiprocess.server\0--token\0NEVER_PRINT\0')
            with patch.object(find_logs.os, 'readlink', return_value=str(log)):
                value = find_logs.discover(root)
            self.assertEqual(value['logs'], [str(log)])
            self.assertEqual(len(value['processes']), 2)
            self.assertNotIn('NEVER_PRINT', json.dumps(value))

    def test_pipe_and_missing_proc_are_not_opened_as_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proc = root/'123'
            proc.mkdir()
            (proc/'cmdline').write_bytes(b'vllm\0serve\0synthetic\0')
            with patch.object(find_logs.os, 'readlink', return_value='pipe:[1234]'):
                value = find_logs.discover(root)
            self.assertEqual(value['logs'], [])
            self.assertTrue(all(not p['regular_file'] for p in value['processes']))
            self.assertEqual(find_logs.discover(root/'missing')['logs'], [])


if __name__ == '__main__':
    unittest.main()
