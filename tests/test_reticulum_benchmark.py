#!/usr/bin/env python3
"""Regression checks for Linux process/disk sampling; no daemon required."""
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from reticulum_benchmark import allocated


class AllocationSampling(unittest.TestCase):
    def test_fd_permission_race_preserves_named_files_and_counts_miss(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data = root / 'data'
            data.write_bytes(b'payload')
            original = Path.iterdir

            def denied(path):
                if str(path).startswith('/proc/'):
                    raise PermissionError('injected exec/exit race')
                return original(path)

            misses = dict(proc_fd=0)
            with patch.object(Path, 'iterdir', denied):
                size = allocated(root, [SimpleNamespace(pid=123)], misses)
            self.assertEqual(size, data.stat().st_blocks * 512)
            self.assertEqual(misses, dict(proc_fd=1))

    def test_open_named_files_are_deduplicated_and_anonymous_files_counted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data = root / 'data'
            data.write_bytes(b'payload')
            (root / 'excluded.log').write_bytes(b'log')
            with data.open('rb'), tempfile.TemporaryFile(dir=root) as anonymous:
                anonymous.write(b'x' * 8192)
                anonymous.flush()
                expected = (data.stat().st_blocks + os.fstat(anonymous.fileno()).st_blocks) * 512
                misses = dict(proc_fd=0)
                self.assertEqual(allocated(root, [SimpleNamespace(pid=os.getpid())], misses), expected)
                self.assertEqual(misses, dict(proc_fd=0))


if __name__ == '__main__':
    unittest.main()
