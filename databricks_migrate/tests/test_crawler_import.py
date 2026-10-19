from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class CrawlerImportTests(unittest.TestCase):
    def test_crawler_import_does_not_require_talib(self) -> None:
        script = """
import sys
sys.modules['talib'] = None
sys.modules['talib.abstract'] = None
import finlab.crawler
assert 'finlab.data' not in sys.modules
"""
        environment = os.environ.copy()
        environment["PYTHONPATH"] = os.pathsep.join(
            part
            for part in (str(PROJECT_ROOT), environment.get("PYTHONPATH", ""))
            if part
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
