"""Smoke tests for cryptoatlas. Standard library only, no network."""

import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptoatlas import TOOL_NAME, TOOL_VERSION, build, stats, query, export
from cryptoatlas.cli import main

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestMetadata(unittest.TestCase):
    def test_metadata(self):
        self.assertEqual(TOOL_NAME, "cryptoatlas")
        self.assertTrue(TOOL_VERSION)


class TestPipeline(unittest.TestCase):
    def test_offline_build_ingests_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "a.sqlite")
            s = build(db_path=db, live=False)
            self.assertGreater(s.inserted, 0)
            # The deliberately-invalid PII placeholder row must be rejected.
            self.assertGreaterEqual(s.rejected, 1)
            st = stats(db)
            self.assertGreater(st["record_count"], 0)
            self.assertEqual(st["is_synthetic_rows"], 0)

    def test_no_synthetic_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "a.sqlite")
            build(db_path=db, live=False)
            st = stats(db)
            self.assertEqual(st["is_synthetic_rows"], 0)

    def test_query_finds_entity(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "a.sqlite")
            build(db_path=db, live=False)
            rows = query("Binance", db_path=db)
            self.assertTrue(rows)
            self.assertTrue(all("binance" in r["entity_name"].lower() for r in rows))

    def test_every_row_has_source_url(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "a.sqlite")
            build(db_path=db, live=False)
            data = json.loads(export("json", db_path=db))
            self.assertTrue(data["records"])
            for r in data["records"]:
                self.assertTrue(r["source_url"].startswith(("http://", "https://")))


class TestCli(unittest.TestCase):
    def test_version_subprocess(self):
        proc = subprocess.run(
            [sys.executable, "-m", "cryptoatlas", "--version"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(TOOL_VERSION, proc.stdout)

    def test_build_stats_query_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "a.sqlite")
            self.assertEqual(main(["--db", db, "build", "--offline"]), 0)
            self.assertEqual(main(["--db", db, "stats"]), 0)
            self.assertEqual(main(["--db", db, "query", "Coinbase"]), 0)
            out = os.path.join(tmp, "out.csv")
            self.assertEqual(main(["--db", db, "export", "--format", "csv", "--out", out]), 0)
            self.assertTrue(os.path.getsize(out) > 0)

    def test_no_command_exits_2(self):
        self.assertEqual(main([]), 2)

    def test_sources(self):
        self.assertEqual(main(["sources"]), 0)


if __name__ == "__main__":
    unittest.main()
