"""Deep tests for cryptoatlas: schema validation, dedupe, MCP, exports, OFAC parse."""

import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptoatlas.core import (
    Record, IngestError, validate, connect, upsert, build, stats,
    export, query, fetch_ofac_sdn, source_catalog, _OFAC_XBT_RE, _OFAC_ETH_RE,
    SEED_RECORDS, ENTITY_TYPES, CATEGORIES, CHAINS,
)
from cryptoatlas import mcp_server


def _valid_rec(**kw):
    base = dict(address="0x71660c4005ba85c37ccec55d0c4493e66fe775d3",
                chain="ethereum", entity_name="Test Exchange",
                entity_type="exchange", category="cex",
                source_url="https://example.com/proof")
    base.update(kw)
    return Record(**base)


class TestValidation(unittest.TestCase):
    def test_requires_name(self):
        with self.assertRaises(IngestError):
            validate(_valid_rec(entity_name=""))

    def test_rejects_unknown_type(self):
        with self.assertRaises(IngestError):
            validate(_valid_rec(entity_type="spy"))

    def test_rejects_unknown_chain(self):
        with self.assertRaises(IngestError):
            validate(_valid_rec(chain="notachain"))

    def test_rejects_unknown_category(self):
        with self.assertRaises(IngestError):
            validate(_valid_rec(category="private-person"))

    def test_requires_real_source_url(self):
        with self.assertRaises(IngestError):
            validate(_valid_rec(source_url="not-a-url"))

    def test_rejects_pii_in_address(self):
        # No-PII guardrail: an address with an @ (email-like) is rejected.
        with self.assertRaises(IngestError):
            validate(_valid_rec(address="alice@gmail.com"))

    def test_rejects_synthetic(self):
        with self.assertRaises(IngestError):
            validate(_valid_rec(is_synthetic=True))

    def test_entity_level_no_address_ok(self):
        # Treasury disclosures may have no single address.
        validate(_valid_rec(address="", entity_type="treasury",
                            category="public-company"))

    def test_vocab_consistency(self):
        self.assertIn("exchange", ENTITY_TYPES)
        self.assertIn("cex", CATEGORIES)
        self.assertIn("bitcoin", CHAINS)


class TestDedupe(unittest.TestCase):
    def test_same_address_dedupes(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(os.path.join(tmp, "d.sqlite"))
            r = _valid_rec()
            self.assertTrue(upsert(conn, r))
            self.assertFalse(upsert(conn, r))  # second insert = duplicate
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM records").fetchone()[0], 1)
            conn.close()

    def test_key_stable(self):
        self.assertEqual(_valid_rec().key(), _valid_rec().key())


class TestSeedIntegrity(unittest.TestCase):
    def test_seed_rows_attributed(self):
        for raw in SEED_RECORDS:
            self.assertIn("source_url", raw)
            self.assertTrue(raw["source_url"].startswith("http"))

    def test_seed_has_each_major_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "s.sqlite")
            build(db_path=db, live=False)
            st = stats(db)
            for t in ("exchange", "treasury", "etf", "seizure", "reserve"):
                self.assertIn(t, st["by_entity_type"], t)


class TestExport(unittest.TestCase):
    def test_csv_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "e.sqlite")
            build(db_path=db, live=False)
            csv_text = export("csv", db_path=db)
            self.assertIn("entity_name", csv_text.splitlines()[0])
            self.assertGreater(len(csv_text.splitlines()), 1)

    def test_json_export_scope_note(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "e.sqlite")
            build(db_path=db, live=False)
            data = json.loads(export("json", db_path=db))
            self.assertIn("no private-individual PII", data["scope"])

    def test_bad_format_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "e.sqlite")
            build(db_path=db, live=False)
            with self.assertRaises(ValueError):
                export("yaml", db_path=db)


class TestOfacParser(unittest.TestCase):
    def test_xbt_regex(self):
        sample = "Digital Currency Address - XBT 1Q9UAQHFQPLEUSGTBHGEUH7DERZWQ3QQ8E"
        m = _OFAC_XBT_RE.search(sample)
        self.assertIsNotNone(m)

    def test_eth_regex(self):
        sample = "Digital Currency Address - ETH 0x098B716B8Aaf21512996dC57EB0615e2383E2f96"
        m = _OFAC_ETH_RE.search(sample)
        self.assertIsNotNone(m)

    def test_fetch_returns_list(self):
        # Network may be blocked; must return a list either way (never raise).
        result = fetch_ofac_sdn(timeout=2.0)
        self.assertIsInstance(result, list)


class TestSourceCatalog(unittest.TestCase):
    def test_catalog_nonempty_and_attributed(self):
        cat = source_catalog()
        self.assertGreaterEqual(len(cat), 6)
        for s in cat:
            self.assertTrue(s["url"].startswith("http"))
            self.assertIn(s["type"], ENTITY_TYPES)


class TestMcpServer(unittest.TestCase):
    def setUp(self):
        # MCP server uses the default DB; ensure it is populated.
        build(live=False)

    def _rpc(self, req):
        return mcp_server.handle_request(req)

    def test_initialize(self):
        res = self._rpc({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
        self.assertEqual(res["result"]["serverInfo"]["name"], "cryptoatlas")

    def test_tools_list(self):
        res = self._rpc({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        names = {t["name"] for t in res["result"]["tools"]}
        self.assertEqual(names, {"query", "stats", "sources"})

    def test_tools_call_query(self):
        res = self._rpc({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                         "params": {"name": "query", "arguments": {"term": "Binance"}}})
        self.assertFalse(res["result"]["isError"])
        text = res["result"]["content"][0]["text"]
        self.assertIn("Binance", text)

    def test_tools_call_stats(self):
        res = self._rpc({"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                         "params": {"name": "stats", "arguments": {}}})
        payload = json.loads(res["result"]["content"][0]["text"])
        self.assertIn("record_count", payload)

    def test_unknown_method(self):
        res = self._rpc({"jsonrpc": "2.0", "id": 5, "method": "bogus"})
        self.assertEqual(res["error"]["code"], -32601)

    def test_notification_returns_none(self):
        self.assertIsNone(self._rpc({"jsonrpc": "2.0", "method": "initialize"}))

    def test_run_loop_parse_error(self):
        out = io.StringIO()
        mcp_server.run_mcp_server(stdin=io.StringIO("{ not json\n"), stdout=out)
        self.assertIn("parse error", out.getvalue())


if __name__ == "__main__":
    unittest.main()
