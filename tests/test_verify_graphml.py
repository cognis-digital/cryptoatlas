"""Tests for the verify + GraphML export features."""
import unittest
import xml.etree.ElementTree as ET

from cryptoatlas.core import verify_records, records_to_graphml, _address_ok


GOOD = [
    {"entity_name": "Binance", "entity_type": "exchange", "category": "reserves",
     "chain": "btc", "address": "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo",
     "source_url": "https://example.org/proof"},
    {"entity_name": "Strategy", "entity_type": "company", "category": "flows",
     "chain": "eth", "address": "0x52908400098527886E0F7030069857D2E4169EE7",
     "source_url": "https://sec.gov/filing"},
    {"entity_name": "El Salvador", "entity_type": "government", "category": "strategic_reserves",
     "chain": "btc", "address": "", "source_url": "https://example.gov/reserve"},
]


class TestAddressOk(unittest.TestCase):
    def test_sol_base58_not_int_base58(self):
        # regression: a valid Solana base58 address must pass (old draft used
        # int(addr, 58) which raises and wrongly failed every SOL row)
        self.assertTrue(_address_ok("So11111111111111111111111111111111111111112", "sol"))

    def test_eth_requires_40_hex(self):
        self.assertTrue(_address_ok("0x52908400098527886E0F7030069857D2E4169EE7", "eth"))
        self.assertFalse(_address_ok("0x123", "eth"))

    def test_empty_address_allowed(self):
        self.assertTrue(_address_ok("", "btc"))

    def test_unknown_chain_not_failed(self):
        self.assertTrue(_address_ok("whatever", "dogecoin"))


class TestVerifyRecords(unittest.TestCase):
    def test_all_good_passes(self):
        rep = verify_records(GOOD)
        self.assertTrue(rep["ok"])
        self.assertEqual(rep["passed"], rep["total"])

    def test_bad_source_fails(self):
        rows = [dict(GOOD[0], source_url="ftp://nope")]
        rep = verify_records(rows)
        self.assertFalse(rep["ok"])
        self.assertIn("source_url", rep["failed"][0]["reasons"][0])

    def test_malformed_address_fails(self):
        rows = [dict(GOOD[1], address="0xZZZ")]
        rep = verify_records(rows)
        self.assertFalse(rep["ok"])


class TestGraphML(unittest.TestCase):
    def test_valid_xml_with_nodes_and_edges(self):
        xml = records_to_graphml(GOOD)
        root = ET.fromstring(xml)  # raises if malformed
        ns = "{http://graphml.graphdrawing.org/xmlns}"
        graph = root.find(f"{ns}graph")
        self.assertIsNotNone(graph)
        nodes = graph.findall(f"{ns}node")
        edges = graph.findall(f"{ns}edge")
        # 3 entities + 2 distinct addresses (El Salvador has no address)
        self.assertEqual(len(nodes), 5)
        self.assertEqual(len(edges), 2)


if __name__ == "__main__":
    unittest.main()
