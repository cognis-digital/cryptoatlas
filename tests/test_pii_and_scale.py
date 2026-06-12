"""PII-rejection guardrails + scale-pipeline tests for cryptoatlas.

These tests are NETWORK-FREE: they validate the no-private-PII policy and the
structure of the expanded source catalog / fetcher registry without requiring
egress. The live fetchers themselves are exercised in test_deep's
"returns a list either way" pattern.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptoatlas.core import (
    Record, IngestError, validate, source_catalog, LIVE_FETCHERS,
    _classify_label, _ingest_token_list, _looks_like_pii, ENTITY_TYPES,
    CATEGORIES, CHAINS, fetch_ofac_sdn_mirror, fetch_etherscan_labels,
    fetch_uniswap_token_list, fetch_oneinch_token_lists, fetch_trustwallet_tokens,
    fetch_multichain_explorer_labels, fetch_coingecko_token_lists,
    fetch_defillama_protocols, _parse_scan_labels, _defillama_chain_addr,
    fetch_extra_token_lists, fetch_trustwallet_assets,
    fetch_opensanctions_crypto, _os_chain_for, _tw_address_ok,
)


def _rec(**kw):
    base = dict(address="0x71660c4005ba85c37ccec55d0c4493e66fe775d3",
                chain="ethereum", entity_name="Acme Exchange",
                entity_type="exchange", category="cex",
                source_url="https://example.com/proof")
    base.update(kw)
    return Record(**base)


class TestPiiRejection(unittest.TestCase):
    def test_email_in_entity_name_rejected(self):
        with self.assertRaises(IngestError):
            validate(_rec(entity_name="alice.private@gmail.com"))

    def test_email_in_notes_rejected(self):
        with self.assertRaises(IngestError):
            validate(_rec(notes="owner contact: bob@hotmail.com"))

    def test_phone_number_rejected(self):
        with self.assertRaises(IngestError):
            validate(_rec(notes="call the holder at +1 (415) 555-0199"))

    def test_ssn_rejected(self):
        with self.assertRaises(IngestError):
            validate(_rec(notes="taxpayer 123-45-6789"))

    def test_pii_marker_phrase_rejected(self):
        with self.assertRaises(IngestError):
            validate(_rec(notes="this is the private wallet of a retail user"))
        with self.assertRaises(IngestError):
            validate(_rec(balance_hint="home address on file"))

    def test_personal_name_tied_to_address_rejected(self):
        # First Last bound to a concrete address = deanonymization -> reject.
        with self.assertRaises(IngestError):
            validate(_rec(entity_name="John Smith"))
        with self.assertRaises(IngestError):
            validate(_rec(entity_name="Jane A. Doe"))

    def test_org_name_with_suffix_allowed(self):
        # An org/entity name (even two-word) is fine — it's public, entity-level.
        validate(_rec(entity_name="Block Inc"))
        validate(_rec(entity_name="Wintermute Trading"))
        validate(_rec(entity_name="Coinbase Custody"))

    def test_personal_name_without_address_allowed(self):
        # No address bound -> no real-world deanonymization link.
        validate(_rec(address="", entity_type="government",
                      category="nation-state", entity_name="John Smith"))

    def test_helper_detects_pii(self):
        self.assertIsNotNone(_looks_like_pii("x@y.com"))
        self.assertIsNotNone(_looks_like_pii("ssn 111-22-3333"))
        self.assertIsNone(_looks_like_pii("Binance hot wallet"))


class TestExpandedCatalog(unittest.TestCase):
    def test_catalog_grew(self):
        cat = source_catalog()
        self.assertGreaterEqual(len(cat), 16)
        ids = {s["id"] for s in cat}
        for want in ("ofac_sdn_mirror", "etherscan_labels", "uniswap_token_list",
                     "oneinch_token_lists", "trustwallet_assets",
                     "multichain_explorer_labels", "coingecko_token_lists",
                     "defillama_protocols"):
            self.assertIn(want, ids)

    def test_catalog_types_valid_and_attributed(self):
        for s in source_catalog():
            self.assertTrue(s["url"].startswith("http"))
            self.assertIn(s["type"], ENTITY_TYPES)

    def test_live_fetcher_registry(self):
        self.assertGreaterEqual(len(LIVE_FETCHERS), 9)
        for source_id, fn in LIVE_FETCHERS:
            self.assertTrue(callable(fn))
            self.assertIsInstance(source_id, str)


class TestClassification(unittest.TestCase):
    def test_exchange_classified(self):
        self.assertEqual(_classify_label("Binance: Hot Wallet", ["binance"]),
                         ("exchange", "cex"))

    def test_mixer_classified(self):
        et, cat = _classify_label("Tornado Cash", ["tornado-cash"])
        self.assertEqual((et, cat), ("mixer", "sanctioned-entity"))

    def test_bridge_classified(self):
        et, cat = _classify_label("Wormhole Bridge", ["bridge"])
        self.assertEqual((et, cat), ("bridge", "bridge-contract"))

    def test_default_is_contract_not_pii(self):
        et, cat = _classify_label("Some Random Contract", ["take-action"])
        self.assertEqual(et, "contract")
        self.assertIn(et, ENTITY_TYPES)


class TestTokenListNormalization(unittest.TestCase):
    def test_ingest_token_list_maps_chain_and_entity(self):
        toks = [
            {"chainId": 1, "address": "0x" + "a" * 40, "name": "TokenA", "symbol": "TKA"},
            {"chainId": 137, "address": "0x" + "b" * 40, "name": "TokenB", "symbol": "TKB"},
            {"chainId": 1, "address": "not-an-address", "name": "Bad"},  # skipped
        ]
        recs = _ingest_token_list(toks, "uniswap_token_list",
                                  "https://tokens.uniswap.org/", "ethereum", 100)
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].chain, "ethereum")
        self.assertEqual(recs[1].chain, "polygon")
        for r in recs:
            self.assertEqual(r.entity_type, "token")
            self.assertEqual(r.category, "token-contract")
            validate(r)  # must pass the no-PII validator

    def test_cap_respected(self):
        toks = [{"chainId": 1, "address": "0x" + f"{i:040x}", "name": f"T{i}"}
                for i in range(50)]
        recs = _ingest_token_list(toks, "x", "https://x/", "ethereum", 10)
        self.assertEqual(len(recs), 10)


class TestScanLabelParser(unittest.TestCase):
    """The per-chain explorer-label parser maps a combinedAllLabels.json blob
    onto validated, correctly-chained Records (no private PII)."""

    def test_parse_maps_chain_and_classifies(self):
        data = {
            "0x" + "1" * 40: {"name": "Binance: Hot Wallet", "labels": ["binance"]},
            "0x" + "2" * 40: {"name": "Wormhole Bridge", "labels": ["bridge"]},
            "0x" + "3" * 40: {"name": "MultiSig Vault Contract", "labels": []},
            "not-an-address": {"name": "skip me"},          # skipped (bad addr)
            "0x" + "4" * 40: {"labels": []},                 # skipped (no name)
        }
        recs = _parse_scan_labels(data, "bsc", "multichain_explorer_labels",
                                  "https://raw.githubusercontent.com/x/y.json", 100)
        self.assertEqual(len(recs), 3)
        for r in recs:
            self.assertEqual(r.chain, "bsc")
            self.assertEqual(r.label_source, "multichain_explorer_labels")
            validate(r)  # must pass the no-PII validator
        self.assertEqual(recs[0].entity_type, "exchange")
        self.assertEqual(recs[1].entity_type, "bridge")
        self.assertEqual(recs[2].entity_type, "contract")

    def test_parse_handles_non_dict(self):
        self.assertEqual(_parse_scan_labels(None, "bsc", "s", "https://x", 10), [])
        self.assertEqual(_parse_scan_labels([], "bsc", "s", "https://x", 10), [])


class TestDefillamaAddressParser(unittest.TestCase):
    def test_chain_prefixed_address(self):
        chain, addr = _defillama_chain_addr("arbitrum:0x" + "A" * 40)
        self.assertEqual(chain, "arbitrum")
        self.assertEqual(addr, "0x" + "a" * 40)

    def test_bare_address_is_ethereum(self):
        chain, addr = _defillama_chain_addr("0x" + "B" * 40)
        self.assertEqual(chain, "ethereum")
        self.assertEqual(addr, "0x" + "b" * 40)

    def test_unknown_chain_skipped(self):
        self.assertEqual(_defillama_chain_addr("nosuchchain:0x" + "c" * 40), ("", ""))

    def test_unparseable_address_skipped(self):
        self.assertEqual(_defillama_chain_addr("ethereum:0xabc"), ("", ""))
        self.assertEqual(_defillama_chain_addr(""), ("", ""))


class TestOpenSanctionsChainMap(unittest.TestCase):
    """OpenSanctions CryptoWallet -> verifiable cryptoatlas chain mapping."""

    def test_evm_shape_wins(self):
        # An EVM publicKey is mapped by shape regardless of declared currency.
        self.assertEqual(_os_chain_for("0x" + "a" * 40, "BSC"), "ethereum")

    def test_tron_shape_wins(self):
        self.assertEqual(_os_chain_for("T" + "1" * 33, "USDT"), "tron")

    def test_bech32_btc(self):
        self.assertEqual(_os_chain_for("bc1qabcdefghijklmnop", "BTC"), "bitcoin")

    def test_currency_fallback(self):
        self.assertEqual(_os_chain_for("Ltc1exampleaddr", "LTC"), "litecoin")
        self.assertEqual(_os_chain_for("Xexampleaddr", "XMR"), "monero")

    def test_unmappable_returns_empty(self):
        self.assertEqual(_os_chain_for("someaddr", "NOSUCH"), "")


class TestTrustWalletAddressGuard(unittest.TestCase):
    """The Trust Wallet asset-dir guard accepts only verifiable on-chain addrs."""

    def test_evm_ok(self):
        self.assertTrue(_tw_address_ok("0x" + "a" * 40, "ethereum"))
        self.assertTrue(_tw_address_ok("0x" + "b" * 40, "linea"))

    def test_solana_ok(self):
        self.assertTrue(_tw_address_ok("So11111111111111111111111111111111111111112",
                                       "solana"))

    def test_tron_ok(self):
        self.assertTrue(_tw_address_ok("T" + "1" * 33, "tron"))

    def test_malformed_rejected(self):
        self.assertFalse(_tw_address_ok("0xshort", "ethereum"))
        self.assertFalse(_tw_address_ok("0x" + "a" * 40, "cosmos"))  # unknown chain


class TestNewSourcesRegistered(unittest.TestCase):
    def test_new_sources_in_catalog_and_registry(self):
        ids = {s["id"] for s in source_catalog()}
        for want in ("trustwallet_assets_full", "extra_token_lists",
                     "opensanctions_crypto"):
            self.assertIn(want, ids)
        reg = {sid for sid, _ in LIVE_FETCHERS}
        for want in ("trustwallet_assets_full", "extra_token_lists",
                     "opensanctions_crypto"):
            self.assertIn(want, reg)

    def test_new_chains_present(self):
        for c in ("celo", "linea", "mantle", "scroll", "dogecoin"):
            self.assertIn(c, CHAINS)


class TestFetchersFailSoft(unittest.TestCase):
    """Every live fetcher must return a list (never raise), online or offline."""

    def test_all_return_lists(self):
        for fn in (fetch_ofac_sdn_mirror, fetch_etherscan_labels,
                   fetch_uniswap_token_list, fetch_oneinch_token_lists,
                   fetch_trustwallet_tokens, fetch_multichain_explorer_labels,
                   fetch_coingecko_token_lists, fetch_defillama_protocols,
                   fetch_extra_token_lists, fetch_trustwallet_assets,
                   fetch_opensanctions_crypto):
            self.assertIsInstance(fn(timeout=0.001), list)


if __name__ == "__main__":
    unittest.main()
