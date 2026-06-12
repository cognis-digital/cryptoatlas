"""cryptoatlas core — schema, ingestion/enrichment pipeline, and query engine.

cryptoatlas is an OPEN, self-hostable dataset of PUBLIC crypto entities and
their on-chain addresses: exchanges, funds, ETFs, public-company treasuries,
governments, law-enforcement seizures, strategic reserves, and labeled whale
clusters.

STRICT SCOPE — entity-level, public-disclosure data ONLY:
  * NO private-individual PII.
  * NO deanonymization of private persons.
  * Every row records the REAL public ``source_url`` it came from.

The pipeline uses the Python standard library only (urllib/json/sqlite3/csv).
When a live source is reachable it is fetched and parsed; when the environment
blocks egress, the pipeline falls back to a small, fully-attributed PUBLIC seed
so the dataset is never fabricated and always provenance-tagged.
"""

from __future__ import annotations

import concurrent.futures as _cf
import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import urllib.error
import urllib.request
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

TOOL_NAME = "cryptoatlas"
TOOL_VERSION = "0.1.0"

DEFAULT_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "data", "cryptoatlas.sqlite")

# ---------------------------------------------------------------------------
# Controlled vocabularies
# ---------------------------------------------------------------------------

ENTITY_TYPES = (
    "exchange", "fund", "etf", "treasury", "government",
    "seizure", "reserve", "whale", "sanctioned", "mixer", "service",
    "contract", "protocol", "token", "bridge",
)

CATEGORIES = (
    "cex", "defi", "etf-spot", "public-company", "nation-state",
    "law-enforcement", "strategic-reserve", "labeled-cluster",
    "sanctioned-entity", "infrastructure",
    "token-contract", "defi-protocol", "bridge-contract",
)

CHAINS = (
    "bitcoin", "ethereum", "tron", "solana", "litecoin",
    "bitcoin-cash", "ripple", "polygon", "arbitrum", "multi",
    "bsc", "optimism", "base", "avalanche", "fantom", "gnosis",
    "ethereum-classic", "monero", "zcash", "dash", "verge",
    "zksync", "cronos", "blast",
    # additional public EVM / L2 / sidechain platforms (token-contract labels)
    "celo", "linea", "mantle", "scroll", "moonbeam", "moonriver",
    "aurora", "metis", "kava", "harmony", "heco", "okc", "kcc",
    "boba", "fuse", "polygon-zkevm", "core", "sonic", "ronin",
    "pulsechain", "astar", "dogechain", "manta", "mode", "fraxtal",
    "rootstock", "dogecoin",
)


# ---------------------------------------------------------------------------
# Record schema
# ---------------------------------------------------------------------------

@dataclass
class Record:
    """A single public crypto-entity address record.

    is_synthetic is always False: cryptoatlas never fabricates rows.
    """
    address: str
    chain: str
    entity_name: str
    entity_type: str
    category: str
    balance_hint: str = ""
    label_source: str = ""
    source_url: str = ""
    first_seen: str = ""
    notes: str = ""
    is_synthetic: bool = False

    def key(self) -> str:
        """Stable dedupe key: chain + normalized address (or name when no addr)."""
        addr = (self.address or "").strip().lower()
        chain = (self.chain or "").strip().lower()
        basis = f"{chain}|{addr}" if addr else f"name|{self.entity_name.strip().lower()}"
        return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:32]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class IngestError(Exception):
    """Raised when a record fails validation before insertion."""


# PII guardrails (entity-level public data ONLY; reject anything that looks like
# a private real-world identity attached to an address).
_RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_RE_PHONE = re.compile(r"(?<!\w)(?:\+?\d[\d ().-]{7,}\d)(?!\w)")
_RE_SSN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
# "John Smith" / "Jane A. Doe" shaped personal names used as the *entity*.
_RE_PERSONAL_NAME = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+$")
# Tokens that signal a private individual rather than a public org/entity.
_PII_HINTS = ("home address", "personal address", "date of birth", "passport",
              "national id", "ssn", "private individual", "private person",
              "private wallet of")


def _looks_like_pii(*fields: str) -> Optional[str]:
    """Return a reason string if any field carries private-person PII, else None."""
    for f in fields:
        if not f:
            continue
        if _RE_EMAIL.search(f):
            return "email address detected"
        if _RE_SSN.search(f):
            return "government id (SSN) detected"
        if _RE_PHONE.search(f):
            return "phone number detected"
        low = f.lower()
        for hint in _PII_HINTS:
            if hint in low:
                return f"private-PII marker detected: {hint!r}"
    return None


def validate(rec: Record) -> Record:
    """Validate a record against the schema. Raises IngestError on violation.

    Enforces the hard scope: PUBLIC, entity-level only. Any record that looks
    like private-individual PII (email/phone/gov-id, a personal-name entity, or
    a PII marker phrase in any field) is rejected.
    """
    if not rec.entity_name or not rec.entity_name.strip():
        raise IngestError("entity_name is required")
    if rec.entity_type not in ENTITY_TYPES:
        raise IngestError(f"unknown entity_type: {rec.entity_type!r}")
    if rec.category not in CATEGORIES:
        raise IngestError(f"unknown category: {rec.category!r}")
    if rec.chain not in CHAINS:
        raise IngestError(f"unknown chain: {rec.chain!r}")
    if not rec.source_url or not rec.source_url.startswith(("http://", "https://")):
        raise IngestError("source_url must be a real public URL")
    if rec.is_synthetic:  # pragma: no cover - guardrail
        raise IngestError("synthetic rows are not permitted in cryptoatlas")

    # No-PII guardrail across all human-readable fields.
    reason = _looks_like_pii(rec.entity_name, rec.notes, rec.balance_hint, rec.label_source)
    if reason:
        raise IngestError(f"record rejected (no private PII): {reason}")
    # Deanonymization guard: a private individual's full name tied to a concrete
    # on-chain address is a real-world identity link — reject. (A bare org/entity
    # name with no address, or with an org suffix, is fine; this only fires when a
    # FirstName LastName pattern is bound to an actual address.)
    name = rec.entity_name.strip()
    has_org_suffix = bool(re.search(
        r"\b(inc|llc|ltd|corp|trust|fund|capital|labs?|exchange|dao|protocol|"
        r"foundation|group|holdings|ventures|partners|gmbh|sa|ag|plc|government|"
        r"treasury|bridge|finance|network|token|swap|wallet|sdn|trading|markets?|"
        r"systems?|technolog\w*|digital|global|custody|securities|bank|reserve)\b",
        name, re.I))
    if rec.address and not has_org_suffix and _RE_PERSONAL_NAME.match(name):
        raise IngestError("entity_name looks like a private individual's name tied "
                          "to an address; cryptoatlas is entity-level public data "
                          "only (no deanonymization)")

    # Address may be empty (entity-level disclosure without a single address),
    # but if present it must look like an on-chain identifier, not PII.
    if rec.address:
        if "@" in rec.address or " " in rec.address.strip():
            raise IngestError("address must be an on-chain identifier, not PII")
    return rec


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    id           TEXT PRIMARY KEY,
    address      TEXT,
    chain        TEXT NOT NULL,
    entity_name  TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    category     TEXT NOT NULL,
    balance_hint TEXT,
    label_source TEXT,
    source_url   TEXT NOT NULL,
    first_seen   TEXT,
    notes        TEXT,
    is_synthetic INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_entity_name ON records(entity_name);
CREATE INDEX IF NOT EXISTS idx_entity_type ON records(entity_type);
CREATE INDEX IF NOT EXISTS idx_address     ON records(address);
CREATE INDEX IF NOT EXISTS idx_source      ON records(label_source);
"""


def connect(db_path: str = DEFAULT_DB) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    return conn


def upsert(conn: sqlite3.Connection, rec: Record) -> bool:
    """Insert (or replace) a record. Returns True if newly inserted."""
    validate(rec)
    rid = rec.key()
    existing = conn.execute("SELECT 1 FROM records WHERE id = ?", (rid,)).fetchone()
    conn.execute(
        """INSERT OR REPLACE INTO records
           (id, address, chain, entity_name, entity_type, category,
            balance_hint, label_source, source_url, first_seen, notes, is_synthetic)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (rid, rec.address, rec.chain, rec.entity_name, rec.entity_type,
         rec.category, rec.balance_hint, rec.label_source, rec.source_url,
         rec.first_seen, rec.notes, 0),
    )
    return existing is None


def record_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]


# ---------------------------------------------------------------------------
# Source catalog — the public label sets the pipeline can grow toward.
# ---------------------------------------------------------------------------

SOURCE_CATALOG: List[Dict[str, str]] = [
    {
        "id": "ofac_sdn_crypto",
        "name": "OFAC SDN crypto digital-currency addresses",
        "url": "https://www.treasury.gov/ofac/downloads/sdn.xml",
        "type": "sanctioned",
        "notes": "U.S. Treasury OFAC SDN list. 'Digital Currency Address' IDs are "
                 "extracted. Public, machine-readable, authoritative.",
    },
    {
        "id": "ofac_sdn_advanced_json",
        "name": "OFAC SDN advanced JSON feed",
        "url": "https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.xml",
        "type": "sanctioned",
        "notes": "Structured OFAC feed with feature-type crypto addresses.",
    },
    {
        "id": "gov_btc_treasuries",
        "name": "Government & public-company BTC holdings (disclosed)",
        "url": "https://bitcointreasuries.net/",
        "type": "treasury",
        "notes": "Public-company / government BTC treasury disclosures. Entity-level "
                 "totals from filings; addresses where publicly attributed.",
    },
    {
        "id": "us_marshals_seizures",
        "name": "US Marshals / DOJ crypto seizure disclosures",
        "url": "https://www.justice.gov/usao/pressreleases",
        "type": "seizure",
        "notes": "DOJ/USMS press releases disclosing seized wallet addresses.",
    },
    {
        "id": "spot_etf_custody",
        "name": "Spot BTC/ETH ETF custody disclosures",
        "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany",
        "type": "etf",
        "notes": "SEC EDGAR filings for spot ETFs; custodian-attributed addresses "
                 "where disclosed by the trust.",
    },
    {
        "id": "exchange_cold_wallets",
        "name": "Exchange cold-wallet labels (public attestations)",
        "url": "https://github.com/etherscan-labels/etherscan-labels",
        "type": "exchange",
        "notes": "Community/proof-of-reserves attested exchange labels. Public "
                 "address-to-entity mapping, no PII.",
    },
    {
        "id": "rich_list_snapshot",
        "name": "Public rich-list snapshots (labeled clusters only)",
        "url": "https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html",
        "type": "whale",
        "notes": "Only addresses that carry a PUBLIC entity label are ingested. "
                 "Unlabeled private addresses are intentionally skipped (no deanon).",
    },
    {
        "id": "strategic_reserve",
        "name": "National strategic crypto-reserve disclosures",
        "url": "https://www.whitehouse.gov/presidential-actions/",
        "type": "reserve",
        "notes": "Government strategic-reserve announcements and disclosed holdings.",
    },
    # --- Scale sources (verified fetchable; full multi-chain public label sets) ---
    {
        "id": "ofac_sdn_mirror",
        "name": "OFAC SDN sanctioned digital-currency addresses (per-chain mirror)",
        "url": "https://github.com/0xB10C/ofac-sanctioned-digital-currency-addresses",
        "type": "sanctioned",
        "notes": "Public per-chain extraction of every OFAC SDN 'Digital Currency "
                 "Address' (ARB/BCH/BSC/DASH/ETC/ETH/LTC/SOL/TRX/USDC/USDT/XBT/XMR/"
                 "XRP/XVG/ZEC). Authoritative SDN content, machine-readable.",
    },
    {
        "id": "etherscan_labels",
        "name": "Etherscan public address labels (combined)",
        "url": "https://github.com/brianleect/etherscan-labels",
        "type": "service",
        "notes": "~30k Ethereum addresses each carrying a PUBLIC Etherscan entity/"
                 "protocol/contract label (exchanges, DEXes, protocols, bridges, "
                 "contracts). Address->entity mapping only; no private PII.",
    },
    {
        "id": "uniswap_token_list",
        "name": "Uniswap default token list (issuer-labeled contracts)",
        "url": "https://tokens.uniswap.org/",
        "type": "token",
        "notes": "Canonical token-contract list: each ERC-20 contract address is "
                 "mapped to its public issuer/project name and chain.",
    },
    {
        "id": "oneinch_token_lists",
        "name": "1inch multi-chain token lists",
        "url": "https://tokens.1inch.io/",
        "type": "token",
        "notes": "Per-chain token-contract->issuer maps for Ethereum, BSC, Polygon, "
                 "Arbitrum, Optimism. Entity-level contract labels.",
    },
    {
        "id": "trustwallet_assets",
        "name": "Trust Wallet assets token lists",
        "url": "https://github.com/trustwallet/assets",
        "type": "token",
        "notes": "Community-maintained multi-chain token-contract registry with "
                 "issuer names; public, entity/contract-level.",
    },
    {
        "id": "defi_bridge_contracts",
        "name": "Public cross-chain bridge contract labels",
        "url": "https://github.com/etherscan-labels/etherscan-labels",
        "type": "bridge",
        "notes": "Labeled bridge/protocol contract addresses from public label sets.",
    },
    {
        "id": "multichain_explorer_labels",
        "name": "Multi-chain block-explorer public labels (BSC/Polygon/Arbitrum/"
                "Optimism/Fantom/Avalanche)",
        "url": "https://github.com/brianleect/etherscan-labels",
        "type": "service",
        "notes": "Public per-chain combinedAllLabels.json exports for BscScan, "
                 "PolygonScan, Arbiscan, Optimism, FTMScan and Avalanche. Each "
                 "address carries a PUBLIC explorer entity/protocol/contract "
                 "label; address->entity mapping only, no private PII.",
    },
    {
        "id": "coingecko_token_lists",
        "name": "CoinGecko per-platform token lists",
        "url": "https://tokens.coingecko.com/uniswap/all.json",
        "type": "token",
        "notes": "CoinGecko publishes a Uniswap-schema token list per chain "
                 "platform (Ethereum/Polygon/BSC/Arbitrum/Optimism/Avalanche/"
                 "Base/Fantom/Gnosis). Token-contract -> issuer/project labels.",
    },
    {
        "id": "trustwallet_assets_full",
        "name": "Trust Wallet assets — full multi-chain token registry",
        "url": "https://github.com/trustwallet/assets",
        "type": "token",
        "notes": "Every blockchains/<chain>/assets/<address>/info.json across the "
                 "Trust Wallet assets repo (EVM/Solana/Tron chains we can verify). "
                 "Enumerated via one git-tree call, names from each info.json. "
                 "Token-contract -> issuer label; public, entity/contract-level.",
    },
    {
        "id": "extra_token_lists",
        "name": "Additional public Uniswap-schema token lists",
        "url": "https://api.coinmarketcap.com/data-api/v3/uniswap/all.json",
        "type": "token",
        "notes": "CoinMarketCap, PancakeSwap (extended+top100), Optimism, "
                 "Arbitrum-bridged, Kleros T2CR, Aave, Compound, Gemini and Set "
                 "Protocol token lists. Token-contract -> issuer/project labels; "
                 "entity/contract-level, no private PII.",
    },
    {
        "id": "opensanctions_crypto",
        "name": "OpenSanctions consolidated sanctioned crypto addresses",
        "url": "https://data.opensanctions.org/datasets/latest/sanctions/entities.ftm.json",
        "type": "sanctioned",
        "notes": "CryptoWallet entities from the OpenSanctions consolidated "
                 "sanctions dataset (aggregates OFAC + EU + UN + UK OFSI + more "
                 "public lists). Each publicKey -> verifiable chain. Entity-level "
                 "sanctioned-address labels only; no private-individual PII.",
    },
    {
        "id": "defillama_protocols",
        "name": "DefiLlama public protocol registry",
        "url": "https://api.llama.fi/protocols",
        "type": "protocol",
        "notes": "DefiLlama's public protocol list; each protocol's on-chain "
                 "governance/token/bridge contract address is mapped to its "
                 "entity name + category (DeFi protocol, CEX, bridge). "
                 "Entity/contract-level only, no private PII.",
    },
    {
        "id": "solana_token_lists",
        "name": "Solana SPL token registries (Solana Labs + Jupiter)",
        "url": "https://raw.githubusercontent.com/solana-labs/token-list/main/"
               "src/tokens/solana.tokenlist.json",
        "type": "token",
        "notes": "Public Solana mainnet SPL token registries: the Solana Labs "
                 "token-list and the Jupiter aggregator token list. Each base58 "
                 "mint-contract address -> issuer/project name. Token/contract-"
                 "level only, no private PII.",
    },
    {
        "id": "nft_contract_lists",
        "name": "Public NFT collection CONTRACT registries (0xsequence)",
        "url": "https://github.com/0xsequence/token-directory",
        "type": "contract",
        "notes": "Per-chain ERC-721 / ERC-1155 NFT collection CONTRACT lists from "
                 "the public 0xsequence token-directory (mainnet/Arbitrum/"
                 "Avalanche/Base/BNB/Gnosis/Optimism/Polygon/Polygon-zkEVM/Sonic). "
                 "Each row is a collection's deployed contract address + public "
                 "name — CONTRACTS not holders; entity/contract-level, no PII.",
    },
    {
        "id": "stargate_tokens",
        "name": "Stargate bridgeable-token registry",
        "url": "https://stargate.finance/api/tokens",
        "type": "token",
        "notes": "Stargate's public list of cross-chain bridgeable tokens; each "
                 "chainKey + on-chain address -> token name/symbol across EVM/"
                 "Solana/Tron chains. Token-contract labels only; entity/contract-"
                 "level, no private PII.",
    },
]


# ---------------------------------------------------------------------------
# PUBLIC seed — small, hand-verified, fully-attributed rows.
# Every entry is a publicly-disclosed entity address with a real source_url.
# Used directly and as the offline fallback when live fetch is blocked.
# ---------------------------------------------------------------------------

SEED_RECORDS: List[Dict[str, str]] = [
    # --- Sanctioned (OFAC SDN, public) ---
    {
        "address": "12QtD5BFwRsdNsAZY76UVE1xyCGNTojH9h",
        "chain": "bitcoin", "entity_name": "OFAC SDN listed address (sample)",
        "entity_type": "sanctioned", "category": "sanctioned-entity",
        "label_source": "ofac_sdn_crypto",
        "source_url": "https://www.treasury.gov/ofac/downloads/sdn.xml",
        "notes": "Digital Currency Address XBT from OFAC SDN; entity-level sanction.",
    },
    {
        "address": "0x098b716b8aaf21512996dc57eb0615e2383e2f96",
        "chain": "ethereum", "entity_name": "Tornado Cash (OFAC SDN)",
        "entity_type": "mixer", "category": "sanctioned-entity",
        "label_source": "ofac_sdn_crypto",
        "source_url": "https://home.treasury.gov/news/press-releases/jy0916",
        "notes": "Sanctioned mixer contract; OFAC designation Aug 2022.",
    },
    {
        "address": "0x8589427373d6d84e98730d7795d8f6f8731fda16",
        "chain": "ethereum", "entity_name": "Tornado Cash Donation (OFAC SDN)",
        "entity_type": "mixer", "category": "sanctioned-entity",
        "label_source": "ofac_sdn_crypto",
        "source_url": "https://home.treasury.gov/news/press-releases/jy0916",
        "notes": "OFAC-listed Tornado Cash associated address.",
    },
    # --- Exchanges (public cold wallets / attestations) ---
    {
        "address": "34xp4vrocgjym3xr7ycvpfhocnxv4twseo",
        "chain": "bitcoin", "entity_name": "Binance",
        "entity_type": "exchange", "category": "cex",
        "label_source": "exchange_cold_wallets", "balance_hint": "large",
        "source_url": "https://www.binance.com/en/proof-of-reserves",
        "notes": "Binance cold wallet, proof-of-reserves attested.",
    },
    {
        "address": "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97",
        "chain": "bitcoin", "entity_name": "Binance",
        "entity_type": "exchange", "category": "cex", "balance_hint": "very-large",
        "label_source": "exchange_cold_wallets",
        "source_url": "https://www.binance.com/en/proof-of-reserves",
        "notes": "Binance segwit cold wallet, public PoR attestation.",
    },
    {
        "address": "0x71660c4005ba85c37ccec55d0c4493e66fe775d3",
        "chain": "ethereum", "entity_name": "Coinbase",
        "entity_type": "exchange", "category": "cex", "balance_hint": "large",
        "label_source": "exchange_cold_wallets",
        "source_url": "https://github.com/etherscan-labels/etherscan-labels",
        "notes": "Coinbase hot/cold wallet, public Etherscan label.",
    },
    {
        "address": "0x503828976d22510aad0201ac7ec88293211d23da",
        "chain": "ethereum", "entity_name": "Coinbase",
        "entity_type": "exchange", "category": "cex",
        "label_source": "exchange_cold_wallets",
        "source_url": "https://github.com/etherscan-labels/etherscan-labels",
        "notes": "Coinbase labeled deposit wallet.",
    },
    {
        "address": "0xdc76cd25977e0a5ae17155770273ad58648900d3",
        "chain": "ethereum", "entity_name": "Huobi",
        "entity_type": "exchange", "category": "cex",
        "label_source": "exchange_cold_wallets",
        "source_url": "https://github.com/etherscan-labels/etherscan-labels",
        "notes": "Huobi exchange wallet, public label.",
    },
    {
        "address": "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be",
        "chain": "ethereum", "entity_name": "Binance",
        "entity_type": "exchange", "category": "cex",
        "label_source": "exchange_cold_wallets",
        "source_url": "https://github.com/etherscan-labels/etherscan-labels",
        "notes": "Binance hot wallet, public Etherscan label.",
    },
    {
        "address": "0xtkdd@kraken",  # intentionally invalid -> filtered out by validate()
        "chain": "ethereum", "entity_name": "INVALID PLACEHOLDER",
        "entity_type": "exchange", "category": "cex",
        "label_source": "exchange_cold_wallets",
        "source_url": "https://example.com",
        "notes": "Will be rejected by validate(); proves no-PII / address hygiene.",
    },
    # --- Public-company treasuries ---
    {
        "address": "", "chain": "bitcoin", "entity_name": "MicroStrategy (Strategy Inc.)",
        "entity_type": "treasury", "category": "public-company",
        "balance_hint": "~214,000 BTC (disclosed)", "label_source": "gov_btc_treasuries",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=MSTR",
        "notes": "Largest corporate BTC treasury; per SEC 8-K disclosures.",
    },
    {
        "address": "", "chain": "bitcoin", "entity_name": "Tesla, Inc.",
        "entity_type": "treasury", "category": "public-company",
        "balance_hint": "~9,720 BTC (disclosed)", "label_source": "gov_btc_treasuries",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=TSLA",
        "notes": "Disclosed BTC holdings per 10-Q filings.",
    },
    {
        "address": "", "chain": "bitcoin", "entity_name": "Block, Inc.",
        "entity_type": "treasury", "category": "public-company",
        "balance_hint": "~8,000 BTC (disclosed)", "label_source": "gov_btc_treasuries",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=SQ",
        "notes": "Disclosed corporate BTC treasury.",
    },
    # --- Governments / seizures ---
    {
        "address": "bc1qa5wkgaew2dkv56kfvj49j0av5nml45x9ek9hz6",
        "chain": "bitcoin", "entity_name": "US Government (Bitfinex/Silk Road seizure)",
        "entity_type": "seizure", "category": "law-enforcement",
        "balance_hint": "very-large", "label_source": "us_marshals_seizures",
        "source_url": "https://www.justice.gov/opa/pr/justice-department-investigation-leads-takedown-darknet-cryptocurrency-wallet-over-1-billion",
        "notes": "DOJ-disclosed seized wallet (Silk Road, ~69,370 BTC).",
    },
    {
        "address": "", "chain": "bitcoin", "entity_name": "Government of El Salvador",
        "entity_type": "government", "category": "nation-state",
        "balance_hint": "~6,000 BTC (disclosed)", "label_source": "strategic_reserve",
        "source_url": "https://bitcoin.gob.sv/",
        "notes": "National BTC holdings published on official government tracker.",
    },
    {
        "address": "", "chain": "bitcoin", "entity_name": "United States Strategic Bitcoin Reserve",
        "entity_type": "reserve", "category": "strategic-reserve",
        "balance_hint": "seized-assets basis", "label_source": "strategic_reserve",
        "source_url": "https://www.whitehouse.gov/presidential-actions/",
        "notes": "Strategic Bitcoin Reserve established via Executive Order (2025).",
    },
    # --- ETFs ---
    {
        "address": "", "chain": "bitcoin", "entity_name": "iShares Bitcoin Trust (IBIT)",
        "entity_type": "etf", "category": "etf-spot",
        "balance_hint": "large (Coinbase Custody)", "label_source": "spot_etf_custody",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=ishares+bitcoin",
        "notes": "BlackRock spot BTC ETF; custody disclosed via SEC filings.",
    },
    {
        "address": "", "chain": "bitcoin", "entity_name": "Grayscale Bitcoin Trust (GBTC)",
        "entity_type": "etf", "category": "etf-spot",
        "balance_hint": "large", "label_source": "spot_etf_custody",
        "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=grayscale+bitcoin",
        "notes": "Grayscale spot BTC ETF; custodian-held.",
    },
    # --- Funds / labeled whale clusters ---
    {
        "address": "0xb1adceddb2941033a090dd166a462fe1c2029484",
        "chain": "ethereum", "entity_name": "Wintermute (market maker)",
        "entity_type": "fund", "category": "labeled-cluster",
        "label_source": "rich_list_snapshot",
        "source_url": "https://github.com/etherscan-labels/etherscan-labels",
        "notes": "Publicly-labeled market-maker cluster; entity-level.",
    },
    {
        "address": "bc1q0xcqpzrky6eff2g52qdye53xkk9jxkvrh6yhyw",
        "chain": "bitcoin", "entity_name": "Bitfinex (cold storage)",
        "entity_type": "exchange", "category": "cex", "balance_hint": "very-large",
        "label_source": "rich_list_snapshot",
        "source_url": "https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html",
        "notes": "Publicly-labeled exchange cold wallet on the rich list.",
    },
]


# ---------------------------------------------------------------------------
# Fetch helpers (stdlib urllib, fail-soft)
# ---------------------------------------------------------------------------

def _http_get(url: str, timeout: float = 8.0) -> Optional[bytes]:
    """Fetch a URL with urllib. Returns bytes, or None on any failure."""
    req = urllib.request.Request(
        url, headers={"User-Agent": f"{TOOL_NAME}/{TOOL_VERSION} (+public-data)"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError):
        return None


_OFAC_XBT_RE = re.compile(r"Digital Currency Address - XBT;?\s*([13][a-km-zA-HJ-NP-Z1-9]{25,39}|bc1[a-z0-9]{20,80})", re.I)
_OFAC_ETH_RE = re.compile(r"Digital Currency Address - ETH;?\s*(0x[a-fA-F0-9]{40})", re.I)


def fetch_ofac_sdn(timeout: float = 8.0) -> List[Record]:
    """Pull OFAC SDN crypto addresses (public). Returns [] if unreachable."""
    url = "https://www.treasury.gov/ofac/downloads/sdn.xml"
    raw = _http_get(url, timeout=timeout)
    if not raw:
        return []
    text = raw.decode("utf-8", errors="replace")
    out: List[Record] = []
    for chain, rx in (("bitcoin", _OFAC_XBT_RE), ("ethereum", _OFAC_ETH_RE)):
        for m in rx.finditer(text):
            addr = m.group(1)
            out.append(Record(
                address=addr.lower() if chain == "ethereum" else addr,
                chain=chain,
                entity_name="OFAC SDN sanctioned address",
                entity_type="sanctioned", category="sanctioned-entity",
                label_source="ofac_sdn_crypto", source_url=url,
                notes="Digital Currency Address extracted from OFAC SDN feed.",
            ))
    return out


def _http_get_json(url: str, timeout: float = 20.0) -> Optional[Any]:
    raw = _http_get(url, timeout=timeout)
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8", errors="replace"))
    except (ValueError, UnicodeDecodeError):
        return None


# Per-chain ticker -> (chain id, on-chain shape) used by the OFAC mirror feed.
_OFAC_MIRROR_BASE = ("https://raw.githubusercontent.com/0xB10C/"
                     "ofac-sanctioned-digital-currency-addresses/lists/"
                     "sanctioned_addresses_{tk}.txt")
_OFAC_MIRROR_CHAINS = {
    "ARB": "arbitrum", "BCH": "bitcoin-cash", "BSC": "bsc", "DASH": "dash",
    "ETC": "ethereum-classic", "ETH": "ethereum", "LTC": "litecoin",
    "SOL": "solana", "TRX": "tron", "USDC": "ethereum", "USDT": "ethereum",
    "XBT": "bitcoin", "XMR": "monero", "XRP": "ripple", "XVG": "verge",
    "ZEC": "zcash",
}
# Stablecoin tickers span multiple underlying chains in the OFAC feed; infer the
# chain from the address shape rather than hard-assigning.
_STABLECOIN_TICKERS = {"USDC", "USDT"}


def _infer_chain_from_address(addr: str, default: str) -> str:
    """Override the ticker default ONLY on an unambiguous address shape.

    EVM (0x + 40 hex) and TRON (T + 33 base58, 34 chars total) are unmistakable;
    everything else keeps the ticker's declared chain (BTC/LTC/DASH/etc. share
    overlapping base58 prefixes, so we do not guess between them)."""
    a = (addr or "").strip()
    if a.startswith("0x") and len(a) == 42:
        return "ethereum"
    if a.startswith("T") and len(a) == 34:
        return "tron"
    if default == "bitcoin" and a.lower().startswith("bc1"):
        return "bitcoin"
    # Stablecoins are issued cross-chain. An Omni-layer USDT address is a
    # Bitcoin P2PKH/P2SH base58 (1.../3...) — route it to bitcoin so it verifies.
    if default == "ethereum" and a[:1] in ("1", "3") and _RE_BTC_B58.match(a):
        return "bitcoin"
    return default


def fetch_ofac_sdn_mirror(timeout: float = 20.0,
                          cap: int = 50_000) -> List[Record]:
    """Pull the full per-chain OFAC SDN crypto-address mirror (public).

    Covers every chain OFAC has ever listed a Digital Currency Address on.
    Returns [] if unreachable. Authoritative SDN content, machine-readable.
    """
    out: List[Record] = []
    for tk, chain in _OFAC_MIRROR_CHAINS.items():
        url = _OFAC_MIRROR_BASE.format(tk=tk)
        raw = _http_get(url, timeout=timeout)
        if not raw:
            continue
        for line in raw.decode("utf-8", errors="replace").splitlines():
            addr = line.strip()
            if not addr or addr.startswith("#"):
                continue
            # Only EVM (0x) addresses are case-insensitive; base58 (BTC/TRON/etc.)
            # is case-SENSITIVE and must be preserved verbatim.
            norm = addr.lower() if addr.startswith("0x") else addr
            # OFAC sometimes cross-lists an address under a ticker whose native
            # address shape it does not match (esp. stablecoins, and the
            # occasional XBT/ETH mislabel). Infer the chain from the address
            # surface; fall back to the ticker's default only when ambiguous.
            row_chain = _infer_chain_from_address(addr, chain)
            out.append(Record(
                address=norm, chain=row_chain,
                entity_name=f"OFAC SDN sanctioned address ({tk})",
                entity_type="sanctioned", category="sanctioned-entity",
                label_source="ofac_sdn_mirror", source_url=url,
                notes=f"OFAC SDN Digital Currency Address ({tk}); public mirror.",
            ))
            if len(out) >= cap:
                return out
    return out


# Public labels that denote market-makers / funds (entity-level, not PII).
_FUND_LABEL_HINTS = ("market maker", "wintermute", "jump", "alameda",
                     "amber", "cumberland", "fund", "capital", "ventures")
_EXCHANGE_LABEL_HINTS = ("binance", "coinbase", "kraken", "okx", "okex",
                         "huobi", "bitfinex", "kucoin", "bybit", "gate.io",
                         "gemini", "bitstamp", "crypto.com", "exchange",
                         "deposit", "hot wallet", "cold wallet")
_BRIDGE_LABEL_HINTS = ("bridge", "wormhole", "across", "hop", "celer",
                       "synapse", "stargate", "portal")
_MIXER_LABEL_HINTS = ("tornado", "mixer", "tumbler", "blender")


def _classify_label(name: str, labels: List[str]) -> Tuple[str, str]:
    """Map a public Etherscan-style label to (entity_type, category).

    Defaults to ('contract', 'labeled-cluster') — a public on-chain contract
    label, never a private identity.
    """
    blob = (name + " " + " ".join(labels)).lower()
    if any(h in blob for h in _MIXER_LABEL_HINTS):
        return "mixer", "sanctioned-entity"
    if any(h in blob for h in _EXCHANGE_LABEL_HINTS):
        return "exchange", "cex"
    if any(h in blob for h in _BRIDGE_LABEL_HINTS):
        return "bridge", "bridge-contract"
    if any(h in blob for h in _FUND_LABEL_HINTS):
        return "fund", "labeled-cluster"
    return "contract", "labeled-cluster"


def _parse_scan_labels(data: Any, chain: str, source_id: str, url: str,
                       cap: int) -> List[Record]:
    """Parse a brianleect ``combinedAllLabels.json`` blob into Records.

    The file is a dict ``{address: {name, labels: [...]}}``. Each address carries
    a PUBLIC explorer entity/protocol/contract label — address->entity mapping
    only, no private-individual PII.
    """
    if not isinstance(data, dict):
        return []
    out: List[Record] = []
    for addr, info in data.items():
        if not isinstance(info, dict):
            continue
        a = (addr or "").strip().lower()
        if not (a.startswith("0x") and len(a) == 42):
            continue
        name = (info.get("name") or "").strip()
        labels = info.get("labels") or []
        if not name:
            name = (labels[0] if labels else "").strip()
        if not name:
            continue
        etype, cat = _classify_label(name, labels if isinstance(labels, list) else [])
        out.append(Record(
            address=a, chain=chain, entity_name=name,
            entity_type=etype, category=cat,
            label_source=source_id, source_url=url,
            notes="Public explorer label" + (
                f" [{', '.join(labels[:4])}]" if labels else ""),
        ))
        if len(out) >= cap:
            break
    return out


def fetch_etherscan_labels(timeout: float = 30.0,
                           cap: int = 60_000) -> List[Record]:
    """Pull the public Etherscan combined-labels set (~30k labeled addresses).

    Each address carries a PUBLIC entity/protocol/contract label. Address->entity
    mapping only — no private-individual PII. Returns [] if unreachable.
    """
    url = ("https://raw.githubusercontent.com/brianleect/etherscan-labels/"
           "main/data/etherscan/combined/combinedAllLabels.json")
    data = _http_get_json(url, timeout=timeout)
    return _parse_scan_labels(data, "ethereum", "etherscan_labels", url, cap)


# brianleect ships the same combinedAllLabels.json layout for several other EVM
# explorers. Map each public per-chain file to its cryptoatlas chain.
_MULTICHAIN_LABEL_DIRS = {
    "bscscan": "bsc", "polygonscan": "polygon", "arbiscan": "arbitrum",
    "optimism": "optimism", "ftmscan": "fantom", "avalanche": "avalanche",
}


def fetch_multichain_explorer_labels(timeout: float = 30.0,
                                     cap: int = 80_000) -> List[Record]:
    """Pull public per-chain explorer label sets (BSC/Polygon/Arbitrum/Optimism/
    Fantom/Avalanche) from the same public ``etherscan-labels`` repo.

    Each address carries a PUBLIC explorer entity/protocol/contract label.
    Returns [] if all sources unreachable; skips any individual dead file.
    """
    base = ("https://raw.githubusercontent.com/brianleect/etherscan-labels/"
            "main/data/{dir}/combined/combinedAllLabels.json")
    out: List[Record] = []
    for dir_name, chain in _MULTICHAIN_LABEL_DIRS.items():
        url = base.format(dir=dir_name)
        data = _http_get_json(url, timeout=timeout)
        recs = _parse_scan_labels(data, chain, "multichain_explorer_labels",
                                  url, cap - len(out))
        out.extend(recs)
        if len(out) >= cap:
            break
    return out


# chainId -> cryptoatlas chain name for token-list ingestion.
_EVM_CHAIN_BY_ID = {
    1: "ethereum", 10: "optimism", 56: "bsc", 137: "polygon",
    250: "fantom", 8453: "base", 42161: "arbitrum", 43114: "avalanche",
    100: "gnosis", 324: "zksync", 25: "cronos", 81457: "blast",
    # additional public EVM platforms
    42220: "celo", 59144: "linea", 5000: "mantle", 534352: "scroll",
    1284: "moonbeam", 1285: "moonriver", 1313161554: "aurora",
    1088: "metis", 2222: "kava", 1666600000: "harmony", 128: "heco",
    66: "okc", 321: "kcc", 288: "boba", 122: "fuse", 1101: "polygon-zkevm",
    1116: "core", 146: "sonic", 2020: "ronin", 369: "pulsechain",
    592: "astar", 2000: "dogechain", 169: "manta", 34443: "mode",
    252: "fraxtal", 30: "rootstock",
}


def _ingest_token_list(tokens: List[Dict[str, Any]], source_id: str, url: str,
                       default_chain: str, cap: int) -> List[Record]:
    out: List[Record] = []
    for t in tokens:
        if not isinstance(t, dict):
            continue
        addr = (t.get("address") or "").strip().lower()
        if not (addr.startswith("0x") and len(addr) == 42):
            continue
        name = (t.get("name") or t.get("symbol") or "").strip()
        if not name:
            continue
        chain = _EVM_CHAIN_BY_ID.get(t.get("chainId"), default_chain)
        if chain not in CHAINS:
            chain = default_chain
        out.append(Record(
            address=addr, chain=chain, entity_name=name,
            entity_type="token", category="token-contract",
            label_source=source_id, source_url=url,
            balance_hint=(t.get("symbol") or ""),
            notes="Token-contract issuer label from a public token list.",
        ))
        if len(out) >= cap:
            break
    return out


def fetch_uniswap_token_list(timeout: float = 25.0, cap: int = 20_000) -> List[Record]:
    """Pull the canonical Uniswap default token list (issuer-labeled contracts)."""
    url = "https://tokens.uniswap.org/"
    data = _http_get_json(url, timeout=timeout)
    toks = data.get("tokens") if isinstance(data, dict) else None
    if not isinstance(toks, list):
        return []
    return _ingest_token_list(toks, "uniswap_token_list", url, "ethereum", cap)


def fetch_oneinch_token_lists(timeout: float = 25.0,
                              cap: int = 30_000) -> List[Record]:
    """Pull 1inch per-chain token-contract lists across EVM chains."""
    out: List[Record] = []
    for cid, chain in _EVM_CHAIN_BY_ID.items():
        url = f"https://tokens.1inch.io/v1.2/{cid}"
        data = _http_get_json(url, timeout=timeout)
        if not isinstance(data, dict):
            continue
        # 1inch returns {address: {name, symbol, ...}}
        toks = []
        for addr, meta in data.items():
            if isinstance(meta, dict):
                m = dict(meta)
                m.setdefault("address", addr)
                m.setdefault("chainId", cid)
                toks.append(m)
        out.extend(_ingest_token_list(toks, "oneinch_token_lists", url, chain,
                                      cap - len(out)))
        if len(out) >= cap:
            break
    return out


def fetch_trustwallet_tokens(timeout: float = 25.0,
                             cap: int = 20_000) -> List[Record]:
    """Pull Trust Wallet multi-chain token lists (issuer-labeled contracts)."""
    out: List[Record] = []
    chains = {"ethereum": "ethereum", "smartchain": "bsc", "polygon": "polygon",
              "arbitrum": "arbitrum", "optimism": "optimism",
              "avalanchec": "avalanche", "base": "base"}
    for slug, chain in chains.items():
        url = (f"https://raw.githubusercontent.com/trustwallet/assets/master/"
               f"blockchains/{slug}/tokenlist.json")
        data = _http_get_json(url, timeout=timeout)
        toks = data.get("tokens") if isinstance(data, dict) else None
        if not isinstance(toks, list):
            continue
        out.extend(_ingest_token_list(toks, "trustwallet_assets", url, chain,
                                      cap - len(out)))
        if len(out) >= cap:
            break
    return out


# CoinGecko publishes a token list per chain platform (Uniswap-token-list JSON
# schema). slug -> cryptoatlas chain.
_COINGECKO_PLATFORMS = {
    "uniswap": "ethereum", "polygon-pos": "polygon",
    "binance-smart-chain": "bsc", "arbitrum-one": "arbitrum",
    "optimistic-ethereum": "optimism", "avalanche": "avalanche",
    "base": "base", "fantom": "fantom", "xdai": "gnosis",
    # additional public CoinGecko asset platforms that serve /all.json
    "celo": "celo", "linea": "linea", "mantle": "mantle", "scroll": "scroll",
    "moonbeam": "moonbeam", "moonriver": "moonriver", "aurora": "aurora",
    "metis-andromeda": "metis", "kava": "kava", "harmony-shard-0": "harmony",
    "huobi-token": "heco", "okex-chain": "okc",
    "kucoin-community-chain": "kcc", "boba": "boba", "fuse": "fuse",
    "core": "core", "sonic": "sonic", "ronin": "ronin",
    "pulsechain": "pulsechain", "astar": "astar", "dogechain": "dogechain",
    "cronos": "cronos",
}


def fetch_coingecko_token_lists(timeout: float = 30.0,
                                cap: int = 60_000) -> List[Record]:
    """Pull CoinGecko per-platform token lists (issuer-labeled contracts).

    Each platform list follows the Uniswap token-list schema
    (``{"tokens": [{chainId, address, name, symbol}, ...]}``). Token-contract
    -> issuer/project mapping only; entity/contract-level, no private PII.
    """
    out: List[Record] = []
    for slug, chain in _COINGECKO_PLATFORMS.items():
        url = f"https://tokens.coingecko.com/{slug}/all.json"
        data = _http_get_json(url, timeout=timeout)
        toks = data.get("tokens") if isinstance(data, dict) else None
        if not isinstance(toks, list):
            continue
        out.extend(_ingest_token_list(toks, "coingecko_token_lists", url, chain,
                                      cap - len(out)))
        if len(out) >= cap:
            break
    return out


# Additional public token lists that follow the Uniswap token-list schema
# (``{"tokens": [{chainId, address, name, symbol}, ...]}``). Each is a public,
# project-published registry of token-contract -> issuer/project labels. The
# chainId in every entry routes the row to the right chain via _EVM_CHAIN_BY_ID;
# default_chain is only a fall-back for an unmapped chainId.
_EXTRA_TOKEN_LISTS = (
    ("https://api.coinmarketcap.com/data-api/v3/uniswap/all.json",
     "ethereum", "CoinMarketCap public token list"),
    ("https://tokens.pancakeswap.finance/pancakeswap-extended.json",
     "bsc", "PancakeSwap extended token list"),
    ("https://tokens.pancakeswap.finance/pancakeswap-top-100.json",
     "bsc", "PancakeSwap top-100 token list"),
    ("https://static.optimism.io/optimism.tokenlist.json",
     "optimism", "Optimism bridged token list"),
    ("https://bridge.arbitrum.io/token-list-42161.json",
     "arbitrum", "Arbitrum bridged token list"),
    ("https://t2crtokens.eth.link/",
     "ethereum", "Kleros Tokens (T2CR) curated list"),
    ("https://tokenlist.aave.eth.link/",
     "ethereum", "Aave token list"),
    ("https://raw.githubusercontent.com/compound-finance/token-list/master/"
     "compound.tokenlist.json", "ethereum", "Compound token list"),
    ("https://www.gemini.com/uniswap/manifest.json",
     "ethereum", "Gemini token list"),
    ("https://raw.githubusercontent.com/SetProtocol/uniswap-tokenlist/main/"
     "set.tokenlist.json", "ethereum", "Set Protocol token list"),
    # --- additional public DEX / chain token lists (verified fetchable) ---
    ("https://unpkg.com/quickswap-default-token-list@latest/build/"
     "quickswap-default.tokenlist.json", "polygon", "QuickSwap default token list"),
    ("https://raw.githubusercontent.com/SpookySwap/spooky-info/master/src/"
     "constants/token/spookyswap.json", "fantom", "SpookySwap token list"),
    ("https://raw.githubusercontent.com/traderjoe-xyz/joe-tokenlists/main/"
     "mc.tokenlist.json", "avalanche", "Trader Joe multi-chain token list"),
    ("https://celo-org.github.io/celo-token-list/celo.tokenlist.json",
     "celo", "Celo token list"),
    ("https://raw.githubusercontent.com/balancer/tokenlists/main/generated/"
     "balancer.tokenlist.json", "ethereum", "Balancer token list"),
)


def fetch_extra_token_lists(timeout: float = 25.0,
                            cap: int = 60_000) -> List[Record]:
    """Pull additional public Uniswap-schema token lists (CMC / PancakeSwap /
    Optimism / Arbitrum-bridged / Kleros / Aave / Compound / Gemini / Set).

    Token-contract -> issuer/project labels only; entity/contract-level, no
    private PII. Each individual list is fail-soft; skips any that 404s/SSL-fails.
    """
    out: List[Record] = []
    for url, default_chain, _label in _EXTRA_TOKEN_LISTS:
        data = _http_get_json(url, timeout=timeout)
        toks = data.get("tokens") if isinstance(data, dict) else None
        if not isinstance(toks, list):
            continue
        out.extend(_ingest_token_list(toks, "extra_token_lists", url,
                                      default_chain, cap - len(out)))
        if len(out) >= cap:
            break
    return out


# --- Solana SPL token registries (base58 mint addresses, not EVM) -----------
# Solana mints are base58 (32-44 chars), so they need a dedicated ingester:
# _ingest_token_list only accepts EVM ``0x`` addresses. Both registries follow
# a tokens-array / flat-array shape with {chainId:101, address, name, symbol}.
# Token (mint) contract -> issuer/project labels only; entity/contract-level.
_SOLANA_TOKEN_LISTS = (
    ("https://raw.githubusercontent.com/solana-labs/token-list/main/src/"
     "tokens/solana.tokenlist.json", "Solana Labs token registry"),
    ("https://cache.jup.ag/tokens", "Jupiter aggregator token list"),
)


def _ingest_solana_token_list(tokens: List[Dict[str, Any]], source_id: str,
                              url: str, cap: int) -> List[Record]:
    """Parse a Solana SPL token list into Records.

    Keeps only mainnet (chainId 101 when present) base58 mint addresses that pass
    the Solana on-chain shape (32-44 base58 chars). Mint-contract -> issuer label
    only; public, entity/contract-level, no private PII.
    """
    out: List[Record] = []
    for t in tokens:
        if not isinstance(t, dict):
            continue
        cid = t.get("chainId")
        if cid is not None and cid != 101:  # only Solana mainnet-beta
            continue
        addr = (t.get("address") or "").strip()
        if not (_RE_B58.match(addr) and 32 <= len(addr) <= 44):
            continue
        name = (t.get("name") or t.get("symbol") or "").strip()
        if not name:
            continue
        out.append(Record(
            address=addr, chain="solana", entity_name=name,
            entity_type="token", category="token-contract",
            label_source=source_id, source_url=url,
            balance_hint=(t.get("symbol") or ""),
            notes="Solana SPL mint-contract issuer label from a public token list.",
        ))
        if len(out) >= cap:
            break
    return out


def fetch_solana_token_lists(timeout: float = 45.0,
                             cap: int = 60_000) -> List[Record]:
    """Pull public Solana SPL token registries (Solana Labs + Jupiter).

    Solana mints are base58 token contracts; each carries a public issuer/project
    name. Entity/contract-level only, no private PII. Fail-soft per list.
    """
    out: List[Record] = []
    for url, _label in _SOLANA_TOKEN_LISTS:
        data = _http_get_json(url, timeout=timeout)
        # Solana Labs: {"tokens": [...]}; Jupiter cache: a flat [...] list.
        if isinstance(data, dict):
            toks = data.get("tokens")
        elif isinstance(data, list):
            toks = data
        else:
            toks = None
        if not isinstance(toks, list):
            continue
        out.extend(_ingest_solana_token_list(toks, "solana_token_lists", url,
                                             cap - len(out)))
        if len(out) >= cap:
            break
    return out


# --- Public NFT collection CONTRACT registries (0xsequence token-directory) --
# 0xsequence publishes per-chain Uniswap-schema files of ERC-721 / ERC-1155 NFT
# collection CONTRACTS (each {chainId, address, name, symbol, standard}). These
# are CONTRACT addresses (the collection), never a private holder — entity/
# contract-level only. chainId routes each row via _EVM_CHAIN_BY_ID.
_SEQ_NFT_BASE = ("https://raw.githubusercontent.com/0xsequence/token-directory/"
                 "master/index/{slug}/{kind}.json")
# Sequence dir slugs whose chainId we can attribute to a cryptoatlas chain.
_SEQ_NFT_SLUGS = (
    "mainnet", "arbitrum", "avalanche", "base", "bnb", "gnosis",
    "optimism", "polygon", "polygon-zkevm", "sonic",
)
_SEQ_NFT_KINDS = ("erc721", "erc1155")


def fetch_nft_contract_lists(timeout: float = 30.0,
                             cap: int = 40_000) -> List[Record]:
    """Pull public NFT collection CONTRACT registries (0xsequence token-directory).

    Enumerates per-chain ``erc721.json`` / ``erc1155.json`` files; each row is an
    NFT collection's deployed contract address + public collection name. CONTRACT
    addresses only (not holders) — entity/contract-level, no private PII. chainId
    in each entry routes via _EVM_CHAIN_BY_ID. Fail-soft per file.
    """
    out: List[Record] = []
    for slug in _SEQ_NFT_SLUGS:
        for kind in _SEQ_NFT_KINDS:
            url = _SEQ_NFT_BASE.format(slug=slug, kind=kind)
            data = _http_get_json(url, timeout=timeout)
            toks = data.get("tokens") if isinstance(data, dict) else None
            if not isinstance(toks, list):
                continue
            for t in toks:
                if not isinstance(t, dict):
                    continue
                addr = (t.get("address") or "").strip().lower()
                if not (addr.startswith("0x") and len(addr) == 42):
                    continue
                name = (t.get("name") or t.get("symbol") or "").strip()
                if not name:
                    continue
                chain = _EVM_CHAIN_BY_ID.get(t.get("chainId"))
                if not chain or chain not in CHAINS:
                    continue
                out.append(Record(
                    address=addr, chain=chain, entity_name=name,
                    entity_type="contract", category="token-contract",
                    label_source="nft_contract_lists", source_url=url,
                    balance_hint=(t.get("symbol") or kind.upper()),
                    notes=f"Public NFT collection {kind.upper()} contract label "
                          "(0xsequence token-directory); entity/contract-level.",
                ))
                if len(out) >= cap:
                    return out
    return out


# --- Stargate cross-chain bridged-token registry ----------------------------
# Stargate publishes its bridgeable token set as a flat list whose rows carry a
# ``chainKey`` (chain slug, NOT a numeric chainId) + an on-chain ``address``.
# Map the chainKey to a cryptoatlas chain; keep only EVM/Solana/Tron addresses
# whose shape verifies. Bridged token-contract labels only; entity-level, no PII.
_STARGATE_TOKENS_URL = "https://stargate.finance/api/tokens"
_STARGATE_CHAINKEY = {
    "ethereum": "ethereum", "bsc": "bsc", "polygon": "polygon",
    "arbitrum": "arbitrum", "optimism": "optimism", "base": "base",
    "avalanche": "avalanche", "fantom": "fantom", "linea": "linea",
    "mantle": "mantle", "scroll": "scroll", "metis": "metis", "kava": "kava",
    "aurora": "aurora", "moonbeam": "moonbeam", "moonriver": "moonriver",
    "sonic": "sonic", "blast": "blast", "mode": "mode", "fraxtal": "fraxtal",
    "gnosis": "gnosis", "zksync": "zksync", "manta": "manta", "fuse": "fuse",
    "astar": "astar", "rootstock": "rootstock", "celo": "celo",
    "solana": "solana", "tron": "tron",
}


def fetch_stargate_tokens(timeout: float = 30.0,
                          cap: int = 10_000) -> List[Record]:
    """Pull Stargate's public bridgeable-token registry (cross-chain contracts).

    Each row maps a ``chainKey`` (chain slug) + on-chain ``address`` to a public
    token name/symbol. Bridged token-contract labels only; entity/contract-level,
    no private PII. Only addresses that verify for their resolved chain are kept.
    Fail-soft -> [] if unreachable.
    """
    data = _http_get_json(_STARGATE_TOKENS_URL, timeout=timeout)
    toks = data if isinstance(data, list) else (
        data.get("tokens") if isinstance(data, dict) else None)
    if not isinstance(toks, list):
        return []
    out: List[Record] = []
    for t in toks:
        if not isinstance(t, dict):
            continue
        chain = _STARGATE_CHAINKEY.get((t.get("chainKey") or "").strip().lower())
        if not chain or chain not in CHAINS:
            continue
        addr = (t.get("address") or "").strip()
        norm = addr.lower() if _RE_EVM.match(addr) else addr
        if not _address_ok(norm, chain):
            continue
        name = (t.get("name") or t.get("symbol") or "").strip()
        if not name:
            continue
        out.append(Record(
            address=norm, chain=chain, entity_name=name,
            entity_type="token", category="token-contract",
            label_source="stargate_tokens", source_url=_STARGATE_TOKENS_URL,
            balance_hint=(t.get("symbol") or ""),
            notes="Stargate bridgeable token-contract label (public); "
                  "entity/contract-level.",
        ))
        if len(out) >= cap:
            break
    return out


# Trust Wallet ``assets`` blockchain slug -> cryptoatlas chain. We ingest only
# chains whose on-chain address shape we can verify (EVM / Solana / Tron); other
# Trust Wallet chains (Cosmos/TON/Aptos/etc.) are skipped, never guessed.
_TW_ASSET_CHAINS = {
    "ethereum": "ethereum", "smartchain": "bsc", "polygon": "polygon",
    "arbitrum": "arbitrum", "optimism": "optimism", "avalanchec": "avalanche",
    "base": "base", "classic": "ethereum-classic", "fantom": "fantom",
    "xdai": "gnosis", "blast": "blast", "scroll": "scroll", "linea": "linea",
    "zksync": "zksync", "mantle": "mantle", "celo": "celo", "heco": "heco",
    "kcc": "kcc", "sonic": "sonic", "cronos": "cronos", "moonbeam": "moonbeam",
    "moonriver": "moonriver", "aurora": "aurora", "metis": "metis",
    "boba": "boba", "fuse": "fuse", "okc": "okc", "manta": "manta",
    "rootstock": "rootstock", "tomochain": "ethereum",
    "solana": "solana", "tron": "tron",
}
_TW_TREE_URL = ("https://api.github.com/repos/trustwallet/assets/git/trees/"
                "master?recursive=1")
_TW_RAW = ("https://raw.githubusercontent.com/trustwallet/assets/master/"
           "blockchains/{slug}/assets/{addr}/info.json")
_TW_PATH_RE = re.compile(
    r"^blockchains/([^/]+)/assets/([^/]+)/info\.json$")


def _tw_address_ok(addr: str, chain: str) -> bool:
    """Verify a Trust Wallet asset directory name is an on-chain address we
    can attribute to the given chain (EVM / Solana / Tron)."""
    if chain in _EVM_CHAINS:
        return bool(_RE_EVM.match(addr))
    if chain == "solana":
        return bool(_RE_B58.match(addr)) and 32 <= len(addr) <= 44
    if chain == "tron":
        return bool(_RE_TRON.match(addr))
    return False


def fetch_trustwallet_assets(timeout: float = 30.0,
                             cap: int = 30_000,
                             max_workers: int = 24) -> List[Record]:
    """Pull the FULL Trust Wallet ``assets`` token registry across all chains.

    Enumerates every ``blockchains/<chain>/assets/<address>/info.json`` via one
    git-tree call, then fetches each ``info.json`` in parallel for the public
    token name/symbol. Token-contract -> issuer/project labels only; the asset
    directory name is the contract address. Entity/contract-level, no PII.

    Fail-soft: returns [] if the tree is unreachable; skips any asset whose
    info.json 404s, whose chain we can't verify, or whose address is malformed.
    """
    tree = _http_get_json(_TW_TREE_URL, timeout=timeout)
    if not isinstance(tree, dict) or not isinstance(tree.get("tree"), list):
        return []
    targets: List[Tuple[str, str, str]] = []  # (chain, addr, url)
    for node in tree["tree"]:
        path = node.get("path") if isinstance(node, dict) else None
        if not path:
            continue
        m = _TW_PATH_RE.match(path)
        if not m:
            continue
        slug, addr = m.group(1), m.group(2)
        chain = _TW_ASSET_CHAINS.get(slug)
        if not chain:
            continue
        norm = addr.lower() if _RE_EVM.match(addr) else addr
        if not _tw_address_ok(norm, chain):
            continue
        targets.append((chain, norm, _TW_RAW.format(slug=slug, addr=addr)))
        if len(targets) >= cap:
            break

    def _fetch_one(t: Tuple[str, str, str]) -> Optional[Record]:
        chain, addr, url = t
        info = _http_get_json(url, timeout=timeout)
        name = symbol = ""
        if isinstance(info, dict):
            name = (info.get("name") or "").strip()
            symbol = (info.get("symbol") or "").strip()
        if not name:
            name = symbol or "Trust Wallet listed token"
        return Record(
            address=addr, chain=chain, entity_name=name,
            entity_type="token", category="token-contract",
            label_source="trustwallet_assets_full", source_url=url,
            balance_hint=symbol,
            notes="Token-contract issuer label from the Trust Wallet assets "
                  "registry (public, entity/contract-level).",
        )

    out: List[Record] = []
    if not targets:
        return out
    with _cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        for rec in ex.map(_fetch_one, targets):
            if rec is not None:
                out.append(rec)
    return out


# OpenSanctions consolidated sanctions dataset (aggregates OFAC + EU + UN + UK +
# more public lists). Its FollowTheMoney ``CryptoWallet`` entities expose a
# ``publicKey`` (on-chain address) + ``currency`` (chain ticker). Entity-level
# sanctioned-address labels only; ``holder`` is an internal code, not PII.
_OPENSANCTIONS_FTM = ("https://data.opensanctions.org/datasets/latest/"
                      "sanctions/entities.ftm.json")
_OS_CURRENCY_CHAIN = {
    "XBT": "bitcoin", "BTC": "bitcoin", "BSV": "bitcoin", "BTG": "bitcoin",
    "ETH": "ethereum", "USDT": "ethereum", "USDC": "ethereum",
    "TRX": "tron", "LTC": "litecoin", "XMR": "monero", "DOGE": "dogecoin",
    "BCH": "bitcoin-cash", "ZEC": "zcash", "DASH": "dash", "XRP": "ripple",
    "XVG": "verge", "ETC": "ethereum-classic", "ARB": "arbitrum",
    "BSC": "bsc", "BNB": "bsc", "SOL": "solana",
}


def _os_chain_for(addr: str, currency: str) -> str:
    """Resolve an OpenSanctions CryptoWallet to a verifiable cryptoatlas chain.

    Prefer the unambiguous address shape (EVM/Tron/bech32-BTC); else fall back
    to the declared currency ticker. The returned (chain, addr) pair MUST pass
    ``_address_ok`` — OpenSanctions sometimes carries homoglyph/Unicode-confusable
    obfuscation variants of an address; those don't match a clean on-chain shape
    and are skipped (return '') rather than mis-attributed. Returns '' if not
    verifiably mappable."""
    a = (addr or "").strip()
    # Reject any non-ASCII publicKey outright (homoglyph obfuscation variants).
    if not a.isascii():
        return ""
    if _RE_EVM.match(a):
        return "ethereum"
    if _RE_TRON.match(a):
        return "tron"
    if a.lower().startswith("bc1"):
        return "bitcoin"
    chain = _OS_CURRENCY_CHAIN.get((currency or "").strip().upper(), "")
    # Only keep the currency fallback when the address actually verifies for
    # that chain (covers BTC/LTC/etc. base58 whose shape we can check).
    if chain and _address_ok(a, chain):
        return chain
    return ""


def fetch_opensanctions_crypto(timeout: float = 60.0,
                               cap: int = 20_000) -> List[Record]:
    """Pull sanctioned crypto addresses from the OpenSanctions consolidated
    sanctions dataset (public; aggregates OFAC/EU/UN/UK + more).

    Streams the FollowTheMoney NDJSON and keeps only ``CryptoWallet`` entities,
    mapping each ``publicKey`` to a verifiable chain. Entity-level sanctioned
    labels only; no private-individual PII. Fail-soft -> [] if unreachable.
    """
    req = urllib.request.Request(
        _OPENSANCTIONS_FTM,
        headers={"User-Agent": f"{TOOL_NAME}/{TOOL_VERSION} (+public-data)"})
    out: List[Record] = []
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            for raw_line in resp:
                if len(out) >= cap:
                    break
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    ent = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    continue
                if not isinstance(ent, dict) or ent.get("schema") != "CryptoWallet":
                    continue
                props = ent.get("properties") or {}
                keys = props.get("publicKey") or []
                if not isinstance(keys, list):
                    continue
                currency = ""
                cur_list = props.get("currency") or []
                if isinstance(cur_list, list) and cur_list:
                    currency = str(cur_list[0])
                for pk in keys:
                    addr = str(pk).strip()
                    if not addr:
                        continue
                    chain = _os_chain_for(addr, currency)
                    if not chain or chain not in CHAINS:
                        continue
                    norm = addr.lower() if _RE_EVM.match(addr) else addr
                    out.append(Record(
                        address=norm, chain=chain,
                        entity_name="Sanctioned crypto address (consolidated)",
                        entity_type="sanctioned", category="sanctioned-entity",
                        label_source="opensanctions_crypto",
                        source_url=_OPENSANCTIONS_FTM,
                        balance_hint=(currency or ""),
                        notes="Sanctioned digital-currency address from the "
                              "OpenSanctions consolidated list (OFAC/EU/UN/UK+); "
                              "entity-level, no private PII.",
                    ))
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError):
        return out
    return out


# DefiLlama protocol ``address`` values are either a bare EVM/Solana address or
# a ``<chain>:<address>`` pair. Map DefiLlama chain slugs to cryptoatlas chains.
_DEFILLAMA_CHAINS = {
    "ethereum": "ethereum", "bsc": "bsc", "binance": "bsc",
    "polygon": "polygon", "arbitrum": "arbitrum", "optimism": "optimism",
    "base": "base", "avax": "avalanche", "avalanche": "avalanche",
    "fantom": "fantom", "xdai": "gnosis", "gnosis": "gnosis",
    "era": "zksync", "zksync": "zksync", "cronos": "cronos",
    "blast": "blast", "solana": "solana", "tron": "tron",
}
# DefiLlama category -> (entity_type, category). Defaults to protocol/defi.
_DEFILLAMA_CAT = {
    "cex": ("exchange", "cex"),
    "bridge": ("bridge", "bridge-contract"),
}


def _defillama_chain_addr(raw_addr: str) -> Tuple[str, str]:
    """Split a DefiLlama address into (chain, normalized_address).

    Returns ('', '') if the chain is unknown or the address is not an
    unambiguous on-chain identifier we can verify.
    """
    s = (raw_addr or "").strip()
    if ":" in s:
        slug, _, addr = s.partition(":")
        chain = _DEFILLAMA_CHAINS.get(slug.strip().lower(), "")
    else:
        addr, chain = s, "ethereum"  # bare DefiLlama addresses are EVM
    addr = addr.strip()
    if not chain:
        return "", ""
    al = addr.lower()
    if al.startswith("0x") and len(al) == 42:
        return chain if chain in CHAINS else "ethereum", al
    if chain == "solana" and _RE_B58.match(addr) and 32 <= len(addr) <= 44:
        return "solana", addr
    if chain == "tron" and _RE_TRON.match(addr):
        return "tron", addr
    return "", ""


def fetch_defillama_protocols(timeout: float = 30.0,
                              cap: int = 30_000) -> List[Record]:
    """Pull DefiLlama's public protocol registry (entity-level contract labels).

    Each protocol carries a public on-chain ``address`` (token/governance/bridge
    contract) plus a name + category. Entity/contract-level only; no private PII.
    Protocols without a parseable on-chain address are skipped.
    """
    data = _http_get_json("https://api.llama.fi/protocols", timeout=timeout)
    if not isinstance(data, list):
        return []
    out: List[Record] = []
    for p in data:
        if not isinstance(p, dict):
            continue
        chain, addr = _defillama_chain_addr(p.get("address") or "")
        if not addr:
            continue
        name = (p.get("name") or "").strip()
        if not name:
            continue
        cat_raw = (p.get("category") or "").strip().lower()
        etype, cat = _DEFILLAMA_CAT.get(cat_raw, ("protocol", "defi-protocol"))
        out.append(Record(
            address=addr, chain=chain, entity_name=name,
            entity_type=etype, category=cat,
            label_source="defillama_protocols",
            source_url="https://api.llama.fi/protocols",
            balance_hint=(p.get("symbol") or ""),
            notes=f"DefiLlama protocol ({p.get('category') or 'DeFi'}); "
                  "public on-chain contract label.",
        ))
        if len(out) >= cap:
            break
    return out


# Registry of live scale-fetchers. Each returns a list of Records (never raises).
LIVE_FETCHERS = (
    ("ofac_sdn_crypto", fetch_ofac_sdn),
    ("ofac_sdn_mirror", fetch_ofac_sdn_mirror),
    ("etherscan_labels", fetch_etherscan_labels),
    ("multichain_explorer_labels", fetch_multichain_explorer_labels),
    ("uniswap_token_list", fetch_uniswap_token_list),
    ("oneinch_token_lists", fetch_oneinch_token_lists),
    ("trustwallet_assets", fetch_trustwallet_tokens),
    ("trustwallet_assets_full", fetch_trustwallet_assets),
    ("coingecko_token_lists", fetch_coingecko_token_lists),
    ("extra_token_lists", fetch_extra_token_lists),
    ("opensanctions_crypto", fetch_opensanctions_crypto),
    ("defillama_protocols", fetch_defillama_protocols),
    ("solana_token_lists", fetch_solana_token_lists),
    ("nft_contract_lists", fetch_nft_contract_lists),
    ("stargate_tokens", fetch_stargate_tokens),
)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

@dataclass
class IngestStats:
    inserted: int = 0
    skipped_duplicate: int = 0
    rejected: int = 0
    by_source: Dict[str, int] = field(default_factory=dict)
    live_fetched: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _ingest_records(conn: sqlite3.Connection, recs: Iterable[Dict[str, Any]],
                    stats: IngestStats) -> None:
    for raw in recs:
        try:
            rec = Record(**raw) if isinstance(raw, dict) else raw
            inserted = upsert(conn, rec)
        except IngestError as exc:
            stats.rejected += 1
            stats.errors.append(f"{raw.get('entity_name', '?') if isinstance(raw, dict) else '?'}: {exc}")
            continue
        if inserted:
            stats.inserted += 1
            stats.by_source[rec.label_source] = stats.by_source.get(rec.label_source, 0) + 1
        else:
            stats.skipped_duplicate += 1


def build(db_path: str = DEFAULT_DB, live: bool = True,
          timeout: float = 8.0) -> IngestStats:
    """Run the ingestion/enrichment pipeline.

    1. Ingest the attributed PUBLIC seed (always).
    2. If ``live``, fetch every registered public scale-source in turn
       (OFAC SDN + per-chain mirror, Etherscan public labels, multi-chain
       token-contract lists). Each row carries its real ``source_url`` and is
       validated/deduped on insert. Any source that is unreachable is reported,
       never fabricated — the build reports the HONEST count.
    """
    conn = connect(db_path)
    stats = IngestStats()
    try:
        # 1. Seed (always available, fully attributed).
        _ingest_records(conn, SEED_RECORDS, stats)

        # 2. Live public sources (best-effort, per-source fail-soft).
        if live:
            for source_id, fetcher in LIVE_FETCHERS:
                try:
                    recs = fetcher(timeout=timeout)
                except Exception as exc:  # pragma: no cover - defensive
                    recs = []
                    stats.errors.append(f"{source_id}: fetch error ({exc!r}); skipped.")
                if recs:
                    stats.live_fetched += len(recs)
                    _ingest_records(conn, recs, stats)
                    conn.commit()
                else:
                    stats.errors.append(
                        f"{source_id}: live fetch unavailable in this environment "
                        "(offline/blocked/empty); no rows fabricated.")
        conn.commit()
    finally:
        conn.close()
    return stats


# ---------------------------------------------------------------------------
# Stats / query / export
# ---------------------------------------------------------------------------

def stats(db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    conn = connect(db_path)
    try:
        total = record_count(conn)
        by_type = {r["entity_type"]: r["n"] for r in conn.execute(
            "SELECT entity_type, COUNT(*) n FROM records GROUP BY entity_type ORDER BY n DESC")}
        by_source = {r["label_source"]: r["n"] for r in conn.execute(
            "SELECT label_source, COUNT(*) n FROM records GROUP BY label_source ORDER BY n DESC")}
        by_chain = {r["chain"]: r["n"] for r in conn.execute(
            "SELECT chain, COUNT(*) n FROM records GROUP BY chain ORDER BY n DESC")}
        by_category = {r["category"]: r["n"] for r in conn.execute(
            "SELECT category, COUNT(*) n FROM records GROUP BY category ORDER BY n DESC")}
        with_addr = conn.execute(
            "SELECT COUNT(*) FROM records WHERE address != ''").fetchone()[0]
        return {
            "record_count": total,
            "with_address": with_addr,
            "entity_level_only": total - with_addr,
            "by_entity_type": by_type,
            "by_label_source": by_source,
            "by_chain": by_chain,
            "by_category": by_category,
            "is_synthetic_rows": conn.execute(
                "SELECT COUNT(*) FROM records WHERE is_synthetic = 1").fetchone()[0],
        }
    finally:
        conn.close()


def query(term: str, db_path: str = DEFAULT_DB, limit: int = 100) -> List[Dict[str, Any]]:
    """Query by entity name (substring, case-insensitive) or exact address."""
    conn = connect(db_path)
    try:
        like = f"%{term.strip()}%"
        addr = term.strip().lower()
        rows = conn.execute(
            """SELECT * FROM records
               WHERE entity_name LIKE ? COLLATE NOCASE
                  OR lower(address) = ?
                  OR address LIKE ? COLLATE NOCASE
               ORDER BY entity_name LIMIT ?""",
            (like, addr, like, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def export(fmt: str, db_path: str = DEFAULT_DB, out: Optional[str] = None) -> str:
    """Export the dataset as json or csv. Returns the serialized text."""
    conn = connect(db_path)
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM records ORDER BY entity_type, entity_name")]
    finally:
        conn.close()

    if fmt == "json":
        text = json.dumps({
            "tool": TOOL_NAME, "version": TOOL_VERSION,
            "record_count": len(rows),
            "scope": "PUBLIC entity-level only; no private-individual PII",
            "records": rows,
        }, indent=2)
    elif fmt == "csv":
        buf = io.StringIO()
        fields = ["id", "address", "chain", "entity_name", "entity_type",
                  "category", "balance_hint", "label_source", "source_url",
                  "first_seen", "notes", "is_synthetic"]
        w = csv.DictWriter(buf, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})
        text = buf.getvalue()
    elif fmt == "graphml":
        text = records_to_graphml(rows)
    else:
        raise ValueError(f"unknown export format: {fmt!r}")

    if out:
        with open(out, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
    return text


# ---------------------------------------------------------------------------
# Integrity verification + graph export
# (verify_records / records_to_graphml drafted by the local fleet, corrected
#  and verified here: fixed the SOL base58 check and the GraphML structure.)
# ---------------------------------------------------------------------------

_RE_BTC_B58 = re.compile(r"^[13][1-9A-HJ-NP-Za-km-z]{25,39}$")
_RE_BTC_BECH32 = re.compile(r"^bc1[0-9ac-hj-np-z]{8,87}$")
_RE_EVM = re.compile(r"^0x[0-9a-fA-F]{40}$")
_RE_TRON = re.compile(r"^T[1-9A-HJ-NP-Za-km-z]{33}$")
_RE_B58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]+$")


# Chains whose addresses are EVM-shaped (0x + 40 hex).
_EVM_CHAINS = {"eth", "ethereum", "bsc", "polygon", "arbitrum", "optimism",
               "base", "avalanche", "fantom", "gnosis", "ethereum-classic",
               "zksync", "cronos", "blast",
               "celo", "linea", "mantle", "scroll", "moonbeam", "moonriver",
               "aurora", "metis", "kava", "harmony", "heco", "okc", "kcc",
               "boba", "fuse", "polygon-zkevm", "core", "sonic", "ronin",
               "pulsechain", "astar", "dogechain", "manta", "mode", "fraxtal",
               "rootstock"}


def _address_ok(address: str, chain: str) -> bool:
    """Heuristic per-chain address validation. Empty address is allowed
    (entity-level rows). Unknown chains are not failed."""
    a = (address or "").strip()
    c = (chain or "").strip().lower()
    if not a:
        return True
    if c in ("btc", "bitcoin"):
        return bool(_RE_BTC_B58.match(a) or _RE_BTC_BECH32.match(a))
    if c in _EVM_CHAINS:
        return bool(_RE_EVM.match(a))
    if c in ("tron",):
        return bool(_RE_TRON.match(a))
    if c in ("sol", "solana"):
        return bool(_RE_B58.match(a)) and 32 <= len(a) <= 44
    return True


def verify_records(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Check every record has an http(s) source_url and a well-formed address
    for its chain. Returns a structured pass/fail report."""
    rows = list(rows)
    failed: List[Dict[str, Any]] = []
    for r in rows:
        reasons: List[str] = []
        src = (r.get("source_url") or "").strip()
        if not (src.startswith("http://") or src.startswith("https://")):
            reasons.append("source_url missing or not http(s)")
        if not _address_ok(r.get("address", ""), r.get("chain", "")):
            reasons.append(f"malformed {r.get('chain') or '?'} address")
        if reasons:
            failed.append({"entity_name": r.get("entity_name"),
                           "address": r.get("address"), "reasons": reasons})
    total = len(rows)
    return {"ok": not failed, "total": total,
            "passed": total - len(failed), "failed": failed}


def verify(db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """Load the dataset and verify every record's source + address."""
    conn = connect(db_path)
    try:
        rows = [dict(r) for r in conn.execute("SELECT * FROM records")]
    finally:
        conn.close()
    return verify_records(rows)


def records_to_graphml(rows: Iterable[Dict[str, Any]]) -> str:
    """Render entities + their addresses as a valid GraphML graph: one node per
    entity, one per distinct address, a directed edge entity -> address."""
    from xml.sax.saxutils import escape, quoteattr
    ents: Dict[str, str] = {}
    addrs: Dict[str, str] = {}
    edges: List[Tuple[str, str]] = []
    for r in rows:
        name = (r.get("entity_name") or "").strip()
        addr = (r.get("address") or "").strip()
        if name:
            ents.setdefault(name, r.get("entity_type") or "")
        if addr:
            addrs.setdefault(addr, r.get("chain") or "")
            if name:
                edges.append((name, addr))
    out: List[str] = ['<?xml version="1.0" encoding="UTF-8"?>',
                      '<graphml xmlns="http://graphml.graphdrawing.org/xmlns">']
    for kid, attr in (("d0", "label"), ("d1", "kind"), ("d2", "type"), ("d3", "chain")):
        out.append(f'  <key id="{kid}" for="node" attr.name="{attr}" attr.type="string"/>')
    out.append('  <graph edgedefault="directed">')

    def _node(nid: str, label: str, kind: str, type_: str = "", chain: str = "") -> None:
        out.append(f"    <node id={quoteattr(nid)}>")
        out.append(f'      <data key="d0">{escape(label)}</data>')
        out.append(f'      <data key="d1">{escape(kind)}</data>')
        if type_:
            out.append(f'      <data key="d2">{escape(type_)}</data>')
        if chain:
            out.append(f'      <data key="d3">{escape(chain)}</data>')
        out.append("    </node>")

    for name, typ in sorted(ents.items()):
        _node("ent:" + name, name, "entity", type_=typ)
    for addr, chain in sorted(addrs.items()):
        _node("addr:" + addr, addr, "address", chain=chain)
    for i, (name, addr) in enumerate(edges):
        out.append(f'    <edge id="e{i}" source={quoteattr("ent:" + name)} '
                   f"target={quoteattr('addr:' + addr)}/>")
    out.append("  </graph>")
    out.append("</graphml>")
    return "\n".join(out) + "\n"


def source_catalog() -> List[Dict[str, str]]:
    return list(SOURCE_CATALOG)
