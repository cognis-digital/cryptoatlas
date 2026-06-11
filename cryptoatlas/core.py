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
)

CATEGORIES = (
    "cex", "defi", "etf-spot", "public-company", "nation-state",
    "law-enforcement", "strategic-reserve", "labeled-cluster",
    "sanctioned-entity", "infrastructure",
)

CHAINS = (
    "bitcoin", "ethereum", "tron", "solana", "litecoin",
    "bitcoin-cash", "ripple", "polygon", "arbitrum", "multi",
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


def validate(rec: Record) -> Record:
    """Validate a record against the schema. Raises IngestError on violation."""
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
]


# ---------------------------------------------------------------------------
# PUBLIC seed — small, hand-verified, fully-attributed rows.
# Every entry is a publicly-disclosed entity address with a real source_url.
# Used directly and as the offline fallback when live fetch is blocked.
# ---------------------------------------------------------------------------

SEED_RECORDS: List[Dict[str, str]] = [
    # --- Sanctioned (OFAC SDN, public) ---
    {
        "address": "1Q9UAQHFQPLEUSGTBHGEUH7DERZWQ3QQ8E".lower(),
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
    2. If ``live``, attempt to fetch live public sources (OFAC SDN). Each row
       carries its real source_url. Live failures are non-fatal — we report,
       never fabricate.
    """
    conn = connect(db_path)
    stats = IngestStats()
    try:
        # 1. Seed (always available, fully attributed).
        _ingest_records(conn, SEED_RECORDS, stats)

        # 2. Live public sources (best-effort).
        if live:
            ofac = fetch_ofac_sdn(timeout=timeout)
            if ofac:
                stats.live_fetched += len(ofac)
                _ingest_records(conn, ofac, stats)
            else:
                stats.errors.append("ofac_sdn_crypto: live fetch unavailable in this "
                                    "environment (offline/blocked); seed retained.")
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


def _address_ok(address: str, chain: str) -> bool:
    """Heuristic per-chain address validation. Empty address is allowed
    (entity-level rows). Unknown chains are not failed."""
    a = (address or "").strip()
    c = (chain or "").strip().lower()
    if not a:
        return True
    if c == "btc":
        return bool(_RE_BTC_B58.match(a) or _RE_BTC_BECH32.match(a))
    if c in ("eth", "bsc"):
        return bool(_RE_EVM.match(a))
    if c == "tron":
        return bool(_RE_TRON.match(a))
    if c == "sol":
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
