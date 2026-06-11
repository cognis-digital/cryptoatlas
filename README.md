# cryptoatlas

**Open, enriched dataset of PUBLIC crypto entities and their addresses** —
exchanges, funds, ETFs, public-company treasuries, governments, law-enforcement
seizures, strategic reserves, and labeled whale clusters.

Part of the [Cognis Neural Suite](https://github.com/cognis-digital). Python
**standard library only** — no pip dependencies. Self-hostable: ships an
ingestion/enrichment pipeline, a SQLite schema, JSON/CSV export, a CLI, and an
MCP server.

---

## Scope and ethics (read this first)

cryptoatlas is **PUBLIC, entity-level data only**:

- ✅ Exchanges, funds, ETFs, public-company treasuries, governments, seizures,
  strategic reserves, and **publicly-labeled** whale/cluster addresses.
- ✅ Every row records the **real public `source_url`** it was derived from.
- ❌ **NO private-individual PII.** No names, emails, or identifying data for
  private persons.
- ❌ **NO deanonymization.** Unlabeled private addresses are intentionally
  skipped. The pipeline ingests only addresses that already carry a public
  entity label or disclosure.

The schema validator enforces this: any record that looks like PII (e.g. an
address containing an `@` or whitespace) is **rejected**, and `is_synthetic`
is always `false` (the pipeline never fabricates rows — see *Honesty* below).

---

## Install / run

```bash
# No dependencies. Use the suite Python (Windows): C:\Python314\python.exe
python -m cryptoatlas --version
python -m cryptoatlas --help

# Editable install gives you the `cryptoatlas` entry point:
pip install -e .
cryptoatlas --help
```

## Quick start

```bash
# Build the dataset. --offline uses the bundled attributed PUBLIC seed;
# drop it to also pull live public sources (OFAC SDN) when you have egress.
python -m cryptoatlas build --offline

# Real counts, by entity type / source / chain
python -m cryptoatlas stats

# Look up an entity or an address
python -m cryptoatlas query Binance
python -m cryptoatlas query 0x71660c4005ba85c37ccec55d0c4493e66fe775d3

# Export
python -m cryptoatlas export --format json --out atlas.json
python -m cryptoatlas export --format csv  --out atlas.csv

# List the public source catalog
python -m cryptoatlas sources
```

## Schema

Each record (SQLite table `records`, also the JSON/CSV export shape):

| field          | meaning                                                        |
|----------------|----------------------------------------------------------------|
| `address`      | On-chain address (may be empty for entity-level disclosures)   |
| `chain`        | `bitcoin`, `ethereum`, `tron`, `solana`, …                     |
| `entity_name`  | The public entity (e.g. "Coinbase", "MicroStrategy")           |
| `entity_type`  | `exchange`, `fund`, `etf`, `treasury`, `government`, `seizure`, `reserve`, `whale`, `sanctioned`, `mixer`, `service` |
| `category`     | `cex`, `defi`, `etf-spot`, `public-company`, `nation-state`, `law-enforcement`, `strategic-reserve`, `labeled-cluster`, `sanctioned-entity`, `infrastructure` |
| `balance_hint` | Coarse, disclosed balance note (never a fabricated number)     |
| `label_source` | Which catalog source produced the row                          |
| `source_url`   | **Real** public URL — required, must be `http(s)://`           |
| `first_seen`   | Optional ISO date                                              |
| `notes`        | Provenance / context                                           |
| `is_synthetic` | Always `false`                                                 |

Records are deduped on `chain + address` (or `entity_name` when no address).

## Honesty about record counts

This repo does **not** fabricate rows to hit a target. `build` ingests what the
public sources actually yield and `stats` reports the **real** `record_count`.

- The bundled attributed **seed** is small and hand-verified — every row links
  to a real public disclosure.
- With network egress, `build` (without `--offline`) additionally pulls the
  **live OFAC SDN** crypto feed and ingests every `Digital Currency Address`
  entry it parses. In a blocked/offline environment the live fetch is reported
  as unavailable and the seed is retained — no fabrication.

### Growing toward 180k records

The dataset scales by adding more **public label sets** to the source catalog
(`cryptoatlas sources`). Each is a genuinely public, attributable source:

| source id              | what it adds                                            |
|------------------------|---------------------------------------------------------|
| `ofac_sdn_crypto`      | OFAC SDN sanctioned crypto addresses (live-fetched)     |
| `ofac_sdn_advanced_json` | Structured OFAC advanced feed                         |
| `gov_btc_treasuries`   | Public-company / government BTC treasury disclosures    |
| `us_marshals_seizures` | DOJ/USMS seizure press releases with wallet addresses   |
| `spot_etf_custody`     | SEC EDGAR spot-ETF custody filings                      |
| `exchange_cold_wallets`| Community/PoR-attested exchange labels                  |
| `rich_list_snapshot`   | Public rich-list snapshots (labeled clusters **only**)  |
| `strategic_reserve`    | National strategic-reserve disclosures                  |

To grow the dataset: add a fetch/parse function in `cryptoatlas/core.py` (model
it on `fetch_ofac_sdn`), register its source in `SOURCE_CATALOG`, and call it
from `build`. Every new row must carry its real `source_url` and pass
`validate()`. The largest public label sets (Etherscan-style label dumps,
chain-analytics open label exports, full OFAC history) push the total into the
six-figure range; cryptoatlas is the schema + pipeline + provenance layer that
keeps that growth honest and attributable.

## MCP server

Run as a local MCP server (stdio JSON-RPC, stdlib only):

```json
{ "command": "python", "args": ["-m", "cryptoatlas", "mcp"] }
```

Tools: `query`, `stats`, `sources`.

## Demo

```bash
cd demos/01-basic && PYTHON=python ./run.sh
```

## Tests

```bash
python -m pytest -q          # or: python -m unittest discover -s tests
```

## Credits / Built on

- **OFAC SDN list** — U.S. Department of the Treasury (public domain).
- **SEC EDGAR** filings for treasury/ETF disclosures (public).
- **DOJ/USMS** press releases for seizure disclosures (public).
- Community proof-of-reserves and public address-label datasets.

All third-party data remains under its own terms; cryptoatlas stores only
public, entity-level labels with attribution.

## License

Cognis Open Collaboration License (COCL) v1.0 — see [LICENSE](LICENSE).
