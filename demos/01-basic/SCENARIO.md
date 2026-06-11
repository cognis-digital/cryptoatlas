# Demo 01 — Build and query the public crypto-entity atlas

This demo builds the dataset from the bundled, fully-attributed PUBLIC seed
(no network needed), then shows the real record count, a query, the source
catalog, and a CSV export.

## Run

```bash
PYTHON=python ./run.sh
# or, on Windows:
#   set PYTHON=C:\Python314\python.exe & bash run.sh
```

## What you'll see

1. `build --offline` ingests the attributed public seed (OFAC SDN samples,
   exchange cold wallets, public-company treasuries, ETF custody, government
   seizures, strategic reserves, labeled whale clusters). The deliberately
   invalid PII-shaped placeholder row is **rejected** by the schema validator —
   proving the no-PII guardrail.
2. `stats` prints the **real** record_count and the breakdown by entity type,
   label source, and chain. `is_synthetic_rows` is always `0`.
3. `query Binance` returns labeled public exchange wallets, each with its real
   `source_url`.
4. `sources` lists the public source catalog you can grow the dataset with.
5. `export --format csv` emits the dataset.

Drop `--offline` to additionally pull the live OFAC SDN crypto feed when the
environment has network egress.
