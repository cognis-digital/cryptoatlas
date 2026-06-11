#!/usr/bin/env bash
# cryptoatlas — basic demo: build (offline seed), inspect, query, export.
# Standard library only; no network required.
set -euo pipefail

PY="${PYTHON:-python}"
DB="$(mktemp -d)/cryptoatlas-demo.sqlite"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "== 1. Build the dataset from PUBLIC sources (offline seed) =="
"$PY" -m cryptoatlas --db "$DB" build --offline

echo
echo "== 2. Real record_count + breakdown =="
"$PY" -m cryptoatlas --db "$DB" stats

echo
echo "== 3. Query a public entity =="
"$PY" -m cryptoatlas --db "$DB" query Binance

echo
echo "== 4. Public source catalog (how to grow toward 180k) =="
"$PY" -m cryptoatlas --db "$DB" sources

echo
echo "== 5. Export to CSV =="
"$PY" -m cryptoatlas --db "$DB" export --format csv | head -5
echo "demo complete."
