"""Command-line interface for cryptoatlas."""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from cryptoatlas import TOOL_NAME, TOOL_VERSION
from cryptoatlas.core import (
    DEFAULT_DB,
    build,
    export,
    query,
    source_catalog,
    stats,
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=TOOL_NAME,
        description="Open, enriched dataset of PUBLIC crypto entities and their "
                    "addresses (exchanges, funds, ETFs, treasuries, governments, "
                    "seizures, reserves, labeled whales). Entity-level public data "
                    "only — no private-individual PII.",
    )
    p.add_argument("--version", action="version",
                   version=f"{TOOL_NAME} {TOOL_VERSION}")
    p.add_argument("--db", default=DEFAULT_DB, help="SQLite database path.")
    sub = p.add_subparsers(dest="command")

    b = sub.add_parser("build", help="Run the ingestion/enrichment pipeline.")
    b.add_argument("--offline", action="store_true",
                   help="Skip live fetch; ingest the attributed public seed only.")
    b.add_argument("--timeout", type=float, default=8.0,
                   help="Per-source live-fetch timeout (seconds).")
    b.add_argument("--json", action="store_true", help="Emit ingest stats as JSON.")

    st = sub.add_parser("stats", help="Show counts by entity type / source / chain.")
    st.add_argument("--json", action="store_true", help="Emit as JSON.")

    q = sub.add_parser("query", help="Query by entity name or address.")
    q.add_argument("term", help="Entity name substring or exact address.")
    q.add_argument("--limit", type=int, default=100)
    q.add_argument("--json", action="store_true", help="Emit as JSON.")

    ex = sub.add_parser("export", help="Export the dataset (json or csv).")
    ex.add_argument("--format", choices=("json", "csv"), default="json")
    ex.add_argument("--out", help="Write to this file instead of stdout.")

    sub.add_parser("sources", help="List the public source catalog.")

    sub.add_parser("mcp", help="Run as an MCP server (stdio JSON-RPC).")
    return p


def _run_build(args) -> int:
    s = build(db_path=args.db, live=not args.offline, timeout=args.timeout)
    if args.json:
        print(json.dumps(s.to_dict(), indent=2))
        return 0
    print(f"{TOOL_NAME} build complete")
    print("=" * 60)
    print(f"inserted            : {s.inserted}")
    print(f"skipped (duplicate) : {s.skipped_duplicate}")
    print(f"rejected (validate) : {s.rejected}")
    print(f"live fetched        : {s.live_fetched}")
    if s.by_source:
        print("by source:")
        for src, n in sorted(s.by_source.items(), key=lambda kv: -kv[1]):
            print(f"  {src:<28} {n}")
    if s.errors:
        print("notes:")
        for e in s.errors[:20]:
            print(f"  - {e}")
    final = stats(args.db)
    print("-" * 60)
    print(f"REAL record_count: {final['record_count']}")
    return 0


def _run_stats(args) -> int:
    s = stats(args.db)
    if args.json:
        print(json.dumps(s, indent=2))
        return 0
    print(f"{TOOL_NAME} stats — {s['record_count']} records "
          f"({s['with_address']} with address, "
          f"{s['entity_level_only']} entity-level)")
    print("=" * 60)
    print("by entity_type:")
    for k, v in s["by_entity_type"].items():
        print(f"  {k:<14} {v}")
    print("by label_source:")
    for k, v in s["by_label_source"].items():
        print(f"  {k:<28} {v}")
    print("by chain:")
    for k, v in s["by_chain"].items():
        print(f"  {k:<14} {v}")
    print(f"synthetic rows: {s['is_synthetic_rows']} (policy: must be 0)")
    return 0


def _run_query(args) -> int:
    rows = query(args.term, db_path=args.db, limit=args.limit)
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        print(f"no records match {args.term!r}")
        return 0
    print(f"{len(rows)} record(s) matching {args.term!r}")
    print("=" * 60)
    for r in rows:
        addr = r["address"] or "(entity-level, no single address)"
        print(f"[{r['entity_type']}] {r['entity_name']}")
        print(f"    {r['chain']}: {addr}")
        if r["balance_hint"]:
            print(f"    balance_hint: {r['balance_hint']}")
        print(f"    source: {r['source_url']}")
    return 0


def _run_export(args) -> int:
    text = export(args.format, db_path=args.db, out=args.out)
    if args.out:
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        print(text)
    return 0


def _run_sources() -> int:
    cat = source_catalog()
    print(f"{TOOL_NAME} public source catalog — {len(cat)} sources")
    print("=" * 60)
    for s in cat:
        print(f"[{s['type']}] {s['id']}")
        print(f"    {s['name']}")
        print(f"    {s['url']}")
        print(f"    {s['notes']}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "build":
        return _run_build(args)
    if args.command == "stats":
        return _run_stats(args)
    if args.command == "query":
        return _run_query(args)
    if args.command == "export":
        return _run_export(args)
    if args.command == "sources":
        return _run_sources()
    if args.command == "mcp":
        from cryptoatlas.mcp_server import run_mcp_server
        run_mcp_server()
        return 0
    parser.print_help(sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
