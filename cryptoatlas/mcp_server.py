"""cryptoatlas MCP server.

Exposes the public-crypto-entity dataset over stdio using newline-delimited
JSON-RPC 2.0. Standard library only — runs anywhere Python does and can be
wired into Cognis.Studio, Claude Desktop, or Cursor:

    {"command": "python", "args": ["-m", "cryptoatlas", "mcp"]}

Tools:
  * query     — look up entities/addresses by name or address
  * stats     — dataset counts by type/source/chain
  * sources   — the public source catalog
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional

from cryptoatlas import TOOL_NAME, TOOL_VERSION
from cryptoatlas.core import query, source_catalog, stats

PROTOCOL_VERSION = "2024-11-05"

_TOOLS = [
    {
        "name": "query",
        "description": "Query the open PUBLIC crypto-entity dataset by entity name "
                       "(substring) or on-chain address. Returns labeled entities "
                       "(exchanges, funds, ETFs, treasuries, governments, seizures, "
                       "reserves, whales) with their real public source_url.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "term": {"type": "string",
                         "description": "Entity name substring or exact address."},
                "limit": {"type": "integer", "description": "Max rows (default 100)."},
            },
            "required": ["term"],
            "additionalProperties": False,
        },
    },
    {
        "name": "stats",
        "description": "Return dataset counts broken down by entity type, label "
                       "source, and chain, plus the real record_count.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "sources",
        "description": "List the public source catalog the pipeline ingests from.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
]


def _result(req_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _error(req_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _call_tool(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    if name == "query":
        term = arguments.get("term")
        if not isinstance(term, str) or not term:
            raise ValueError("`term` (string) is required")
        limit = arguments.get("limit", 100)
        payload: Any = query(term, limit=int(limit))
    elif name == "stats":
        payload = stats()
    elif name == "sources":
        payload = source_catalog()
    else:
        raise ValueError(f"unknown tool: {name}")
    return {"content": [{"type": "text", "text": json.dumps(payload, indent=2)}],
            "isError": False}


def handle_request(req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Dispatch a single JSON-RPC request. Returns None for notifications."""
    method = req.get("method")
    req_id = req.get("id")
    params = req.get("params") or {}
    is_notification = "id" not in req

    if method == "initialize":
        res = _result(req_id, {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": TOOL_NAME, "version": TOOL_VERSION},
        })
        return None if is_notification else res
    if method in ("notifications/initialized", "initialized"):
        return None
    if method == "ping":
        return None if is_notification else _result(req_id, {})
    if method == "tools/list":
        return _result(req_id, {"tools": _TOOLS})
    if method == "tools/call":
        name = params.get("name", "")
        arguments = params.get("arguments") or {}
        try:
            return _result(req_id, _call_tool(name, arguments))
        except (ValueError, OSError) as exc:
            return _error(req_id, -32602, str(exc))
        except Exception as exc:  # pragma: no cover - defensive
            return _error(req_id, -32603, f"internal error: {exc}")
    if is_notification:
        return None
    return _error(req_id, -32601, f"method not found: {method}")


def run_mcp_server(stdin=None, stdout=None) -> None:
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            stdout.write(json.dumps(_error(None, -32700, "parse error")) + "\n")
            stdout.flush()
            continue
        response = handle_request(req)
        if response is not None:
            stdout.write(json.dumps(response) + "\n")
            stdout.flush()


if __name__ == "__main__":
    run_mcp_server()
