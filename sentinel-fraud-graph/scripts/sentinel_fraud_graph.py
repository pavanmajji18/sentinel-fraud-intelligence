"""
Project SENTINEL — Neo4j Graph Fraud Ring & Shared-Device Detection Engine.
Scripts directory execution forwarding wrapper to root sentinel_fraud_graph.py.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parent.parent
TARGET_PATH = MODULE_ROOT / "sentinel_fraud_graph.py"
if str(MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(MODULE_ROOT))

_spec = importlib.util.spec_from_file_location("_root_sentinel_fraud_graph", TARGET_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Unable to load root module from {TARGET_PATH}")

_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

SentinelGraphStore = _mod.SentinelGraphStore
load_sentinel_tables = _mod.load_sentinel_tables
build_batch_payload = _mod.build_batch_payload
CONSTRAINT_STATEMENTS = _mod.CONSTRAINT_STATEMENTS
BATCH_MERGE_CYPHER = _mod.BATCH_MERGE_CYPHER
QUERY_1_CYPHER = _mod.QUERY_1_CYPHER
QUERY_2_CYPHER = _mod.QUERY_2_CYPHER
QUERY_3_CYPHER = _mod.QUERY_3_CYPHER
main = _mod.main

__all__ = [
    "SentinelGraphStore",
    "load_sentinel_tables",
    "build_batch_payload",
    "CONSTRAINT_STATEMENTS",
    "BATCH_MERGE_CYPHER",
    "QUERY_1_CYPHER",
    "QUERY_2_CYPHER",
    "QUERY_3_CYPHER",
    "main",
]

if __name__ == "__main__":
    raise SystemExit(main())
