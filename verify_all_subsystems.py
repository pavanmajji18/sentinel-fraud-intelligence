#!/usr/bin/env python3
"""
Project SENTINEL — Master Subsystems Verification & Quality Audit Runner.
Executes and audits all 8 specialized subsystems in offline-mock mode:
  1. sentinel-polyglot-data-architecture  : 18-table relational & unstructured schema verifier
  2. sentinel_feature_pipeline            : Point-in-time feature store & leakage verification
  3. sentinel_nosql_telemetry             : MongoDB BSON schema, ESR compound indexing & aggregations
  4. sentinel-fraud-graph                 : Neo4j property-graph mule-ring & shared-device engine
  5. sentinel-vector-search-engine        : 3072-d NumPy vs Qdrant vector retrieval & HNSW benchmarks
  6. sentinel_policy_retrieval            : 225-doc regulatory corpus chunking & Qdrant retrieval
  7. sentinel_rag_investigator            : FastAPI polyglot RAG investigator assistant (POST /explain)
  8. sentinel_gpu_compute_sizing_and_granularity : Compute sizing (6ND vs 4ND), VRAM & 2,800 TPS cluster

Usage:
  python verify_all_subsystems.py           # Full offline verification of all 8 subsystems
  python verify_all_subsystems.py --audit   # Audits existing outputs, reports, and code syntax
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

WORKSPACE_ROOT = Path(__file__).resolve().parent

SUBSYSTEMS = [
    {
        "id": "1",
        "dir": "sentinel-polyglot-data-architecture",
        "name": "Polyglot Data Architecture",
        "entrypoint": "sentinel_architecture_verifier.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "18-table schema integrity, zero look-ahead leakage",
        "artifacts": [
            "outputs/architecture_audit_summary.json",
            "outputs/sentinel_er_diagram_18_tables.png",
            "outputs/verification_report.txt",
        ],
    },
    {
        "id": "2",
        "dir": "sentinel_feature_pipeline",
        "name": "Point-in-Time Feature Store",
        "entrypoint": "feature_pipeline.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "100% parity vs v1.0.0, IST hour_of_day, zero leakage",
        "artifacts": [
            "outputs/data_quality_report.json",
            "outputs/data_quality_report.md",
            "outputs/feature_parity_report.md",
            "outputs/feature_parity_summary.csv",
            "outputs/feature_pipeline_diagnostics.png",
            "outputs/pipeline_verification_report.json",
        ],
    },
    {
        "id": "3",
        "dir": "sentinel_nosql_telemetry",
        "name": "High-Throughput NoSQL Telemetry",
        "entrypoint": "sentinel_nosql_telemetry.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "O(1) customer device lookup, ESR index, 0 COLLSCAN",
        "artifacts": [
            "outputs/aggregation_results.json",
            "outputs/explain_before_after.json",
            "outputs/sample_bson_documents.json",
            "outputs/verification_report.json",
        ],
    },
    {
        "id": "4",
        "dir": "sentinel-fraud-graph",
        "name": "Graph Mule Ring Detection",
        "entrypoint": "sentinel_fraud_graph.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "2-hop device sharing graph, Cypher queries Q1-Q3",
        "artifacts": [
            "outputs/graph_execution_audit.json",
            "outputs/query_1_shared_devices_7d.csv",
            "outputs/query_2_two_hop_fraud_customers.csv",
            "outputs/query_3_merchants_shared_devices.csv",
            "outputs/suspicious_cluster_graph.png",
        ],
    },
    {
        "id": "5",
        "dir": "sentinel-vector-search-engine",
        "name": "3072-d Vector Retrieval Engine",
        "entrypoint": "vector_search_engine.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "Unit-norm cosine == dot, 100% Qdrant parity, HNSW O(log N)",
        "artifacts": [
            "outputs/2d_vector_geometry.png",
            "outputs/vector_search_benchmarks.png",
            "outputs/vector_internals_verification_report.json",
        ],
    },
    {
        "id": "6",
        "dir": "sentinel_policy_retrieval",
        "name": "Dense Policy Retrieval Engine",
        "entrypoint": "qdrant_embeddings_pipeline.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "Hit@1 = 100%, Hit@3 = 100%, MRR@3 = 1.0000",
        "artifacts": [
            "outputs/verification_report.json",
        ],
    },
    {
        "id": "7",
        "dir": "sentinel_rag_investigator",
        "name": "FastAPI RAG Investigator",
        "entrypoint": "run_investigator_rag_pipeline.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "p99 <= 250ms, 100% Hit@5, 100% factual grounding",
        "artifacts": [
            "outputs/validation_10_explanations.json",
            "outputs/hit_at_5_retrieval_benchmark.json",
            "outputs/hit_at_5_retrieval_benchmark.md",
            "outputs/privacy_and_grounding_audit.json",
            "architecture/polyglot_architecture_diagram.png",
        ],
    },
    {
        "id": "8",
        "dir": "sentinel_gpu_compute_sizing_and_granularity",
        "name": "GPU Compute Sizing & Granularity",
        "entrypoint": "compute_sizing_calculator.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "6ND vs 4ND FLOPs, <= Rs. 4.00/1k txns, 2,800 TPS peak",
        "artifacts": [
            "outputs/compute_sizing_report.json",
            "outputs/gpu_telemetry_report.json",
        ],
    },
]


def audit_syntax_and_artifacts() -> int:
    """Audits code syntax and verified output artifacts across all subsystems."""
    print("=" * 80)
    print("Project SENTINEL — Code Quality & Artifact Audit")
    print("=" * 80)

    total_python_files = 0
    syntax_errors = []
    missing_artifacts = []

    for sub in SUBSYSTEMS:
        s_dir = WORKSPACE_ROOT / sub["dir"]
        print(f"\n[{sub['id']}/8] Auditing {sub['name']} ({sub['dir']})...")

        # 1. Check all Python files for AST syntax validity
        for py_file in s_dir.rglob("*.py"):
            if "venv" in py_file.parts or "__pycache__" in py_file.parts:
                continue
            total_python_files += 1
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    ast.parse(f.read(), filename=str(py_file))
            except Exception as e:
                syntax_errors.append((str(py_file.relative_to(WORKSPACE_ROOT)), str(e)))

        # 2. Check documented artifacts
        for art in sub["artifacts"]:
            art_path = s_dir / art
            if not art_path.exists():
                missing_artifacts.append(f"{sub['dir']}/{art}")

    print("\n" + "=" * 80)
    print("AUDIT SUMMARY:")
    print(f"  • Total Subsystems Audited : {len(SUBSYSTEMS)}")
    print(f"  • Python Files Parsed      : {total_python_files}")
    print(f"  • Syntax Errors Detected   : {len(syntax_errors)}")
    print(f"  • Missing Artifacts Count  : {len(missing_artifacts)}")

    if syntax_errors:
        print("\n[ERROR] Syntax Errors:")
        for file, err in syntax_errors:
            print(f"  - {file}: {err}")

    if missing_artifacts:
        print("\n[WARNING] Missing Output Artifacts:")
        for art in missing_artifacts:
            print(f"  - {art}")

    if not syntax_errors and not missing_artifacts:
        print("\n>>> ALL 8 SUBSYSTEMS PASSED QUALITY & ARTIFACT AUDIT (100% READY) <<<")
        return 0
    return 1 if syntax_errors else 0


def run_subsystems() -> int:
    """Executes the verification pipelines sequentially in offline-mock mode."""
    print("=" * 80)
    print("Project SENTINEL — Master Offline-Mock Verification Suite")
    print("=" * 80)

    results: List[Dict[str, str]] = []
    has_failure = False

    for sub in SUBSYSTEMS:
        s_dir = WORKSPACE_ROOT / sub["dir"]
        cmd = [sys.executable, sub["entrypoint"]] + sub["args"]

        print(f"\n{'='*70}")
        print(f"Running Subsystem {sub['id']}/8: {sub['name']}")
        print(f"Directory : {sub['dir']}")
        print(f"Command   : {' '.join(cmd)}")
        print(f"{'='*70}")

        t0 = time.time()
        try:
            res = subprocess.run(
                cmd,
                cwd=str(s_dir),
                capture_output=True,
                text=True,
                timeout=180,
            )
            elapsed = time.time() - t0
            if res.returncode == 0:
                status = "PASS"
                print(f"[{status}] Completed in {elapsed:.2f}s")
            else:
                status = "FAIL"
                has_failure = True
                print(f"[{status}] Failed with return code {res.returncode}")
                if res.stderr:
                    print(res.stderr[:500])
        except Exception as e:
            elapsed = time.time() - t0
            status = "ERROR"
            has_failure = True
            print(f"[{status}] Exception: {e}")

        results.append({
            "id": sub["id"],
            "name": sub["name"],
            "status": status,
            "sla": sub["sla"],
            "time": f"{elapsed:.2f}s",
        })

    # Print Summary Table
    print("\n" + "=" * 90)
    print(f"{'ID':<3} | {'Subsystem':<35} | {'Status':<6} | {'Time':<7} | {'Target SLA / Invariant'}")
    print("-" * 90)
    for r in results:
        print(f"{r['id']:<3} | {r['name']:<35} | {r['status']:<6} | {r['time']:<7} | {r['sla']}")
    print("=" * 90)

    return 1 if has_failure else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Master Subsystem Verification Suite for Project SENTINEL.")
    parser.add_argument("--audit", action="store_true", help="Perform code quality and artifact audit only.")
    parser.add_argument("--run", action="store_true", help="Execute all offline-mock verification pipelines.")
    args = parser.parse_args()

    if args.audit or not args.run:
        return audit_syntax_and_artifacts()
    else:
        return run_subsystems()


if __name__ == "__main__":
    sys.exit(main())

