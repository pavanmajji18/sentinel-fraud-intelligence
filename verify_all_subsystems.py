#!/usr/bin/env python3
"""
Project SENTINEL — Master Subsystems Verification & Quality Audit Runner.
Executes and audits all 8 specialized subsystems in offline-mock mode:
  1. sentinel-polyglot-data-architecture : 18-table relational & unstructured schema verifier
  2. sentinel-feature-pipeline           : Point-in-time feature store & leakage verification
  3. sentinel-nosql-telemetry            : MongoDB BSON schema, ESR compound indexing & aggregations
  4. sentinel-fraud-graph                : Neo4j property-graph mule-ring & shared-device engine
  5. sentinel-vector-search-engine       : 3072-d NumPy vs Qdrant vector retrieval & HNSW benchmarks
  6. sentinel-policy-retrieval           : 225-doc regulatory corpus chunking & Qdrant retrieval
  7. sentinel-rag-investigator           : FastAPI polyglot RAG investigator assistant (POST /explain)
  8. sentinel-gpu-compute-sizing         : Compute sizing (6ND vs 4ND), VRAM & 2,800 TPS cluster

Usage:
  python verify_all_subsystems.py --audit         # Portfolio hygiene, AST syntax, and artifact audit
  python verify_all_subsystems.py --offline-mock  # Full offline-mock execution of all 8 subsystems
  python verify_all_subsystems.py                 # Default: executes all 8 subsystem pipelines
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
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
            "outputs/polyglot_18_table_schema_reference.md",
            "outputs/verification_report.txt",
        ],
    },
    {
        "id": "2",
        "dir": "sentinel-feature-pipeline",
        "name": "Point-in-Time Feature Store",
        "entrypoint": "feature_pipeline.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "100% parity vs client store, IST hour_of_day, zero leakage",
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
        "dir": "sentinel-nosql-telemetry",
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
        "dir": "sentinel-policy-retrieval",
        "name": "Dense Policy Retrieval Engine",
        "entrypoint": "qdrant_embeddings_pipeline.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "Hit@1 = 100%, Hit@3 = 100%, MRR@3 = 1.0000",
        "artifacts": [
            "outputs/verification_report.json",
            "outputs/payload_filtered_playbooks_results.json",
            "outputs/policy_retrieval_10_queries_benchmark.md",
        ],
    },
    {
        "id": "7",
        "dir": "sentinel-rag-investigator",
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
        "dir": "sentinel-gpu-compute-sizing",
        "name": "GPU Compute Sizing & Granularity",
        "entrypoint": "compute_sizing_calculator.py",
        "args": ["--offline-mock", "--verify"],
        "sla": "6ND vs 4ND FLOPs, <= Rs. 4.00/1k txns, 2,800 TPS peak",
        "artifacts": [
            "outputs/compute_sizing_report.json",
            "outputs/gpu_telemetry_report.json",
            "outputs/gpu_benchmark_report.json",
            "outputs/gpu_utilization_chart.png",
        ],
    },
]


def verify_portfolio_hygiene() -> Tuple[bool, List[str]]:
    """
    Automated Portfolio Hygiene & Privacy Regression Gate (DPDP Act 2023 & PCI-DSS v4.0).
    Scans all tracked .py, .md, .json, .txt, and .cypher files to assert:
      1. Zero occurrences of classroom phrases ('Task 1'..'Task 8', 'Rubric', 'Homework', 'Assignment', 'Demonstrate 5 Required CRUD').
      2. Zero occurrences of internal case-study client names ('PayVista').
      3. Zero raw PII column leaks ('customer_name', 'aadhaar', 'pan', 'card_number', 'upi_id', 'vpa').
      4. 100% of referenced relative links and image paths in root README.md exist on disk.
    """
    print("\n" + "-" * 80)
    print("Running Automated Portfolio Hygiene & Privacy Regression Gate...")
    print("-" * 80)

    violations: List[str] = []

    # Get tracked git files
    try:
        res = subprocess.run(
            ["git", "ls-files"],
            cwd=str(WORKSPACE_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        tracked_files = [
            f.strip() for f in res.stdout.splitlines()
            if f.strip().endswith((".py", ".md", ".json", ".txt", ".cypher"))
        ]
    except Exception as e:
        # Fallback to filesystem scan if git command fails in constrained sandbox
        tracked_files = []
        for ext in ("*.py", "*.md", "*.json", "*.txt", "*.cypher"):
            for p in WORKSPACE_ROOT.rglob(ext):
                if any(x in p.parts for x in ("venv", ".venv", "__pycache__", ".git", ".pytest_cache")):
                    continue
                tracked_files.append(str(p.relative_to(WORKSPACE_ROOT)).replace("\\", "/"))

    # 1. Classroom Phrases
    classroom_phrases = [
        "Task 1", "Task 2", "Task 3", "Task 4",
        "Task 5", "Task 6", "Task 7", "Task 8",
        "Rubric", "Homework", "Assignment",
        "Demonstrate 5 Required CRUD",
    ]

    # 2. Case-Study Client Brand Name
    client_brand = "Pay" + "Vista"

    # 3. Raw PII Column Names
    raw_pii_columns = ["customer_name", "aadhaar", "pan", "card_number", "upi_id", "vpa"]
    pii_word_patterns = {col: re.compile(rf"\b{col}\b", re.IGNORECASE) for col in raw_pii_columns}

    # Scan tracked files
    this_script_name = Path(__file__).name
    files_scanned = 0

    for rel_path in tracked_files:
        if Path(rel_path).name == this_script_name:
            continue

        file_path = WORKSPACE_ROOT / rel_path
        if not file_path.exists():
            continue

        files_scanned += 1
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        # Check classroom phrases
        for phrase in classroom_phrases:
            pattern = re.compile(rf"\b{re.escape(phrase)}\b", re.IGNORECASE)
            if pattern.search(content):
                violations.append(f"Classroom phrase '{phrase}' detected in: {rel_path}")

        # Check internal client name
        if re.search(rf"\b{client_brand}\b", content, re.IGNORECASE):
            violations.append(f"Internal client identifier '{client_brand}' detected in: {rel_path}")

        # Check raw PII column leaks (Strictly forbidden in data/JSON/TXT/Cypher files)
        if rel_path.endswith((".json", ".txt", ".cypher")):
            for col, pat in pii_word_patterns.items():
                if pat.search(content):
                    violations.append(f"Raw PII column leak '{col}' detected in data file: {rel_path}")

    # 4. Check relative links and images in root README.md
    readme_path = WORKSPACE_ROOT / "README.md"
    if readme_path.exists():
        readme_text = readme_path.read_text(encoding="utf-8")
        # Extract markdown links and HTML img sources
        md_links = re.findall(r"\[.*?\]\((.*?)\)", readme_text)
        img_srcs = re.findall(r"<img\s+[^>]*?src=[\"'](.*?)[\"']", readme_text)
        all_links = md_links + img_srcs

        for link in all_links:
            clean_link = link.strip()
            # Ignore external URLs, anchors, and email links
            if (
                clean_link.startswith(("http://", "https://", "mailto:", "#"))
                or not clean_link
            ):
                continue

            target_path_str = clean_link.split("#")[0].split("?")[0]
            if not target_path_str:
                continue

            target_path = WORKSPACE_ROOT / target_path_str
            if not target_path.exists():
                violations.append(f"Broken relative link/image in README.md: '{clean_link}' -> missing '{target_path_str}'")

    print(f"  • Tracked Files Scanned   : {files_scanned}")
    print(f"  • Hygiene Violations Found: {len(violations)}")
    if violations:
        print("\n[HYGIENE VIOLATIONS]:")
        for v in violations:
            print(f"  - {v}")
        return False, violations

    print("  [PASS] Zero classroom phrases detected ('Task 1'..'Task 8', 'Assignment', 'Rubric', etc.)")
    print("  [PASS] Zero internal client identifiers detected ('PayVista')")
    print("  [PASS] Zero raw PII column leaks detected in data artifacts ('customer_name', 'aadhaar', 'pan', etc.)")
    print("  [PASS] 100% of referenced relative links and image paths in root README.md exist on disk")
    return True, []


def audit_syntax_and_artifacts() -> int:
    """Audits code syntax, portfolio hygiene, and verified output artifacts across all subsystems."""
    print("=" * 80)
    print("Project SENTINEL — Code Quality & Artifact Audit")
    print("=" * 80)

    total_python_files = 0
    syntax_errors = []
    missing_artifacts = []

    # 1. Run Portfolio Hygiene & Privacy Gate
    hygiene_ok, hygiene_violations = verify_portfolio_hygiene()

    # 2. Check all Subsystems for AST syntax validity & artifacts
    for sub in SUBSYSTEMS:
        s_dir = WORKSPACE_ROOT / sub["dir"]
        print(f"\n[{sub['id']}/8] Auditing {sub['name']} ({sub['dir']})...")

        for py_file in s_dir.rglob("*.py"):
            if any(x in py_file.parts for x in ("venv", ".venv", "__pycache__")):
                continue
            total_python_files += 1
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    ast.parse(f.read(), filename=str(py_file))
            except Exception as e:
                syntax_errors.append((str(py_file.relative_to(WORKSPACE_ROOT)), str(e)))

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
    print(f"  • Portfolio Hygiene Status : {'PASS' if hygiene_ok else 'FAIL'}")

    if syntax_errors:
        print("\n[ERROR] Syntax Errors:")
        for file, err in syntax_errors:
            print(f"  - {file}: {err}")

    if missing_artifacts:
        print("\n[WARNING] Missing Output Artifacts:")
        for art in missing_artifacts:
            print(f"  - {art}")

    if not syntax_errors and not missing_artifacts and hygiene_ok:
        print("\n>>> ALL 8 SUBSYSTEMS PASSED QUALITY, PRIVACY & ARTIFACT AUDIT (100% READY) <<<")
        return 0
    return 1


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
    parser.add_argument("--audit", action="store_true", help="Perform portfolio hygiene, code quality, and artifact audit.")
    parser.add_argument("--offline-mock", action="store_true", help="Execute all offline-mock verification pipelines.")
    parser.add_argument("--run", action="store_true", help="Execute all offline-mock verification pipelines.")
    args = parser.parse_args()

    if args.audit:
        return audit_syntax_and_artifacts()
    else:
        # Default behavior: run all subsystem pipelines end-to-end
        return run_subsystems()


if __name__ == "__main__":
    sys.exit(main())
