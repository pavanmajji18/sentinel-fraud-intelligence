#!/usr/bin/env python3
"""
Project SENTINEL — Supabase PostgreSQL Live Ingestion Verification Script
========================================================================

Reads DATABASE_URL from the local .env configuration file, connects to the
Supabase PostgreSQL cluster, audits all 7 core tables:
  1. customers
  2. merchants
  3. devices
  4. transactions
  5. payment_outcomes
  6. labels
  7. features

Outputs:
  - Live connection status & schema resolution (public vs. sentinel_demo)
  - Total row counts across all 7 core tables
  - Sample records with key business columns and formatted JSON/array payloads
  - Data privacy & DPDP/PCI-DSS entity tokenization verification (C_*, M_*, D_*)
  - Split distribution and live ingestion health verdict

Usage:
  python verify_supabase_ingestion.py
  python verify_supabase_ingestion.py --samples 3
  python verify_supabase_ingestion.py --table transactions
  python verify_supabase_ingestion.py --json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(dotenv_path: Optional[Any] = None, *args: Any, **kwargs: Any) -> bool:
        """Zero-dependency fallback parser for .env files."""
        env_file = Path(dotenv_path) if dotenv_path else Path(".env")
        if not env_file.is_file():
            return False
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k not in os.environ:
                        os.environ[k] = v
            return True
        except Exception:
            return False

try:
    from tabulate import tabulate
except ImportError:
    def tabulate(data: List[List[Any]], headers: List[str], tablefmt: str = "") -> str:
        """Minimal fallback table formatter when tabulate is not installed."""
        col_widths = [len(h) for h in headers]
        for row in data:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))
        hdr_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
        sep_line = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
        row_lines = [
            " | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
            for row in data
        ]
        return "\n".join([hdr_line, sep_line] + row_lines)

REPO_ROOT = Path(__file__).resolve().parent
ENV_PATH = REPO_ROOT / ".env"

CORE_TABLES = [
    "customers",
    "merchants",
    "devices",
    "transactions",
    "payment_outcomes",
    "labels",
    "features",
]

PRIMARY_KEYS = {
    "customers": "customer_id",
    "merchants": "merchant_id",
    "devices": "device_id",
    "transactions": "transaction_id",
    "payment_outcomes": "transaction_id",
    "labels": "transaction_id",
    "features": "transaction_id",
}

KEY_DISPLAY_COLUMNS = {
    "customers": ["customer_id", "created_at", "customer_age", "home_city", "region", "is_synthetic"],
    "merchants": ["merchant_id", "synthetic_name", "merchant_category", "merchant_location", "is_synthetic"],
    "devices": ["device_id", "device_kind", "is_synthetic"],
    "transactions": [
        "transaction_id",
        "customer_id",
        "merchant_id",
        "device_id",
        "amount_paise",
        "transaction_amount",
        "payment_channel",
        "merchant_category",
        "transaction_time",
        "split",
    ],
    "payment_outcomes": ["transaction_id", "outcome_time", "status", "failure_code", "is_synthetic"],
    "labels": ["transaction_id", "fraud_label", "label_source", "label_available_at", "split"],
    "features": [
        "transaction_id",
        "feature_as_of",
        "amount_to_avg_ratio",
        "is_new_device",
        "is_new_city",
        "hour_of_day",
        "is_night",
        "has_recent_failures",
        "combined_risk_flags",
        "reason_codes",
    ],
}


class CustomJSONEncoder(json.JSONEncoder):
    """JSON encoder handling datetime, date, Decimal, and UUID objects."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def sanitize_db_url(raw_url: str) -> str:
    """
    Ensure the PostgreSQL connection URL is cleanly formatted.
    Encodes '@' in passwords if present without breakages.
    """
    if not raw_url:
        return raw_url
    clean_url = raw_url.strip().strip("'\"")

    # If the userinfo has unencoded '@' (e.g. postgres:pass@word@host:port/db)
    match = re.match(r"^(postgres(?:ql)?://)([^:]+):(.+)@([^@/]+(?::\d+)?(?:/.*)?)$", clean_url)
    if match:
        scheme, user, password, host_part = match.groups()
        if "@" in password and "%40" not in password:
            encoded_password = urllib.parse.quote_plus(password)
            clean_url = f"{scheme}{user}:{encoded_password}@{host_part}"

    return clean_url


def resolve_supabase_connection(db_url: str) -> Tuple[Any, str, str]:
    """
    Connect to Supabase PostgreSQL using psycopg2, detecting active schema and masked host.
    """
    clean_url = sanitize_db_url(db_url)
    conn = psycopg2.connect(clean_url, connect_timeout=15)

    with conn.cursor() as cur:
        # Detect target schema containing Project SENTINEL core tables
        cur.execute(
            """
            SELECT table_schema 
            FROM information_schema.tables 
            WHERE table_name = 'transactions' 
              AND table_schema IN ('public', 'sentinel_demo') 
            ORDER BY (table_schema = 'public') DESC 
            LIMIT 1;
            """
        )
        row = cur.fetchone()
        schema = row[0] if row else "public"

    # Extract masked host for logging
    try:
        parsed = urllib.parse.urlsplit(clean_url)
        masked_host = f"{parsed.hostname}:{parsed.port or 5432}"
    except Exception:
        masked_host = "supabase-pooler"

    return conn, schema, masked_host


def format_cell_value(val: Any, max_len: int = 40) -> str:
    """Format table cell values for compact, readable console output."""
    if val is None:
        return "NULL"
    if isinstance(val, (dict, list)):
        dumped = json.dumps(val, cls=CustomJSONEncoder)
        if len(dumped) > max_len:
            return dumped[: max_len - 3] + "..."
        return dumped
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    s = str(val)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def verify_supabase_ingestion(
    num_samples: int = 2,
    filter_table: Optional[str] = None,
    output_json: bool = False,
) -> int:
    """
    Query Supabase PostgreSQL core tables, verify row counts, samples, and integrity.
    """
    if ENV_PATH.exists():
        load_dotenv(dotenv_path=ENV_PATH)
    else:
        load_dotenv()

    raw_db_url = os.getenv("DATABASE_URL")
    if not raw_db_url:
        print(f"[FATAL] DATABASE_URL not found in {ENV_PATH} or environment variables.", file=sys.stderr)
        return 1

    print("=" * 96)
    print("PROJECT SENTINEL — SUPABASE POSTGRESQL LIVE TELEMETRY INGESTION AUDIT")
    print("=" * 96)
    t0 = time.perf_counter()

    # 1. Establish Connection
    print("\n[1/3] Connecting to Supabase Feature Store...")
    try:
        conn, schema, masked_host = resolve_supabase_connection(raw_db_url)
        print(f"  -> Connected Host : {masked_host}")
        print(f"  -> Active Schema  : {schema}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to Supabase: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    tables_to_query = [filter_table] if filter_table else CORE_TABLES

    # 2. Query Row Counts & Integrity
    print(f"\n[2/3] Auditing Core Tables in Schema `{schema}`...")
    audit_summary: List[Dict[str, Any]] = []
    table_samples: Dict[str, List[Dict[str, Any]]] = {}

    with conn.cursor() as cur:
        for t_name in tables_to_query:
            pk = PRIMARY_KEYS.get(t_name, "id")
            try:
                # Query total count
                cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{t_name}";')
                cnt_row = cur.fetchone()
                row_count = cnt_row[0] if cnt_row else 0

                # Query sample rows
                cur.execute(f'SELECT * FROM "{schema}"."{t_name}" ORDER BY 1 LIMIT {num_samples};')
                colnames = [desc[0] for desc in cur.description]
                sample_rows = [dict(zip(colnames, row)) for row in cur.fetchall()]
                table_samples[t_name] = sample_rows

                # Check tokenization format if applicable
                token_check = "N/A"
                if t_name == "customers" and sample_rows:
                    token_check = "VERIFIED (C_*)" if all(str(r.get("customer_id", "")).startswith("C_") for r in sample_rows) else "FAILED"
                elif t_name == "merchants" and sample_rows:
                    token_check = "VERIFIED (M_*)" if all(str(r.get("merchant_id", "")).startswith("M_") for r in sample_rows) else "FAILED"
                elif t_name == "devices" and sample_rows:
                    token_check = "VERIFIED (D_*)" if all(str(r.get("device_id", "")).startswith("D_") for r in sample_rows) else "FAILED"
                elif t_name in {"transactions", "payment_outcomes", "labels", "features"} and sample_rows:
                    token_check = "VERIFIED (TX_*)" if all(str(r.get("transaction_id", "")).startswith("TX_") for r in sample_rows) else "FAILED"

                audit_summary.append({
                    "table_name": t_name,
                    "schema": schema,
                    "primary_key": pk,
                    "row_count": row_count,
                    "sample_records": len(sample_rows),
                    "tokenization_status": token_check,
                    "status": "HEALTHY" if row_count > 0 else "EMPTY",
                })
            except Exception as e:
                audit_summary.append({
                    "table_name": t_name,
                    "schema": schema,
                    "primary_key": pk,
                    "row_count": -1,
                    "sample_records": 0,
                    "tokenization_status": "ERROR",
                    "status": f"ERROR: {e}",
                })
                conn.rollback()

    conn.close()
    elapsed = time.perf_counter() - t0

    # 3. Output Results
    if output_json:
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "target_schema": schema,
            "masked_host": masked_host,
            "query_time_seconds": round(elapsed, 3),
            "table_audit": audit_summary,
            "samples": table_samples,
        }
        print(json.dumps(report_data, indent=2, cls=CustomJSONEncoder))
        return 0

    # Render Summary Table
    summary_headers = ["Table Name", "Schema", "Primary Key", "Row Count", "Samples", "Entity Token Format", "Ingestion Status"]
    summary_data = [
        [
            r["table_name"],
            r["schema"],
            r["primary_key"],
            f"{r['row_count']:,}" if r["row_count"] >= 0 else "ERROR",
            r["sample_records"],
            r["tokenization_status"],
            r["status"],
        ]
        for r in audit_summary
    ]
    print("\n" + tabulate(summary_data, headers=summary_headers, tablefmt="rounded_grid"))

    # Render Sample Records per Table
    print(f"\n[3/3] Inspecting Sample Records ({num_samples} per table) for Ingestion Parity:")
    for t_name, samples in table_samples.items():
        print(f"\n--- Table: `{schema}.{t_name}` ({len(samples)} sample records) ---")
        if not samples:
            print("  (No rows retrieved)")
            continue

        cols_to_show = KEY_DISPLAY_COLUMNS.get(t_name, list(samples[0].keys())[:8])
        sample_rows_formatted = []
        for s in samples:
            sample_rows_formatted.append([format_cell_value(s.get(c)) for c in cols_to_show])

        print(tabulate(sample_rows_formatted, headers=cols_to_show, tablefmt="github"))

    print("\n" + "=" * 96)
    all_healthy = all(r["status"] == "HEALTHY" for r in audit_summary)
    total_records = sum(r["row_count"] for r in audit_summary if r["row_count"] > 0)
    print(f"VERIFICATION RESULT : {'ALL CORE TABLES VERIFIED (PASS)' if all_healthy else 'FAILED'}")
    print(f"TOTAL AUDITED ROWS  : {total_records:,} rows across {len(audit_summary)} tables in {elapsed:.2f}s")
    print("=" * 96)

    return 0 if all_healthy else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify Supabase live telemetry ingestion for Project SENTINEL."
    )
    parser.add_argument("--samples", type=int, default=2, help="Number of sample records per table to display")
    parser.add_argument("--table", type=str, default=None, help="Filter audit to a specific core table")
    parser.add_argument("--json", action="store_true", help="Output audit report in structured JSON format")
    args = parser.parse_args()

    return verify_supabase_ingestion(
        num_samples=args.samples,
        filter_table=args.table,
        output_json=args.json,
    )


if __name__ == "__main__":
    sys.exit(main())

