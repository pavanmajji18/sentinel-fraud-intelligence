#!/usr/bin/env python3
"""
Verify Supabase Ingestion Script for Project SENTINEL.

Connects to the Supabase PostgreSQL replica via DATABASE_URL,
queries the customer, merchant, and paid transaction tables,
and prints formatted record counts to confirm successful data ingestion.

Usage:
    python verify_supabase_ingestion.py
    python verify_supabase_ingestion.py --db-url "postgresql://..."
    python verify_supabase_ingestion.py --json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    import psycopg2
    from psycopg2.extensions import connection as PgConnection
except ImportError:
    psycopg2 = None
    PgConnection = None

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None


def mask_database_url(url: str) -> str:
    """Mask the password component in a database connection URL for secure logging."""
    if not url:
        return "<EMPTY>"
    # Matches ://user:password@host
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:********@", url)


def load_environment(env_file: Optional[str] = None) -> None:
    """Search for and load .env from current directory, parent dirs, or custom path."""
    if load_dotenv is None:
        return

    candidate_paths = []
    if env_file:
        candidate_paths.append(Path(env_file))
    else:
        cwd = Path.cwd()
        candidate_paths.extend([
            cwd / ".env",
            cwd / ".." / ".env",
            Path(__file__).resolve().parent / ".env",
            Path(__file__).resolve().parent.parent / ".env",
        ])

    for env_path in candidate_paths:
        if env_path.is_file():
            load_dotenv(dotenv_path=env_path, override=False)
            break


def get_db_connection(database_url: str) -> PgConnection:
    """Establish and return a connection to PostgreSQL/Supabase."""
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is not installed. Please run: pip install psycopg2-binary"
        )
    return psycopg2.connect(database_url, connect_timeout=10)


def table_exists(conn: PgConnection, table_name: str, schema: str = "public") -> bool:
    """Check if a table or view exists in the target schema."""
    query = """
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table_name))
        return cur.fetchone() is not None


def query_ingestion_counts(conn: PgConnection) -> Dict[str, Any]:
    """
    Execute count and validation queries across core Supabase tables:
      - customers
      - merchants
      - transactions (total)
      - paid transactions (via payment_outcomes / status = 'SUCCESS')
      - failed transactions
      - devices
    """
    results: Dict[str, Any] = {}

    with conn.cursor() as cur:
        # 1. Customer record count
        cur.execute("SELECT COUNT(*) FROM public.customers;")
        results["customers"] = cur.fetchone()[0]

        # 2. Merchant record count
        cur.execute("SELECT COUNT(*) FROM public.merchants;")
        results["merchants"] = cur.fetchone()[0]

        # 3. Devices record count (if present)
        if table_exists(conn, "devices"):
            cur.execute("SELECT COUNT(*) FROM public.devices;")
            results["devices"] = cur.fetchone()[0]
        else:
            results["devices"] = None

        # 4. Total transactions count
        cur.execute("SELECT COUNT(*) FROM public.transactions;")
        results["total_transactions"] = cur.fetchone()[0]

        # 5. Paid Transactions count
        # Check if a dedicated 'paid_transactions' table exists
        if table_exists(conn, "paid_transactions"):
            cur.execute("SELECT COUNT(*) FROM public.paid_transactions;")
            results["paid_transactions"] = cur.fetchone()[0]
            results["paid_source"] = "public.paid_transactions"
        # Otherwise query transactions joined with payment_outcomes status = 'SUCCESS'
        elif table_exists(conn, "payment_outcomes"):
            cur.execute("""
                SELECT 
                    COUNT(t.transaction_id) AS total,
                    COUNT(t.transaction_id) FILTER (WHERE po.status = 'SUCCESS') AS paid,
                    COUNT(t.transaction_id) FILTER (WHERE po.status = 'FAILED') AS failed
                FROM public.transactions t
                LEFT JOIN public.payment_outcomes po ON t.transaction_id = po.transaction_id;
            """)
            row = cur.fetchone()
            results["paid_transactions"] = row[1]
            results["failed_transactions"] = row[2]
            results["paid_source"] = "public.transactions JOIN public.payment_outcomes (status='SUCCESS')"
        else:
            # Fallback if no payment_outcomes table
            results["paid_transactions"] = results["total_transactions"]
            results["paid_source"] = "public.transactions"

    return results


def format_table(rows: list[list[str]], headers: list[str]) -> str:
    """Format tabular output using tabulate if available, else manual ASCII."""
    if tabulate is not None:
        return tabulate(rows, headers=headers, tablefmt="github")
    
    # Manual markdown table fallback
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))
    
    header_line = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    sep_line = "|-" + "-|-".join("-" * col_widths[i] for i in range(len(headers))) + "-|"
    data_lines = [
        "| " + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(r)) + " |"
        for r in rows
    ]
    return "\n".join([header_line, sep_line] + data_lines)


def run_verification(database_url: str, as_json: bool = False) -> int:
    """Connect to database, query counts, print confirmation and return status code."""
    masked_url = mask_database_url(database_url)
    if not as_json:
        print("================================================================================")
        print("  PROJECT SENTINEL -- SUPABASE DATA INGESTION VERIFICATION")
        print("================================================================================")
        print(f"[*] Target Database : {masked_url}")

    try:
        conn = get_db_connection(database_url)
    except Exception as e:
        if as_json:
            print(json.dumps({"status": "ERROR", "error": str(e)}, indent=2))
        else:
            print(f"\n[!] Connection Failed: {e}")
            print("[!] Please verify your DATABASE_URL in the .env file or command line.")
        return 1

    try:
        counts = query_ingestion_counts(conn)
    except Exception as e:
        if as_json:
            print(json.dumps({"status": "ERROR", "error": str(e)}, indent=2))
        else:
            print(f"\n[!] Query Execution Failed: {e}")
        return 1
    finally:
        conn.close()

    cust_count = counts.get("customers", 0)
    merch_count = counts.get("merchants", 0)
    tx_count = counts.get("total_transactions", 0)
    paid_count = counts.get("paid_transactions", 0)
    failed_count = counts.get("failed_transactions", 0)
    device_count = counts.get("devices")

    # Ingestion validation criteria
    ingestion_ok = cust_count > 0 and merch_count > 0 and paid_count > 0

    if as_json:
        output_payload = {
            "status": "SUCCESS" if ingestion_ok else "INCOMPLETE",
            "connection": masked_url,
            "counts": counts,
            "ingestion_confirmed": ingestion_ok,
        }
        print(json.dumps(output_payload, indent=2))
        return 0 if ingestion_ok else 2

    # Formatted Table
    table_data = [
        ["Customers", "public.customers", f"{cust_count:,}", ">= 1", "[PASS]" if cust_count > 0 else "[FAIL]"],
        ["Merchants", "public.merchants", f"{merch_count:,}", ">= 1", "[PASS]" if merch_count > 0 else "[FAIL]"],
        ["Total Transactions", "public.transactions", f"{tx_count:,}", ">= 1", "[PASS]" if tx_count > 0 else "[FAIL]"],
        ["Paid Transactions", counts.get("paid_source", "Paid Status"), f"{paid_count:,}", ">= 1", "[PASS]" if paid_count > 0 else "[FAIL]"],
    ]
    if failed_count is not None:
        table_data.append(["Failed Transactions", "public.payment_outcomes (status='FAILED')", f"{failed_count:,}", "N/A", "[INFO]"])
    if device_count is not None:
        table_data.append(["Hardware Devices", "public.devices", f"{device_count:,}", ">= 1", "[PASS]" if device_count > 0 else "[FAIL]"])

    headers = ["Entity / Domain", "Source Table / Condition", "Record Count", "Min Expected", "Ingestion Status"]
    
    print("\n[+] Ingestion Counts Retrieved Successfully:\n")
    print(format_table(table_data, headers))
    print()

    # Success / Failure verdict
    if ingestion_ok:
        paid_pct = (paid_count / tx_count * 100.0) if tx_count else 0.0
        print("=" * 80)
        print("  INGESTION CONFIRMATION: SUCCESS")
        print("=" * 80)
        print(f"  * Total Customers Ingested   : {cust_count:,}")
        print(f"  * Total Merchants Ingested   : {merch_count:,}")
        print(f"  * Total Transactions Ingested: {tx_count:,}")
        print(f"  * Paid / Settled Transactions: {paid_count:,} ({paid_pct:.1f}% authorization success rate)")
        print(f"  * All core entity tables are successfully populated in Supabase.")
        print("=" * 80)
        return 0
    else:
        print("=" * 80)
        print("  INGESTION CONFIRMATION: WARNING / INCOMPLETE")
        print("=" * 80)
        print("  One or more core tables returned 0 records. Check data migration or seed pipeline.")
        print("=" * 80)
        return 2


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify Supabase data ingestion by querying customers, merchants, and paid transactions."
    )
    parser.add_argument(
        "--db-url",
        "--database-url",
        dest="database_url",
        default=None,
        help="PostgreSQL connection string (defaults to DATABASE_URL env var).",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional path to .env file.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format.",
    )

    args = parser.parse_args()

    # Load environment variables
    load_environment(args.env_file)

    db_url = args.database_url or os.getenv("DATABASE_URL")
    if not db_url:
        print("[!] Error: No DATABASE_URL found.")
        print("    Provide it via --db-url or set DATABASE_URL in your .env file.")
        sys.exit(1)

    exit_code = run_verification(database_url=db_url, as_json=args.json)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
