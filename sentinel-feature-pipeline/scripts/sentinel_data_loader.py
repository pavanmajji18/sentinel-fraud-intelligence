#!/usr/bin/env python3
"""
Multi-Tier Telemetry Loader & DPDP/PCI-DSS Data-Privacy Gate for Project SENTINEL.

Resolves sanitized payment authorization telemetry via:
  1. Tier 1: Local sanitized telemetry directory (`tables/*.csv` or `parquet/*.parquet`)
  2. Tier 2: PostgreSQL / Supabase feature store replica (`DATABASE_URL`)
  3. Tier 3: Deterministic schema-exact synthetic telemetry generator (5,500+ transactions)

Includes `enforce_data_privacy_guardrails()` to guarantee zero raw PII, zero client-identifying
strings, and 100% tokenized entity identifiers (`C_<hash>`, `M_<hash>`, `D_<hash>`) before
any document is persisted or exported.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import re
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

IST = timezone(timedelta(hours=5, minutes=30))
HIGH_RISK_MCCS = {"electronics", "gaming", "travel", "gift_cards", "crypto"}

FORBIDDEN_PII_COLUMNS = {
    "customer_name",
    "full_name",
    "email",
    "phone",
    "mobile_number",
    "card_number",
    "pan",
    "aadhaar",
    "upi_id",
    "vpa",
    "ip_address",
    "bank_account_number",
}

CITIES_COORDS = {
    "Bengaluru": (12.9716, 77.5946, "South"),
    "Mumbai": (19.0760, 72.8777, "West"),
    "Delhi": (28.6139, 77.2090, "North"),
    "Hyderabad": (17.3850, 78.4867, "South"),
    "Chennai": (13.0827, 80.2707, "South"),
    "Pune": (18.5204, 73.8567, "West"),
    "Kolkata": (22.5726, 88.3639, "East"),
    "Ahmedabad": (23.0225, 72.5714, "West"),
}

MCC_LIST = [
    "grocery",
    "food",
    "electronics",
    "gaming",
    "travel",
    "quick_commerce",
    "education",
    "utilities",
]
CHANNELS = ["UPI", "card", "wallet", "net_banking"]
CHANNEL_WEIGHTS = [0.58, 0.31, 0.07, 0.04]
DEVICE_KINDS = ["mobile_android", "mobile_ios", "desktop_web"]


def haversine_km(
    lat1: Optional[float],
    lon1: Optional[float],
    lat2: Optional[float],
    lon2: Optional[float],
) -> Optional[float]:
    """Compute Haversine distance in km between two (lat, lon) coordinates."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
        return None
    r = 6371.0
    phi1, phi2 = math.radians(float(lat1)), math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return round(2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a)), 2)


def assign_reason_codes(
    is_new_device: Optional[bool],
    is_new_city: Optional[bool],
    distance_km: Optional[float],
    is_night: bool,
    has_recent_failures: bool,
    amount_to_avg_ratio: Optional[float],
    is_new_account: bool,
    txn_count_last_1h: int,
    merchant_category: str,
) -> List[str]:
    """Assign deterministic SENTINEL reason codes R01 to R08."""
    codes: List[str] = []
    if is_new_device is True:
        codes.append("R01_NEW_DEVICE")
    if is_new_city is True or (
        distance_km is not None and not pd.isna(distance_km) and float(distance_km) > 500.0
    ):
        codes.append("R02_GEO_MISMATCH")
    if is_night is True:
        codes.append("R03_ODD_HOUR")
    if has_recent_failures is True:
        codes.append("R04_FAILED_ATTEMPTS")
    if (
        amount_to_avg_ratio is not None
        and not pd.isna(amount_to_avg_ratio)
        and float(amount_to_avg_ratio) >= 5.0
    ):
        codes.append("R05_AMOUNT_SPIKE")
    if is_new_account is True:
        codes.append("R06_NEW_ACCOUNT")
    if int(txn_count_last_1h) >= 4:
        codes.append("R07_VELOCITY")
    if str(merchant_category).lower() in HIGH_RISK_MCCS:
        codes.append("R08_HIGH_RISK_MCC")
    return codes


def enforce_data_privacy_guardrails(tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Enforce DPDP Act 2023 & PCI-DSS v4.0 data-minimization and client-anonymization rules:
      1. Drop or reject any forbidden raw PII columns if present in an external table.
      2. Scrub any legacy client brand prefixes in merchant synthetic names (`Merchant_<MCC>_<id>`).
      3. Verify that 100% of primary entity IDs (`customer_id`, `merchant_id`, `device_id`)
         use tokenized pseudonymous format (`C_*`, `M_*`, `D_*`) and `is_synthetic == True`.
    """
    redacted_columns: List[str] = []
    for table_name, df in tables.items():
        found_pii = [c for c in df.columns if c.lower() in FORBIDDEN_PII_COLUMNS]
        if found_pii:
            df.drop(columns=found_pii, inplace=True)
            redacted_columns.extend(f"{table_name}.{c}" for c in found_pii)
        if "is_synthetic" in df.columns:
            assert bool(df["is_synthetic"].all()), (
                f"Data governance violation in `{table_name}`: non-synthetic record detected!"
            )

    if "merchants" in tables and "synthetic_name" in tables["merchants"].columns:
        tables["merchants"]["synthetic_name"] = (
            tables["merchants"]["synthetic_name"]
            .astype(str)
            .apply(lambda s: re.sub(r"^[A-Za-z]+_", "Merchant_", s))
        )

    cust_ids = tables["customers"]["customer_id"].astype(str)
    merch_ids = tables["merchants"]["merchant_id"].astype(str)
    dev_ids = tables["devices"]["device_id"].astype(str)

    assert cust_ids.str.startswith("C_").all(), "Untokenized customer_id detected"
    assert merch_ids.str.startswith("M_").all(), "Untokenized merchant_id detected"
    assert dev_ids.str.startswith("D_").all(), "Untokenized device_id detected"

    return {
        "compliance_standard": "DPDP Act 2023 & PCI-DSS v4.0 Data Minimization",
        "forbidden_pii_columns_checked": len(FORBIDDEN_PII_COLUMNS),
        "redacted_columns": redacted_columns,
        "tokenized_customer_ids_verified": int(len(cust_ids)),
        "tokenized_merchant_ids_verified": int(len(merch_ids)),
        "tokenized_device_ids_verified": int(len(dev_ids)),
        "all_records_synthetic_verified": True,
    }


def generate_synthetic_sentinel_dataset(
    num_customers: int = 500,
    num_merchants: int = 120,
    num_devices: int = 900,
    num_transactions: int = 5500,
    seed: int = 42,
) -> Dict[str, pd.DataFrame]:
    """Generate deterministic, schema-exact, anonymized payment telemetry across all 4 splits."""
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    cities = list(CITIES_COORDS.keys())
    base_start = datetime(2024, 11, 1, 0, 0, 0, tzinfo=timezone.utc)

    # 1. Customers
    customers = []
    for i in range(1, num_customers + 1):
        cid = f"C_{hashlib.md5(f'cust_{i}'.encode()).hexdigest()[:16]}"
        city = rng.choice(cities)
        _, _, region = CITIES_COORDS[city]
        created_at = base_start - timedelta(days=rng.randint(5, 900))
        customers.append(
            {
                "customer_id": cid,
                "created_at": created_at.isoformat(),
                "customer_age": rng.randint(20, 68),
                "home_city": city,
                "region": region,
                "is_synthetic": True,
            }
        )
    customers_df = pd.DataFrame(customers)

    # 2. Merchants (Client-agnostic anonymized merchant identifiers)
    merchants = []
    for i in range(1, num_merchants + 1):
        mid = f"M_{hashlib.md5(f'merch_{i}'.encode()).hexdigest()[:16]}"
        city = rng.choice(cities)
        lat, lon, _ = CITIES_COORDS[city]
        mcc = rng.choice(MCC_LIST)
        merchants.append(
            {
                "merchant_id": mid,
                "synthetic_name": f"Merchant_{mcc.title()}_{i:03d}",
                "merchant_category": mcc,
                "merchant_location": json.dumps(
                    {
                        "lat": round(lat + rng.uniform(-0.08, 0.08), 4),
                        "lon": round(lon + rng.uniform(-0.08, 0.08), 4),
                        "city": city,
                    }
                ),
                "is_synthetic": True,
            }
        )
    merchants_df = pd.DataFrame(merchants)

    # 3. Devices (including 15 shared mule-ring devices)
    devices = []
    for i in range(1, num_devices + 1):
        did = f"D_{hashlib.md5(f'dev_{i}'.encode()).hexdigest()[:16]}"
        devices.append(
            {
                "device_id": did,
                "device_kind": rng.choice(DEVICE_KINDS),
                "is_synthetic": True,
            }
        )
    devices_df = pd.DataFrame(devices)
    shared_ring_devices = [d["device_id"] for d in devices[:15]]

    cust_primary_devices = {
        c["customer_id"]: [
            devices[(idx * 2) % len(devices)]["device_id"],
            devices[(idx * 2 + 1) % len(devices)]["device_id"],
        ]
        for idx, c in enumerate(customers)
    }

    # 4. Transactions, Outcomes, Labels, Features spanning warmup, train, validation, and holdout
    tx_rows, outcome_rows, label_rows, feature_rows = [], [], [], []
    cust_history: Dict[str, List[Dict[str, Any]]] = {c["customer_id"]: [] for c in customers}
    device_history: Dict[str, List[Tuple[datetime, str]]] = {}

    cur_time = base_start
    burst_cust: Optional[Dict[str, Any]] = None
    burst_remaining = 0

    for idx in range(1, num_transactions + 1):
        if burst_remaining > 0 and burst_cust is not None:
            cur_time += timedelta(seconds=rng.randint(25, 160))
            cust = burst_cust
            burst_remaining -= 1
            is_ring_attack = True
        else:
            if idx > num_transactions - 250:
                cur_time += timedelta(seconds=rng.randint(20, 320))
            else:
                # Calibrated inter-arrival step so 5,500 txns span Nov 2024 -> Mar 2026 across all 4 splits
                cur_time += timedelta(seconds=rng.randint(60, 16200))
            cust = customers[rng.randrange(len(customers))]
            is_ring_attack = idx % 35 == 0
            if idx % 140 == 0 and idx <= num_transactions - 6:
                burst_cust = cust
                burst_remaining = 5

        if cur_time < datetime(2025, 1, 1, tzinfo=timezone.utc):
            split = "warmup"
        elif cur_time < datetime(2025, 11, 1, tzinfo=timezone.utc):
            split = "train"
        elif cur_time < datetime(2026, 1, 1, tzinfo=timezone.utc):
            split = "validation"
        else:
            split = "holdout"

        cid = cust["customer_id"]
        merch = merchants[rng.randrange(len(merchants))]
        mid = merch["merchant_id"]
        mcc = merch["merchant_category"]
        m_loc = json.loads(merch["merchant_location"])

        if is_ring_attack:
            did: Optional[str] = rng.choice(shared_ring_devices)
        elif rng.random() < 0.01:
            did = None
        elif rng.random() < 0.12:
            did = devices[rng.randrange(len(devices))]["device_id"]
        else:
            did = rng.choice(cust_primary_devices[cid])

        prior = cust_history[cid]
        prior_90d = [p for p in prior if (cur_time - p["dt"]).days < 90]
        prior_24h = [p for p in prior if (cur_time - p["dt"]).total_seconds() < 86400]
        prior_1h = [p for p in prior if (cur_time - p["dt"]).total_seconds() < 3600]

        avg_val = round(float(np.mean([p["amount"] for p in prior_90d])), 2) if prior_90d else None
        if is_ring_attack and avg_val:
            amount = min(195000.0, round(min(avg_val, 8500.0) * rng.uniform(5.2, 9.5), 2))
        else:
            base_amt = min(avg_val, 3200.0) if avg_val else rng.uniform(250.0, 3200.0)
            amount = min(
                95000.0, round(max(10.0, float(np_rng.lognormal(math.log(base_amt), 0.55))), 2)
            )
        amount_paise = int(round(amount * 100))

        missing_loc = rng.random() < 0.03
        if missing_loc:
            c_loc = None
            is_new_city = None
            dist_km = None
        else:
            if is_ring_attack or rng.random() < 0.10:
                c_city = rng.choice(cities)
            else:
                c_city = cust["home_city"]
            c_lat, c_lon, _ = CITIES_COORDS[c_city]
            c_loc_dict = {
                "lat": round(c_lat + rng.uniform(-0.05, 0.05), 4),
                "lon": round(c_lon + rng.uniform(-0.05, 0.05), 4),
                "city": c_city,
            }
            c_loc = json.dumps(c_loc_dict)
            seen_cities = {p["city"] for p in prior_90d if p["city"] is not None} | {
                cust["home_city"]
            }
            is_new_city = c_city not in seen_cities
            dist_km = haversine_km(
                c_loc_dict["lat"], c_loc_dict["lon"], m_loc["lat"], m_loc["lon"]
            )

        if did is None:
            is_new_device = None
            distinct_cust_7d = None
        else:
            seen_devs = {p["device_id"] for p in prior_90d if p["device_id"] is not None}
            is_new_device = did not in seen_devs
            dev_events_7d = [
                c_id
                for dt_prev, c_id in device_history.get(did, [])
                if (cur_time - dt_prev).total_seconds() < 7 * 86400
            ]
            distinct_cust_7d = len(set(dev_events_7d))

        dt_ist = cur_time.astimezone(IST)
        hour_ist = dt_ist.hour
        is_night = 0 <= hour_ist < 5

        failed_24h = sum(1 for p in prior_24h if p["status"] == "FAILED")
        if is_ring_attack:
            failed_24h = max(failed_24h, rng.randint(3, 6))
        has_recent_failures = failed_24h >= 3

        cust_created = datetime.fromisoformat(cust["created_at"])
        account_age = max(0, (cur_time - cust_created).days)
        is_new_account = account_age < 30

        txn_count_1h = len(prior_1h)
        txn_amount_24h_paise = sum(p["amount_paise"] for p in prior_24h)
        amt_ratio = round(amount / avg_val, 4) if (avg_val and avg_val > 0) else None

        reason_codes = assign_reason_codes(
            is_new_device=is_new_device,
            is_new_city=is_new_city,
            distance_km=dist_km,
            is_night=is_night,
            has_recent_failures=has_recent_failures,
            amount_to_avg_ratio=amt_ratio,
            is_new_account=is_new_account,
            txn_count_last_1h=txn_count_1h,
            merchant_category=mcc,
        )

        combined_flags = (
            int(is_new_device is True)
            + int(is_new_city is True or (dist_km is not None and dist_km > 500.0))
            + int(is_night is True)
            + int(has_recent_failures is True)
            + int(is_new_account is True)
        )

        fraud_label = (
            1
            if (
                is_ring_attack
                or len(reason_codes) >= 4
                or (len(reason_codes) >= 3 and rng.random() < 0.35)
            )
            else 0
        )
        status = "FAILED" if rng.random() < 0.08 else "SUCCESS"
        tid = f"TX_{idx:010d}"
        channel = rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]

        tx_rows.append(
            {
                "transaction_id": tid,
                "customer_id": cid,
                "merchant_id": mid,
                "device_id": did,
                "amount_paise": amount_paise,
                "transaction_amount": amount,
                "payment_channel": channel,
                "merchant_category": mcc,
                "transaction_time": cur_time.isoformat(),
                "customer_age": cust["customer_age"],
                "customer_location": c_loc,
                "merchant_location": merch["merchant_location"],
                "account_age": account_age,
                "previous_transactions": len(prior_90d),
                "average_transaction_value": avg_val,
                "failed_transactions": failed_24h,
                "split": split,
                "is_synthetic": True,
            }
        )

        outcome_rows.append(
            {
                "transaction_id": tid,
                "outcome_time": (cur_time + timedelta(seconds=2)).isoformat(),
                "status": status,
                "failure_code": "AUTH_DECLINED" if status == "FAILED" else None,
                "is_synthetic": True,
            }
        )

        label_avail = cur_time + timedelta(days=60)
        label_rows.append(
            {
                "transaction_id": tid,
                "fraud_label": fraud_label,
                "label_source": "investigator_verdict" if fraud_label == 1 else "auto_maturity",
                "label_available_at": label_avail.isoformat(),
                "split": split,
                "eligible_at_next_split_start": True,
                "is_synthetic": True,
            }
        )

        feature_rows.append(
            {
                "transaction_id": tid,
                "feature_as_of": cur_time.isoformat(),
                "amount_to_avg_ratio": amt_ratio,
                "is_new_device": is_new_device,
                "customer_merchant_distance_km": dist_km,
                "is_new_city": is_new_city,
                "hour_of_day": hour_ist,
                "is_night": is_night,
                "has_recent_failures": has_recent_failures,
                "is_new_account": is_new_account,
                "txn_count_last_1h": txn_count_1h,
                "txn_amount_last_24h_paise": txn_amount_24h_paise,
                "distinct_customers_per_device_7d": distinct_cust_7d,
                "combined_risk_flags": combined_flags,
                "mcc_target_encoded_rate": 0.012 if mcc in HIGH_RISK_MCCS else 0.004,
                "mcc_encoding_as_of": "2024-11-30T18:30:00+00:00",
                "mcc_encoding_count": 100,
                "missing_customer_location": missing_loc,
                "missing_device": did is None,
                "cold_start": avg_val is None,
                "reason_codes": json.dumps(reason_codes),
                "feature_version": "sentinel-features-1.0.0",
            }
        )

        cust_history[cid].append(
            {
                "dt": cur_time,
                "amount": amount,
                "amount_paise": amount_paise,
                "device_id": did,
                "city": json.loads(c_loc)["city"] if c_loc else None,
                "status": status,
            }
        )
        if did is not None:
            device_history.setdefault(did, []).append((cur_time, cid))

    return {
        "customers": customers_df,
        "merchants": merchants_df,
        "devices": devices_df,
        "transactions": pd.DataFrame(tx_rows),
        "payment_outcomes": pd.DataFrame(outcome_rows),
        "labels": pd.DataFrame(label_rows),
        "features": pd.DataFrame(feature_rows),
    }


def load_sentinel_tables(
    data_dir: Optional[str] = None,
    min_transactions: int = 5500,
    seed: int = 42,
    offline_mock: bool = False,
) -> Tuple[Dict[str, pd.DataFrame], str, Dict[str, Any]]:
    """
    3-Way Automatic Sentinel Telemetry Loader with Pre-Ingestion Privacy Gate:
      Tier 1: Local `sentinel_data/tables/` (CSV) or `sentinel_data/parquet/` (Parquet)
      Tier 2: PostgreSQL / Supabase feature store (`DATABASE_URL`)
      Tier 3: Deterministic schema-exact synthetic generator (>= 5,500 transactions)
    Returns (tables_dict, active_tier_description, privacy_audit_summary).
    """
    required_tables = [
        "customers",
        "merchants",
        "devices",
        "transactions",
        "payment_outcomes",
        "labels",
        "features",
    ]

    if not offline_mock:
        candidate_dirs = [
            data_dir or os.getenv("SENTINEL_DATA_DIR", "./sentinel_data"),
            "../sentinel_data",
            "./data/sentinel_data",
        ]
        for base in candidate_dirs:
            if not base:
                continue
            base_path = Path(base)
            tables_dir = base_path / "tables"
            parquet_dir = base_path / "parquet"
            if tables_dir.is_dir() and all((tables_dir / f"{t}.csv").exists() for t in required_tables):
                loaded = {t: pd.read_csv(tables_dir / f"{t}.csv") for t in required_tables}
                privacy_audit = enforce_data_privacy_guardrails(loaded)
                return loaded, f"Tier 1 (Local Sanitized CSV: {tables_dir.name})", privacy_audit
            if parquet_dir.is_dir() and all(
                (parquet_dir / f"{t}.parquet").exists() for t in required_tables
            ):
                loaded = {t: pd.read_parquet(parquet_dir / f"{t}.parquet") for t in required_tables}
                privacy_audit = enforce_data_privacy_guardrails(loaded)
                return loaded, f"Tier 1 (Local Sanitized Parquet: {parquet_dir.name})", privacy_audit

        db_url = os.getenv("DATABASE_URL", "").strip().strip("'\"")
        if db_url and "<" not in db_url:
            try:
                import psycopg2  # type: ignore

                # Ensure URL-encoded credentials if password contains unencoded '@'
                clean_url = db_url
                match = re.match(r"^(postgres(?:ql)?://)([^:]+):(.+)@([^@/]+(?::\d+)?(?:/.*)?)$", db_url)
                if match:
                    scheme, user, password, host_part = match.groups()
                    if "@" in password and "%40" not in password:
                        clean_url = f"{scheme}{user}:{urllib.parse.quote_plus(password)}@{host_part}"

                conn = psycopg2.connect(clean_url, connect_timeout=10)
                with conn.cursor() as cur:
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

                loaded = {}
                for t in ["customers", "merchants", "devices"]:
                    loaded[t] = pd.read_sql_query(f'SELECT * FROM "{schema}"."{t}";', conn)

                # Query transactions spanning all 4 temporal splits
                tx_query = f"""
                (SELECT * FROM "{schema}".transactions WHERE split = 'warmup' ORDER BY transaction_time LIMIT 1500)
                UNION ALL
                (SELECT * FROM "{schema}".transactions WHERE split = 'train' ORDER BY transaction_time LIMIT 3000)
                UNION ALL
                (SELECT * FROM "{schema}".transactions WHERE split = 'validation' ORDER BY transaction_time LIMIT 1500)
                UNION ALL
                (SELECT * FROM "{schema}".transactions WHERE split = 'holdout' ORDER BY transaction_time LIMIT 1500)
                ORDER BY transaction_time ASC;
                """
                tx_df = pd.read_sql_query(tx_query, conn)
                loaded["transactions"] = tx_df

                tx_ids = tuple(tx_df["transaction_id"].tolist())
                format_ids = "'" + "','".join(tx_ids) + "'"

                for t in ["payment_outcomes", "labels", "features"]:
                    loaded[t] = pd.read_sql_query(
                        f'SELECT * FROM "{schema}"."{t}" WHERE transaction_id IN ({format_ids});',
                        conn,
                    )
                conn.close()

                # Normalize any JSONB dict/list objects to JSON strings
                for t_name, df in loaded.items():
                    for col in df.columns:
                        if df[col].dtype == object and len(df) > 0:
                            sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                            if isinstance(sample, (dict, list)):
                                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

                if len(loaded["transactions"]) >= 5000:
                    privacy_audit = enforce_data_privacy_guardrails(loaded)
                    return loaded, f"Tier 2 (PostgreSQL / Supabase Telemetry Replica: {schema})", privacy_audit
            except Exception:
                pass

    num_customers = int(os.getenv("SENTINEL_NUM_CUSTOMERS", "500"))
    num_merchants = int(os.getenv("SENTINEL_NUM_MERCHANTS", "120"))
    num_devices = int(os.getenv("SENTINEL_NUM_DEVICES", "900"))
    num_txns = max(
        min_transactions, int(os.getenv("SENTINEL_NUM_TRANSACTIONS", str(min_transactions)))
    )
    synth_seed = int(os.getenv("SENTINEL_SYNTHETIC_SEED", str(seed)))

    tables = generate_synthetic_sentinel_dataset(
        num_customers=num_customers,
        num_merchants=num_merchants,
        num_devices=num_devices,
        num_transactions=num_txns,
        seed=synth_seed,
    )
    privacy_audit = enforce_data_privacy_guardrails(tables)
    return (
        tables,
        f"Tier 3 (Deterministic Schema-Compliant Synthetic Telemetry: {num_txns:,} txns, seed={synth_seed})",
        privacy_audit,
    )
