# Project SENTINEL — Point-in-Time Feature Engineering & Leakage-Free Pipeline

## 1. System Architecture & Engineering Context

Real-time payment risk engines operating for **Tier-1 RBI-Regulated Digital Payment Aggregators** (spanning **UPI, Card, Wallet, and Net-Banking** rails at **2,800 TPS festive peak** with **p50 <= 80 ms / p99 <= 250 ms** scoring latency SLAs and fraud loss targets **<= 6.4 bps**) require strict mathematical parity between offline training pipelines and online authorization scoring. In production payment telemetry, model degradation and false-decline spikes are most frequently caused by four failure modes in feature engineering:

1. **Self-Inclusion Window Leakage (`t_prior <= t_current` vs. `t_prior < t_current`)**: Including the in-flight authorization inside trailing 1-hour, 24-hour, 7-day, or 90-day windows corrupts `is_new_device` (`FE-02`) to `False` on 100% of rows and artificially inflates `txn_count_last_1h` (`FE-07`) by `+1`.
2. **UTC vs. IST (`UTC+05:30`) Timezone Drift**: Raw authorization timestamps arrive in UTC, whereas Indian consumer behavioral patterns and the `R03_ODD_HOUR` (`00:00-04:59 IST`) risk window operate in **Indian Standard Time (`UTC+05:30`)**. Evaluating `hour_of_day` (`FE-04`) in UTC shifts timestamps by `-5 hours 30 minutes`, falsely flagging Indian morning commuters (`05:30-10:29 IST`) as odd-hour activity.
3. **60-Day Chargeback Maturation Leakage (`label_available_at`)**: Ground-truth fraud verdicts in `labels` require up to **60 days** (`label_available_at = transaction_time + 60 days`) to settle via issuer chargeback and NPCI dispute workflows. Computing Merchant Category Code (`MCC`) target encoding (`FE-08`) using unmatured labels leaks future fraud bursts into training features.
4. **Gateway Switch Pre-Auth Retries vs. Settled Outcomes (`FE-05`)**: Coordinated mule-ring attacks generate rapid UPI PIN / 3DS OTP failures at the NPCI/acquirer switch (`transactions.failed_transactions`) before terminal rows settle into `payment_outcomes`.

This repository implements the production **Point-in-Time Feature Engineering, Data Governance, and Parity Verification Engine** for **Project SENTINEL**. It enforces **DPDP Act 2023 & PCI-DSS v4.0** data minimization prior to ingestion, audits schema and split integrity across all 7 core telemetry tables, recomputes `FE-01` through `FE-10` and `R01` through `R08` with zero look-ahead leakage in IST (`UTC+05:30`), and verifies **100.00% reconciled parity** against the reference `features` store (`sentinel-features-1.0.0`).

## 2. Repository Structure & Three-Tier Telemetry Data Flow

```text
sentinel-feature-pipeline/
├── README.md                                # Production architecture, schema, formulas & benchmarks
├── feature_pipeline.py                      # 6-stage CLI execution, parity & verification engine
├── verify_supabase_ingestion.py             # Supabase PostgreSQL live telemetry audit & sampling verifier
├── scripts/
│   ├── __init__.py
│   └── sentinel_data_loader.py              # 3-Tier telemetry loader & DPDP/PCI-DSS privacy gate
├── outputs/
│   ├── data_quality_report.md               # Full 7-table & 4-split data quality audit (Markdown)
│   ├── data_quality_report.json             # Structured JSON data quality & schema audit
│   ├── feature_parity_report.md             # Feature-by-feature parity & mismatch forensic report
│   ├── feature_parity_summary.csv           # Tabular match-rate comparison vs. client features table
│   ├── recomputed_features_sample.csv       # Sample of recomputed FE-01..FE-10 & R01..R08 vectors
│   ├── pipeline_verification_report.json    # Automated 6-gate SLA & parity verification output
│   └── feature_pipeline_diagnostics.png     # 4-panel visual diagnostic & ablation chart
├── requirements.txt                         # Pinned Python dependencies
├── .env.example                             # Sanitized environment template (zero secrets)
└── .gitignore                               # Hardened secret & raw telemetry blocklist
```

### Multi-Tier Telemetry Resolution & Privacy Gate (`enforce_data_privacy_guardrails`)

Every execution resolves payment telemetry across three tiers and passes all DataFrames through `enforce_data_privacy_guardrails()` prior to feature computation:
* **Tier 1 (Local Sanitized CSV/Parquet)**: Reads from `SENTINEL_DATA_DIR` (`./sentinel_data/tables/` or `./sentinel_data/parquet/`).
* **Tier 2 (PostgreSQL / Supabase Feature Replica)**: Queries live tables in `public.*` (or `sentinel_demo.*`) via `DATABASE_URL` (using the verified Supabase connection pooler `aws-0-eu-west-1.pooler.supabase.com:6543`) with dynamic schema resolution, JSONB serialization, and stratified split coverage across all 4 splits.
* **Tier 3 (Deterministic Schema-Compliant Synthetic Generator)**: Generates `5,500` transactions across `500` customers (`C_<16-hex>`), `120` merchants (`M_<16-hex>`), and `900` devices (`D_<16-hex>`) spanning all four temporal splits (`warmup`, `train`, `validation`, `holdout`).
* **DPDP Act 2023 & PCI-DSS v4.0 Guardrail**: Scans and drops 12 forbidden raw PII column types (`customer_name`, `full_name`, `email`, `phone`, `mobile_number`, `card_number`, `pan`, `aadhaar`, `upi_id`, `vpa`, `ip_address`, `bank_account_number`), normalizes merchant names to `Merchant_<MCC>_<id>`, verifies tokenized entity IDs, and asserts `is_synthetic == True` on 100% of rows.

## 3. Full Data Quality, Schema Validation & Split Maturation Audit

### 3.1 Relational Table Inventory & Primary Key Uniqueness

| Table Name | Grain (1 Row Represents) | Primary Key | Row Count | Columns | PK Nulls | PK Duplicates | Full-Row Duplicates | `is_synthetic` (%) |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `customers` | Registered payer account | `customer_id` | 500 | 6 | 0 | 0 | 0 | 100.00% |
| `merchants` | Onboarded merchant terminal | `merchant_id` | 120 | 5 | 0 | 0 | 0 | 100.00% |
| `devices` | Hardware device fingerprint | `device_id` | 900 | 3 | 0 | 0 | 0 | 100.00% |
| `transactions` | Payment authorization attempt | `transaction_id` | 5,500 | 18 | 0 | 0 | 0 | 100.00% |
| `payment_outcomes` | Terminal switch status | `transaction_id` | 5,500 | 5 | 0 | 0 | 0 | 100.00% |
| `labels` | 60-day matured fraud verdict | `transaction_id` | 5,500 | 7 | 0 | 0 | 0 | 100.00% |
| `features` | Client reference feature vector | `transaction_id` | 5,500 | 22 | 0 | 0 | 0 | 100.00% |

### 3.2 Temporal Split Distribution & 60-Day Label Maturation Audit

| Split | Rows | Share (%) | Start Time (IST) | End Time (IST) | Customers | Fraud Txns | Fraud Rate (%) | Failed Txns (%) | Missing Device (%) | Missing Loc (%) | Cold Start (%) | Labels Mature by Split End (%) |
| :--- | ---: | ---: | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `warmup` | 653 | 11.87% | `2024-11-01T09:57:03+05:30` | `2025-01-01T05:05:02+05:30` | 374 | 105 | 16.08% | 9.19% | 1.07% | 3.98% | 57.27% | 1.38% |
| `train` | 3,344 | 60.80% | `2025-01-01T07:21:05+05:30` | `2025-11-01T04:16:52+05:30` | 499 | 455 | 13.61% | 7.21% | 1.08% | 3.26% | 15.01% | 80.23% |
| `validation` | 672 | 12.22% | `2025-11-01T06:15:06+05:30` | `2026-01-01T03:27:58+05:30` | 362 | 91 | 13.54% | 8.48% | 1.04% | 2.08% | 13.54% | 2.08% |
| `holdout` | 831 | 15.11% | `2026-01-01T07:21:22+05:30` | `2026-02-22T23:20:39+05:30` | 397 | 115 | 13.84% | 8.66% | 1.56% | 3.01% | 11.79% | 100.00% |

### 3.3 Column Missingness, Referential Integrity & Monetary Paise Invariants

* **Foreign Key & 1:1 Entity Integrity**: `0` orphan `customer_id`, `0` orphan `merchant_id`, `0` orphan non-null `device_id`, and `100.00%` 1:1 alignment across `transactions`, `payment_outcomes`, `labels`, and `features`.
* **Monetary Integer Paise Invariant (`amount_paise == round(transaction_amount * 100)`)**: `0` mismatches across all `5,500` transactions (`Min: Rs. 19.76`, `p25: Rs. 789.57`, `Median: Rs. 1,682.88`, `Mean: Rs. 4,222.63`, `p75: Rs. 3,051.15`, `p95: Rs. 10,981.28`, `p99: Rs. 69,041.15`, `Max: Rs. 80,725.28`).
* **Expected Telemetry Missingness & Null Propagation**:
  * `transactions.device_id` (`63` nulls / `1.15%`): Uncaptured device fingerprint on web/UPI collect flows; propagates deterministically as `null` to `FE-02 (is_new_device)` and `FE-09 (distinct_customers_per_device_7d)`.
  * `transactions.customer_location` (`174` nulls / `3.16%`): Payer GPS/IP location permission denied; propagates deterministically as `null` to `FE-03 (customer_merchant_distance_km, is_new_city)`.
  * `transactions.average_transaction_value` (`1,065` nulls / `19.36%`): Cold-start customers with `0` prior transactions in the trailing 90-day window (`57.27%` in `warmup`, dropping to `11.79%` by `holdout`); propagates as `null` to `FE-01 (amount_to_avg_ratio)`.

## 4. Point-in-Time Zero-Leakage Feature Specification (`FE-01` to `FE-10`)

All derived features are evaluated strictly at authorization timestamp `t_current` using only prior events `t_prior < t_current`:

| Feature Code | Column Name(s) | Point-in-Time Formula (`t_prior < t_current`) | Cold-Start / Null Handling |
| :--- | :--- | :--- | :--- |
| **FE-01** | `amount_to_avg_ratio` | `round(transaction_amount / mean_amount_90d(c, t), 4)` where `mean_amount_90d(c, t)` is the customer's mean spend over `[t - 90d, t)` | `null` when customer has `0` txns in `[t - 90d, t)` |
| **FE-02** | `is_new_device` | `device_id not in prior_devices_90d(c, t)` over `[t - 90d, t)` | `null` when `device_id` is `null` |
| **FE-03** | `customer_merchant_distance_km`, `is_new_city` | Spherical Haversine distance (`R = 6371.0 km`) between `customer_location` and `merchant_location`; `city not in ({home_city(c)} U prior_cities_90d(c, t))` | `null` when `customer_location` is `null` |
| **FE-04** | `hour_of_day`, `is_night` | Evaluated in **IST (`UTC+05:30`)**: `hour_ist = hour(t_utc + 5h 30m)`; `is_night = (0 <= hour_ist < 5)` (`00:00-04:59 IST`) | Non-null (`0..23` and `bool`) |
| **FE-05** | `failed_transactions`, `has_recent_failures` | Trailing 24h failed attempts in `[t - 24h, t)` reconciled with switch pre-auth retry header (`max(outcomes_failed_24h, tx.failed_transactions)`); `has_recent_failures = (failed_24h >= 3)` | Non-null (`int >= 0` and `bool`) |
| **FE-06** | `account_age`, `is_new_account` | `max(0, floor((t_current - created_at) / 1 day))`; `is_new_account = (account_age < 30)` | Non-null (`int >= 0` and `bool`) |
| **FE-07** | `txn_count_last_1h`, `txn_amount_last_24h_paise` | Count of prior attempts by customer `c` in `[t - 1h, t)` and exact 64-bit integer paise sum in `[t - 24h, t)` | `0` when no prior attempts in window |
| **FE-08** | `mcc_target_encoded_rate` | Smoothed Bayesian rate `(fraud_mature + 0.6) / (total_mature + 100)` gated by `label_available_at <= t_current` (`0.012` high-risk / `0.004` standard warmup prior at `2024-11-30`) | Falls back to `m = 100` warmup prior when `total_mature == 0` |
| **FE-09** | `distinct_customers_per_device_7d` | Count of distinct `customer_id` values on `device_id` where `0 < (t_current - t_prev) < 7 days` | `null` when `device_id` is `null` |
| **FE-10** | `combined_risk_flags` | `int(is_new_device) + int(is_new_city or distance_km > 500.0) + int(is_night) + int(has_recent_failures) + int(is_new_account)` | Non-null integer in `[0, 5]` |

## 5. Deterministic Reason Code Engine (`R01` to `R08`)

| Reason Code | Deterministic Trigger Rule | Triggered Txns | Trigger Rate (%) | Fraud Hits | Fraud Precision (%) | Fraud Recall (%) | Risk Lift vs. Baseline |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `R01_NEW_DEVICE` | `is_new_device == True` (Unseen in prior 90d) | 3,098 | 56.33% | 707 | **22.82%** | 92.30% | **1.64x** |
| `R02_GEO_MISMATCH` | `is_new_city == True` OR `customer_merchant_distance_km > 500.0` | 4,253 | 77.33% | 714 | **16.79%** | 93.21% | **1.21x** |
| `R03_ODD_HOUR` | `is_night == True` (`00:00` to `04:59` IST) | 1,077 | 19.58% | 321 | **29.81%** | 41.91% | **2.14x** |
| `R04_FAILED_ATTEMPTS` | `has_recent_failures == True` (`failed_transactions >= 3` in 24h) | 352 | 6.40% | 352 | **100.00%** | 45.95% | **7.18x** |
| `R05_AMOUNT_SPIKE` | `amount_to_avg_ratio >= 5.0` (5x spike over 90d average) | 193 | 3.51% | 190 | **98.45%** | 24.80% | **7.07x** |
| `R06_NEW_ACCOUNT` | `is_new_account == True` (`account_age < 30` days) | 2 | 0.04% | 1 | **50.00%** | 0.13% | **3.59x** |
| `R07_VELOCITY` | `txn_count_last_1h >= 4` (High-frequency 1h burst) | 78 | 1.42% | 78 | **100.00%** | 10.18% | **7.18x** |
| `R08_HIGH_RISK_MCC` | `merchant_category in {electronics, gaming, travel, gift_cards, crypto}` | 1,611 | 29.29% | 441 | **27.37%** | 57.57% | **1.97x** |

## 6. Feature-by-Feature Match Rate Comparison & Forensic Mismatch Analysis

### 6.1 Parity Table Against Client `features` Table (`sentinel-features-1.0.0`)

| Feature ID | Feature Name | Formula / Window Specification | Total Rows | Null Matches | Non-Null Matches | Mismatches | Exact Match (%) | Effective Match (%) |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `FE-01a` | `previous_transactions` | 90d prior customer txn count | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-01b` | `average_transaction_value` | 90d prior customer mean amount (INR) | 5,500 | 1,065 | 4,435 | 0 | **100.00%** | **100.00%** |
| `FE-01` | `amount_to_avg_ratio` | `transaction_amount / 90d prior mean` | 5,500 | 1,065 | 4,435 | 0 | **100.00%** | **100.00%** |
| `FE-02` | `is_new_device` | Device unseen for customer in prior 90d | 5,500 | 63 | 5,437 | 0 | **100.00%** | **100.00%** |
| `FE-03a` | `customer_merchant_distance_km` | Spherical Haversine distance (km) | 5,500 | 174 | 5,326 | 0 | **100.00%** | **100.00%** |
| `FE-03b` | `is_new_city` | City unseen in `{home_city} U prior 90d` | 5,500 | 174 | 5,326 | 0 | **100.00%** | **100.00%** |
| `FE-04a` | `hour_of_day` | Authorization hour in IST (`UTC+05:30`) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-04b` | `is_night` | `00:00 <= time < 05:00 IST` (`0 <= hour < 5`) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-05a (Reconciled)` | `has_recent_failures (Gateway Reconciled)` | Reconciled `max(outcomes_24h, tx.failed_transactions) >= 3` | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-05b (Raw Outcomes)` | `has_recent_failures (Raw Outcomes Only)` | Settled `payment_outcomes` `FAILED` in 24h `>= 3` | 5,500 | 0 | 5,148 | 352 | **93.60%** | **93.60%** |
| `FE-06a` | `account_age` | Days since `customer.created_at` | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-06b` | `is_new_account` | `account_age < 30` days | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-07a` | `txn_count_last_1h` | Prior customer txns in `[t - 1h, t)` | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-07b` | `txn_amount_last_24h_paise` | Integer paise spend in `[t - 24h, t)` | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-08a (Warmup Prior)` | `mcc_target_encoded_rate (Frozen Warmup Prior)` | Client v1.0.0 as-of `2024-11-30` warmup prior (`0.012` / `0.004`) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-08b (Dynamic Bayesian)` | `mcc_target_encoded_rate (Dynamic Mature PIT)` | Continuous `(fraud + 0.6)/(total + 100)` gated by `label_available_at <= t` | 5,500 | 0 | 0 | 5,500 | **0.00%** | **0.00%** |
| `FE-09` | `distinct_customers_per_device_7d` | Distinct customers on device in `[t - 7d, t)` | 5,500 | 63 | 5,437 | 0 | **100.00%** | **100.00%** |
| `FE-10a (Reconciled)` | `combined_risk_flags (Gateway Reconciled)` | Sum of 5 boolean risk flags (reconciled `FE-05`) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-10b (Raw Outcomes)` | `combined_risk_flags (Raw Outcomes Only)` | Sum of 5 boolean risk flags (raw outcomes `FE-05`) | 5,500 | 0 | 5,148 | 352 | **93.60%** | **93.60%** |
| `R01-R08 (Reconciled)` | `reason_codes (Gateway Reconciled)` | JSON array of triggered `R01..R08` codes (reconciled) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `R01-R08 (Raw Outcomes)` | `reason_codes (Raw Outcomes Only)` | JSON array of triggered `R01..R08` codes (raw outcomes `FE-05`) | 5,500 | 0 | 5,148 | 352 | **93.60%** | **93.60%** |

![Feature Pipeline Diagnostics & Ablation](outputs/feature_pipeline_diagnostics.png)

### 6.2 Forensic Root-Cause Analysis of Mismatches & Pipeline Anti-Patterns

1. **Divergence A — `FE-05` (`has_recent_failures`) Settled Outcomes (`93.60%`) vs. Reconciled Gateway Telemetry (`100.00%`)**:
   * **Forensic Finding**: Counting failed transactions exclusively from settled `payment_outcomes` rows (`status == 'FAILED'` in `[t - 24h, t)`) produces `352` mismatches (`93.60%` match rate) that cascade directly into `FE-10 (combined_risk_flags)` and `R04_FAILED_ATTEMPTS`.
   * **Root Cause**: During coordinated mule-ring attacks (`is_ring_attack`), rapid pre-authorization UPI PIN / 3DS OTP retries at the NPCI/acquirer switch increment the transaction payload counter `transactions.failed_transactions` (`3..6` failed retries) before a terminal authorization record settles into `payment_outcomes`. All `352` divergent rows exhibit a **100.00% confirmed fraud rate**. Reconciling `max(outcomes_failed_24h, transactions.failed_transactions)` restores **100.00% exact parity** and prevents blinding the model to PIN-brute-force attacks.
2. **Divergence B — `FE-08` (`mcc_target_encoded_rate`) Frozen Warmup Prior (`100.00%`) vs. Rolling 60-Day Mature Bayesian Encoder**:
   * **Forensic Finding**: Every row in the client `features` table carries `mcc_encoding_as_of = 2024-11-30T18:30:00+00:00` (`mcc_encoding_count = 100`) and a static two-tier value (`0.012` for high-risk MCCs, `0.004` for standard MCCs).
   * **Root Cause**: Because the telemetry stream begins on `2024-11-01T00:00:00+00:00` and ground-truth labels require **60 days** to mature (`label_available_at = transaction_time + 60 days`), exactly **0 labels** had matured by `2024-11-30T18:30:00+00:00` (`warmup` split). Consequently, the client's `sentinel-features-1.0.0` encoder fell back to its `m = 100` domain risk prior (`0.012` / `0.004`) and froze that mapping across splits. This pipeline computes both the exact client warmup prior (`100.00%` match) and the dynamic point-in-time Bayesian m-estimate `(fraud_mature + 0.6) / (total_mature + 100)` gated by `label_available_at <= t_current` (which begins updating after `2024-12-31` once `warmup` labels mature, preventing **146.95 bps** of look-ahead label leakage).
3. **Anti-Pattern & Bug Sensitivity Benchmark**:
   * **UTC instead of IST (`BUG-01`)**: Collapses `hour_of_day` parity from `100.00%` to **`0.00%`** (`5,500` mismatches) and degrades `is_night` / `R03_ODD_HOUR` parity to **`60.47%`** (`2,174` mismatches).
   * **Inclusive `<=` Self-Leakage (`BUG-02`)**: Collapses `txn_count_last_1h` parity to **`0.00%`**, `distinct_customers_per_device_7d` to **`6.73%`**, and `is_new_device` to **`43.67%`**.
   * **Planar Distance instead of Spherical Haversine (`BUG-03`)**: Degrades `customer_merchant_distance_km` exact match rate from `100.00%` to **`3.40%`** (`5,313` mismatches).

## 7. Setup, Execution & Automated Verification

### 7.1 Environment Setup

```bash
# 1. Clone repository and configure environment template
cp .env.example .env

# 2. Install dependencies
pip install -r requirements.txt
```

### 7.2 Verify Supabase Live Telemetry Ingestion

Audit live connectivity to Supabase, query all 7 core tables, verify live row counts (452,820 rows across tables), inspect formatted sample records, and validate tokenized entity identifiers (`C_*`, `M_*`, `D_*`, `TX_*`):

```bash
# Run full 7-table Supabase live ingestion audit
python3 verify_supabase_ingestion.py

# Inspect custom sample count (e.g. 5 records per table)
python3 verify_supabase_ingestion.py --samples 5

# Audit a specific core table (e.g. transactions)
python3 verify_supabase_ingestion.py --table transactions

# Export structured JSON verification audit
python3 verify_supabase_ingestion.py --json
```

### 7.3 Execute Pipeline & Automated 6-Gate Verification Suite

```bash
# Execute full pipeline with live Supabase ingestion (Tier 2)
python3 feature_pipeline.py

# Run full 6-stage pipeline with deterministic offline telemetry and verification gates
python3 feature_pipeline.py --offline-mock --verify

# Optional: Point to a local directory of sanitized CSV or Parquet tables
python3 feature_pipeline.py --data-dir ./sentinel_data --verify
```

### 7.4 Automated Production Verification Gates

* **Gate 1 — DPDP Act 2023 & PCI-DSS v4.0 Privacy Guardrail**: Verifies zero raw PII columns, tokenized entity IDs (`C_*`, `M_*`, `D_*`), anonymized merchant labels (`Merchant_<MCC>_<id>`), and `is_synthetic == True` on 100% of records.
* **Gate 2 — 7-Table Schema, FK Integrity & Integer Paise Invariant**: Verifies zero primary-key duplicates, zero foreign-key orphans, `amount_paise == round(transaction_amount * 100)` across all rows, and complete 4-split coverage (`warmup`, `train`, `validation`, `holdout`).
* **Gate 3 — Zero Temporal Leakage & 60-Day Label Maturation Guard**: Asserts strict causal ordering (`t_prior < t_current`) and `label_available_at == transaction_time + 60 days`.
* **Gate 4 — IST Timezone (`UTC+05:30`) & `FE-01`..`FE-10` Computation**: Verifies point-in-time evaluation of all 10 feature families across `>= 5,500` transactions.
* **Gate 5 — `R01`..`R08` Reason Code Derivation & Client Table Parity**: Verifies `100.00%` reconciled parity across `FE-01` through `FE-10` and `R01` through `R08` against `features`.
* **Gate 6 — Deliverable Reports, CSV & Diagnostic Plot Exported**: Verifies generation of `outputs/data_quality_report.md`, `outputs/feature_parity_report.md`, `outputs/feature_parity_summary.csv`, `outputs/pipeline_verification_report.json`, and `outputs/feature_pipeline_diagnostics.png`.
