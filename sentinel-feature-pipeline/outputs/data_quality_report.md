# Project SENTINEL — Full Data Quality, Schema & Temporal Split Audit Report

* **Active Telemetry Source**: `Tier 2 (PostgreSQL / Supabase Telemetry Replica: public)`
* **Governance Standard**: `DPDP Act 2023 & PCI-DSS v4.0 Data Minimization`
* **Tokenized Entities Verified**: `2,500` customers (`C_*`), `200` merchants (`M_*`), `10,120` devices (`D_*`)
* **Synthetic Guardrail (`is_synthetic == True`)**: `100.00%` verified across all tables

## 1. Relational Table Inventory & Primary Key Uniqueness

| Table Name | Primary Key | Row Count | Columns | PK Nulls | PK Duplicates | Full-Row Duplicates | `is_synthetic` (%) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `customers` | `customer_id` | 2,500 | 6 | 0 | 0 | 0 | 100.00% |
| `merchants` | `merchant_id` | 200 | 5 | 0 | 0 | 0 | 100.00% |
| `devices` | `device_id` | 10,120 | 3 | 0 | 0 | 0 | 100.00% |
| `transactions` | `transaction_id` | 7,500 | 18 | 0 | 0 | 0 | 100.00% |
| `payment_outcomes` | `transaction_id` | 7,500 | 5 | 0 | 0 | 0 | 100.00% |
| `labels` | `transaction_id` | 7,500 | 7 | 0 | 0 | 0 | 100.00% |
| `features` | `transaction_id` | 7,500 | 22 | 0 | 0 | 0 | 100.00% |

## 2. Complete Column-Level Missingness & Null Profile Across All 7 Tables

| Table Name | Column Name | Data Type | Null Count | Null Rate (%) | Non-Null Count | Root Cause / Governance Handling |
| :--- | :--- | :--- | ---: | ---: | ---: | :--- |
| `customers` | `customer_id` | `str` | 0 | 0.00% | 2,500 | Zero nulls observed (mandatory field) |
| `customers` | `created_at` | `datetime64[us, UTC]` | 0 | 0.00% | 2,500 | Zero nulls observed (mandatory field) |
| `customers` | `customer_age` | `int64` | 0 | 0.00% | 2,500 | Zero nulls observed (mandatory field) |
| `customers` | `home_city` | `str` | 0 | 0.00% | 2,500 | Zero nulls observed (mandatory field) |
| `customers` | `region` | `str` | 0 | 0.00% | 2,500 | Zero nulls observed (mandatory field) |
| `customers` | `is_synthetic` | `bool` | 0 | 0.00% | 2,500 | Zero nulls observed (mandatory field) |
| `merchants` | `merchant_id` | `str` | 0 | 0.00% | 200 | Zero nulls observed (mandatory field) |
| `merchants` | `synthetic_name` | `str` | 0 | 0.00% | 200 | Zero nulls observed (mandatory field) |
| `merchants` | `merchant_category` | `str` | 0 | 0.00% | 200 | Zero nulls observed (mandatory field) |
| `merchants` | `merchant_location` | `str` | 0 | 0.00% | 200 | Zero nulls observed (mandatory field) |
| `merchants` | `is_synthetic` | `bool` | 0 | 0.00% | 200 | Zero nulls observed (mandatory field) |
| `devices` | `device_id` | `str` | 0 | 0.00% | 10,120 | Web/UPI collect flows where hardware fingerprint is uncaptured (~1% missing) |
| `devices` | `device_kind` | `str` | 0 | 0.00% | 10,120 | Zero nulls observed (mandatory field) |
| `devices` | `is_synthetic` | `bool` | 0 | 0.00% | 10,120 | Zero nulls observed (mandatory field) |
| `transactions` | `transaction_id` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `customer_id` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `merchant_id` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `device_id` | `str` | 81 | 1.08% | 7,419 | Web/UPI collect flows where hardware fingerprint is uncaptured (~1% missing) |
| `transactions` | `amount_paise` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `transaction_amount` | `float64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `payment_channel` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `merchant_category` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `transaction_time` | `datetime64[us, UTC]` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `customer_age` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `customer_location` | `str` | 231 | 3.08% | 7,269 | GPS/IP geolocation permission denied at authorization (~3% missing) |
| `transactions` | `merchant_location` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `account_age` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `previous_transactions` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `average_transaction_value` | `float64` | 790 | 10.53% | 6,710 | Cold-start customers with 0 prior transactions in 90d window (~10.5% missing) |
| `transactions` | `failed_transactions` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `split` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `transactions` | `is_synthetic` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `payment_outcomes` | `transaction_id` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `payment_outcomes` | `outcome_time` | `datetime64[us, UTC]` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `payment_outcomes` | `status` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `payment_outcomes` | `failure_code` | `str` | 6,727 | 89.69% | 773 | Populated strictly when payment_outcomes.status == 'FAILED' (null on successful authorizations) |
| `payment_outcomes` | `is_synthetic` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `labels` | `transaction_id` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `labels` | `fraud_label` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `labels` | `label_source` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `labels` | `label_available_at` | `datetime64[us, UTC]` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `labels` | `split` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `labels` | `eligible_at_next_split_start` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `labels` | `is_synthetic` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `transaction_id` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `feature_as_of` | `datetime64[us, UTC]` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `amount_to_avg_ratio` | `float64` | 790 | 10.53% | 6,710 | Inherits null from cold-start average_transaction_value (zero-division guard) |
| `features` | `is_new_device` | `object` | 81 | 1.08% | 7,419 | Inherits null when transactions.device_id is null |
| `features` | `customer_merchant_distance_km` | `float64` | 231 | 3.08% | 7,269 | Inherits null when transactions.customer_location is null |
| `features` | `is_new_city` | `object` | 231 | 3.08% | 7,269 | Inherits null when transactions.customer_location is null |
| `features` | `hour_of_day` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `is_night` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `has_recent_failures` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `is_new_account` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `txn_count_last_1h` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `txn_amount_last_24h_paise` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `distinct_customers_per_device_7d` | `float64` | 81 | 1.08% | 7,419 | Inherits null when transactions.device_id is null |
| `features` | `combined_risk_flags` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `mcc_target_encoded_rate` | `float64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `mcc_encoding_as_of` | `datetime64[us, UTC]` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `mcc_encoding_count` | `int64` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `missing_customer_location` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `missing_device` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `cold_start` | `bool` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `reason_codes` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |
| `features` | `feature_version` | `str` | 0 | 0.00% | 7,500 | Zero nulls observed (mandatory field) |

## 3. Duplicates & Referential Integrity Across All 7 Tables

* **Primary Key Nulls Across All Tables**: `0` (`0.00%`)
* **Primary Key Duplicates Across All Tables**: `0` (`0.00%`)
* **Full-Row Duplicate Records Across All Tables**: `0` (`0.00%`)
* **Foreign Key Orphans (`transactions -> customers`)**: `0`
* **Foreign Key Orphans (`transactions -> merchants`)**: `0`
* **Foreign Key Orphans (`transactions -> devices` non-null)**: `0`
* **1:1 Alignment (`transactions <-> payment_outcomes`)**: `0` missing rows
* **1:1 Alignment (`transactions <-> labels`)**: `0` missing rows
* **1:1 Alignment (`transactions <-> features`)**: `0` missing rows
* **Overall Referential Integrity Verdict**: `PASS (100% Valid)`

## 4. Wrong Types, Schema Checks & Invariant Verifications

* **Invalid JSON Coordinates Count (`customer_location`, `merchant_location`)**: `0` (100% valid lat/lon floats)
* **Unparseable Timestamps Across All Tables**: `0` (100% ISO 8601 UTC parseable)
* **Non-Positive Transaction Amounts (`transaction_amount <= 0`)**: `0`
* **Integer Paise Invariant (`amount_paise == round(transaction_amount * 100)`)**: `0` mismatches (`100.00%` exact parity)
* **60-Day Chargeback Maturation Invariant (`label_available_at == transaction_time + 60 days`)**: `0` anomalies (`100.00%` compliant)
* **All Schema Checks Status**: `PASS (Zero Schema Defects)`

## 5. Outliers in Amount & Monetary Distribution Analysis

### Transaction Amount Quantiles (INR)
* **Min**: Rs. 8.43
* **25th Percentile (p25)**: Rs. 589.10
* **Median (p50)**: Rs. 1,225.92
* **Mean**: Rs. 3,264.47 (Std: Rs. 47,573.46)
* **75th Percentile (p75)**: Rs. 2,672.24 (IQR: Rs. 2,083.14)
* **90th Percentile (p90)**: Rs. 5,643.56
* **95th Percentile (p95)**: Rs. 9,125.54
* **99th Percentile (p99)**: Rs. 23,819.16
* **Max**: Rs. 4,060,489.61

### Outlier Thresholds & Explicit Anomaly Counts
1. **IQR Mild Outliers (`Q3 + 1.5 * IQR`)**: Threshold = **Rs. 5,796.95** | Count = **723** (9.64%)
2. **IQR Extreme Outliers (`Q3 + 3.0 * IQR`)**: Threshold = **Rs. 8,921.66** | Count = **394** (5.25%)
3. **Z-Score Outliers (`|z| > 3.0`)**: Threshold = **Rs. 145,984.85** | Count = **4** (0.05%)
4. **P99 Tail Outliers (`> 99th Percentile`)**: Threshold = **Rs. 23,819.16** | Count = **75** (1.00%)

### Top Outlier Transaction Records

| Transaction ID | Amount (INR) | Split | Payment Channel | Merchant Category | IQR 1.5 Outlier | IQR 3.0 Outlier | Z-Score Outlier | P99 Outlier |
| :--- | ---: | :--- | :--- | :--- | :---: | :---: | :---: | :---: |
| `TX_0000000484` | Rs. 4,060,489.61 | `warmup` | `UPI` | `other` | Yes | Yes | Yes | Yes |
| `TX_0000082909` | Rs. 514,908.01 | `validation` | `card` | `gift_cards` | Yes | Yes | Yes | Yes |
| `TX_0000083264` | Rs. 159,576.48 | `validation` | `UPI` | `gift_cards` | Yes | Yes | Yes | Yes |
| `TX_0000096540` | Rs. 156,739.72 | `holdout` | `UPI` | `gaming` | Yes | Yes | Yes | Yes |
| `TX_0000082627` | Rs. 121,439.40 | `validation` | `UPI` | `gift_cards` | Yes | Yes | No | Yes |
| `TX_0000012201` | Rs. 89,525.55 | `train` | `card` | `gaming` | Yes | Yes | No | Yes |
| `TX_0000000392` | Rs. 81,644.17 | `warmup` | `UPI` | `gift_cards` | Yes | Yes | No | Yes |
| `TX_0000000350` | Rs. 81,190.74 | `warmup` | `UPI` | `gaming` | Yes | Yes | No | Yes |
| `TX_0000010006` | Rs. 76,224.74 | `train` | `UPI` | `gift_cards` | Yes | Yes | No | Yes |
| `TX_0000011525` | Rs. 75,198.38 | `train` | `wallet` | `gaming` | Yes | Yes | No | Yes |

## 6. Row Count Per Split & Temporal Breakdown (`warmup`, `train`, `validation`, `holdout`)

| Split | Rows | Share (%) | Start Time (IST) | End Time (IST) | Customers | Fraud Txns | Fraud Rate (%) | Failed Txns (%) | Missing Device (%) | Missing Loc (%) | Cold Start (%) | Labels Mature by Split End (%) |
| :--- | ---: | ---: | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `warmup` | 1,500 | 20.00% | `2024-10-01T00:05:15.602000+05:30` | `2024-10-14T21:08:31.382000+05:30` | 681 | 6 | 0.40% | 11.67% | 2.00% | 2.47% | 45.40% | 100.00% |
| `train` | 3,000 | 40.00% | `2025-01-01T00:37:56.971000+05:30` | `2025-01-13T02:31:15.373000+05:30` | 1,181 | 18 | 0.60% | 10.07% | 1.47% | 3.37% | 2.87% | 100.00% |
| `validation` | 1,500 | 20.00% | `2025-11-01T00:14:47.012000+05:30` | `2025-11-07T09:13:30.262000+05:30` | 744 | 6 | 0.40% | 9.33% | 0.00% | 3.47% | 1.07% | 17.87% |
| `holdout` | 1,500 | 20.00% | `2026-01-01T01:06:56.017000+05:30` | `2026-01-07T11:03:03.836000+05:30` | 727 | 7 | 0.47% | 10.40% | 0.47% | 2.73% | 0.47% | 100.00% |

## 7. Payment Channel & Merchant Category (MCC) Risk Distribution

| Payment Channel | Transactions | Share (%) | Fraud Count | Fraud Rate (%) | Mean Amount (INR) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `UPI` | 4,323 | 57.64% | 23 | 0.53% | Rs. 3,730.67 |
| `card` | 2,454 | 32.72% | 12 | 0.49% | Rs. 2,706.29 |
| `wallet` | 461 | 6.15% | 2 | 0.43% | Rs. 2,482.79 |
| `net_banking` | 262 | 3.49% | 0 | 0.00% | Rs. 2,175.53 |

| Merchant Category (MCC) | High-Risk MCC (`R08`) | Transactions | Share (%) | Fraud Count | Fraud Rate (%) | Mean Amount (INR) |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: |
| `gaming` | Yes | 745 | 9.93% | 15 | 2.01% | Rs. 3,998.16 |
| `gift_cards` | Yes | 554 | 7.39% | 10 | 1.81% | Rs. 4,913.07 |
| `electronics` | Yes | 918 | 12.24% | 12 | 1.31% | Rs. 2,882.23 |
| `education` | No | 379 | 5.05% | 0 | 0.00% | Rs. 2,611.33 |
| `food` | No | 1,008 | 13.44% | 0 | 0.00% | Rs. 2,058.83 |
| `grocery` | No | 1,433 | 19.11% | 0 | 0.00% | Rs. 2,217.01 |
| `healthcare` | No | 494 | 6.59% | 0 | 0.00% | Rs. 2,543.62 |
| `other` | No | 407 | 5.43% | 0 | 0.00% | Rs. 12,025.22 |
| `quick_commerce` | No | 1,098 | 14.64% | 0 | 0.00% | Rs. 2,450.80 |
| `travel` | No | 464 | 6.19% | 0 | 0.00% | Rs. 2,270.22 |
