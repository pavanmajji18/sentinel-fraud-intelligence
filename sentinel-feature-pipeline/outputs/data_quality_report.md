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

## 2. Temporal Split Distribution & 60-Day Label Maturation Audit (`warmup`, `train`, `validation`, `holdout`)

| Split | Rows | Share (%) | Start Time (IST) | End Time (IST) | Customers | Fraud Txns | Fraud Rate (%) | Failed Txns (%) | Missing Device (%) | Missing Loc (%) | Cold Start (%) | Labels Mature by Split End (%) |
| :--- | ---: | ---: | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `warmup` | 1,500 | 20.00% | `2024-10-01T00:05:15.602000+05:30` | `2024-10-14T21:08:31.382000+05:30` | 681 | 6 | 0.40% | 11.67% | 2.00% | 2.47% | 45.40% | 100.00% |
| `train` | 3,000 | 40.00% | `2025-01-01T00:37:56.971000+05:30` | `2025-01-13T02:31:15.373000+05:30` | 1,181 | 18 | 0.60% | 10.07% | 1.47% | 3.37% | 2.87% | 100.00% |
| `validation` | 1,500 | 20.00% | `2025-11-01T00:14:47.012000+05:30` | `2025-11-07T09:13:30.262000+05:30` | 744 | 6 | 0.40% | 9.33% | 0.00% | 3.47% | 1.07% | 17.87% |
| `holdout` | 1,500 | 20.00% | `2026-01-01T01:06:56.017000+05:30` | `2026-01-07T11:03:03.836000+05:30` | 727 | 7 | 0.47% | 10.40% | 0.47% | 2.73% | 0.47% | 100.00% |

## 3. Column-Level Missingness & Null Inheritance Profile

| Table Name | Column Name | Data Type | Null Count | Null Rate (%) | Non-Null Count | Root Cause / Governance Handling |
| :--- | :--- | :--- | ---: | ---: | ---: | :--- |
| `devices` | `device_id` | `str` | 0 | 0.00% | 10,120 | Web/UPI collect flows where hardware fingerprint is uncaptured (~1% missing) |
| `transactions` | `device_id` | `str` | 81 | 1.08% | 7,419 | Web/UPI collect flows where hardware fingerprint is uncaptured (~1% missing) |
| `transactions` | `customer_location` | `str` | 231 | 3.08% | 7,269 | GPS/IP geolocation permission denied at authorization (~3% missing) |
| `transactions` | `average_transaction_value` | `float64` | 790 | 10.53% | 6,710 | Cold-start customers with 0 prior transactions in 90d window |
| `payment_outcomes` | `failure_code` | `str` | 6,727 | 89.69% | 773 | Populated strictly when payment_outcomes.status == 'FAILED' (~92% SUCCESS) |
| `features` | `amount_to_avg_ratio` | `float64` | 790 | 10.53% | 6,710 | Inherits null from cold-start average_transaction_value (zero division guard) |
| `features` | `is_new_device` | `object` | 81 | 1.08% | 7,419 | Inherits null when transactions.device_id is null |
| `features` | `customer_merchant_distance_km` | `float64` | 231 | 3.08% | 7,269 | Inherits null when transactions.customer_location is null |
| `features` | `is_new_city` | `object` | 231 | 3.08% | 7,269 | Inherits null when transactions.customer_location is null |
| `features` | `distinct_customers_per_device_7d` | `float64` | 81 | 1.08% | 7,419 | Inherits null when transactions.device_id is null |

## 4. Referential Integrity & Monetary Paise Invariant Verification

* **Foreign Key Orphans (`transactions -> customers / merchants / devices`)**: `0` / `0` / `0`
* **1:1 Entity Alignment (`transactions <-> outcomes / labels / features`)**: `0` missing rows across all `7,500` transactions
* **Integer Paise Invariant (`amount_paise == round(transaction_amount * 100)`)**: `0` mismatches (`100.00%` exact integer parity)
* **60-Day Chargeback Maturation Invariant (`label_available_at == transaction_time + 60 days`)**: `0` anomalies (`100.00%` compliant)
* **Transaction Amount Quantiles (INR)**: Min `Rs. 8.43` | p25 `Rs. 589.10` | Median `Rs. 1,225.92` | Mean `Rs. 3,264.47` | p75 `Rs. 2,672.24` | p95 `Rs. 9,125.54` | p99 `Rs. 23,819.16` | Max `Rs. 4,060,489.61`

## 5. Payment Channel & Merchant Category (MCC) Risk Distribution

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
| `travel` | Yes | 464 | 6.19% | 0 | 0.00% | Rs. 2,270.22 |
