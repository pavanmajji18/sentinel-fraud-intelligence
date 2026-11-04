# Project SENTINEL — Full Data Quality, Schema & Temporal Split Audit Report

* **Active Telemetry Source**: `Tier 3 (Deterministic Schema-Compliant Synthetic Telemetry: 5,500 txns, seed=42)`
* **Governance Standard**: `DPDP Act 2023 & PCI-DSS v4.0 Data Minimization`
* **Tokenized Entities Verified**: `500` customers (`C_*`), `120` merchants (`M_*`), `900` devices (`D_*`)
* **Synthetic Guardrail (`is_synthetic == True`)**: `100.00%` verified across all tables

## 1. Relational Table Inventory & Primary Key Uniqueness

| Table Name | Primary Key | Row Count | Columns | PK Nulls | PK Duplicates | Full-Row Duplicates | `is_synthetic` (%) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `customers` | `customer_id` | 500 | 6 | 0 | 0 | 0 | 100.00% |
| `merchants` | `merchant_id` | 120 | 5 | 0 | 0 | 0 | 100.00% |
| `devices` | `device_id` | 900 | 3 | 0 | 0 | 0 | 100.00% |
| `transactions` | `transaction_id` | 5,500 | 18 | 0 | 0 | 0 | 100.00% |
| `payment_outcomes` | `transaction_id` | 5,500 | 5 | 0 | 0 | 0 | 100.00% |
| `labels` | `transaction_id` | 5,500 | 7 | 0 | 0 | 0 | 100.00% |
| `features` | `transaction_id` | 5,500 | 22 | 0 | 0 | 0 | 100.00% |

## 2. Temporal Split Distribution & 60-Day Label Maturation Audit (`warmup`, `train`, `validation`, `holdout`)

| Split | Rows | Share (%) | Start Time (IST) | End Time (IST) | Customers | Fraud Txns | Fraud Rate (%) | Failed Txns (%) | Missing Device (%) | Missing Loc (%) | Cold Start (%) | Labels Mature by Split End (%) |
| :--- | ---: | ---: | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `warmup` | 653 | 11.87% | `2024-11-01T09:57:03+05:30` | `2025-01-01T05:05:02+05:30` | 374 | 105 | 16.08% | 9.19% | 1.07% | 3.98% | 57.27% | 1.38% |
| `train` | 3,344 | 60.80% | `2025-01-01T07:21:05+05:30` | `2025-11-01T04:16:52+05:30` | 499 | 455 | 13.61% | 7.21% | 1.08% | 3.26% | 15.01% | 80.23% |
| `validation` | 672 | 12.22% | `2025-11-01T06:15:06+05:30` | `2026-01-01T03:27:58+05:30` | 362 | 91 | 13.54% | 8.48% | 1.04% | 2.08% | 13.54% | 2.08% |
| `holdout` | 831 | 15.11% | `2026-01-01T07:21:22+05:30` | `2026-02-22T23:20:39+05:30` | 397 | 115 | 13.84% | 8.66% | 1.56% | 3.01% | 11.79% | 100.00% |

## 3. Column-Level Missingness & Null Inheritance Profile

| Table Name | Column Name | Data Type | Null Count | Null Rate (%) | Non-Null Count | Root Cause / Governance Handling |
| :--- | :--- | :--- | ---: | ---: | ---: | :--- |
| `devices` | `device_id` | `str` | 0 | 0.00% | 900 | Web/UPI collect flows where hardware fingerprint is uncaptured (~1% missing) |
| `transactions` | `device_id` | `str` | 63 | 1.15% | 5,437 | Web/UPI collect flows where hardware fingerprint is uncaptured (~1% missing) |
| `transactions` | `customer_location` | `str` | 174 | 3.16% | 5,326 | GPS/IP geolocation permission denied at authorization (~3% missing) |
| `transactions` | `average_transaction_value` | `float64` | 1,065 | 19.36% | 4,435 | Cold-start customers with 0 prior transactions in 90d window |
| `payment_outcomes` | `failure_code` | `str` | 5,070 | 92.18% | 430 | Populated strictly when payment_outcomes.status == 'FAILED' (~92% SUCCESS) |
| `features` | `amount_to_avg_ratio` | `float64` | 1,065 | 19.36% | 4,435 | Inherits null from cold-start average_transaction_value (zero division guard) |
| `features` | `is_new_device` | `object` | 63 | 1.15% | 5,437 | Inherits null when transactions.device_id is null |
| `features` | `customer_merchant_distance_km` | `float64` | 174 | 3.16% | 5,326 | Inherits null when transactions.customer_location is null |
| `features` | `is_new_city` | `object` | 174 | 3.16% | 5,326 | Inherits null when transactions.customer_location is null |
| `features` | `distinct_customers_per_device_7d` | `float64` | 63 | 1.15% | 5,437 | Inherits null when transactions.device_id is null |

## 4. Referential Integrity & Monetary Paise Invariant Verification

* **Foreign Key Orphans (`transactions -> customers / merchants / devices`)**: `0` / `0` / `0`
* **1:1 Entity Alignment (`transactions <-> outcomes / labels / features`)**: `0` missing rows across all `5,500` transactions
* **Integer Paise Invariant (`amount_paise == round(transaction_amount * 100)`)**: `0` mismatches (`100.00%` exact integer parity)
* **60-Day Chargeback Maturation Invariant (`label_available_at == transaction_time + 60 days`)**: `0` anomalies (`100.00%` compliant)
* **Transaction Amount Quantiles (INR)**: Min `Rs. 19.76` | p25 `Rs. 789.57` | Median `Rs. 1,682.88` | Mean `Rs. 4,222.63` | p75 `Rs. 3,051.15` | p95 `Rs. 10,981.28` | p99 `Rs. 69,041.15` | Max `Rs. 80,725.28`

## 5. Payment Channel & Merchant Category (MCC) Risk Distribution

| Payment Channel | Transactions | Share (%) | Fraud Count | Fraud Rate (%) | Mean Amount (INR) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `UPI` | 3,147 | 57.22% | 443 | 14.08% | Rs. 4,332.96 |
| `card` | 1,762 | 32.04% | 244 | 13.85% | Rs. 4,070.02 |
| `wallet` | 389 | 7.07% | 52 | 13.37% | Rs. 4,164.97 |
| `net_banking` | 202 | 3.67% | 27 | 13.37% | Rs. 3,946.07 |

| Merchant Category (MCC) | High-Risk MCC (`R08`) | Transactions | Share (%) | Fraud Count | Fraud Rate (%) | Mean Amount (INR) |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: |
| `travel` | Yes | 799 | 14.53% | 225 | 28.16% | Rs. 4,641.06 |
| `gaming` | Yes | 281 | 5.11% | 77 | 27.40% | Rs. 4,354.77 |
| `electronics` | Yes | 531 | 9.65% | 139 | 26.18% | Rs. 3,988.72 |
| `utilities` | No | 982 | 17.85% | 88 | 8.96% | Rs. 3,781.93 |
| `quick_commerce` | No | 1,011 | 18.38% | 89 | 8.80% | Rs. 4,467.61 |
| `grocery` | No | 512 | 9.31% | 45 | 8.79% | Rs. 4,219.83 |
| `food` | No | 897 | 16.31% | 72 | 8.03% | Rs. 4,416.95 |
| `education` | No | 487 | 8.85% | 31 | 6.37% | Rs. 3,740.05 |
