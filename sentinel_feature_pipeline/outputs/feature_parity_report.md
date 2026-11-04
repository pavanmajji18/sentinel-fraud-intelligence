# Project SENTINEL — Point-in-Time Feature Parity & Forensic Mismatch Analysis

## 1. Feature-by-Feature Match Rate Comparison vs. Client `features` Table

| Feature ID | Feature Name | Formula / Window Specification | Total Rows | Null Matches | Non-Null Matches | Mismatches | Exact Match (%) | Effective Match (%) |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `FE-01a` | `previous_transactions` | 90d prior customer txn count | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-01b` | `average_transaction_value` | 90d prior customer mean amount (INR) | 5,500 | 1,065 | 4,435 | 0 | **100.00%** | **100.00%** |
| `FE-01` | `amount_to_avg_ratio` | transaction_amount / 90d prior mean | 5,500 | 1,065 | 4,435 | 0 | **100.00%** | **100.00%** |
| `FE-02` | `is_new_device` | Device unseen for customer in prior 90d | 5,500 | 63 | 5,437 | 0 | **100.00%** | **100.00%** |
| `FE-03a` | `customer_merchant_distance_km` | Spherical Haversine distance (km) | 5,500 | 174 | 5,326 | 0 | **100.00%** | **100.00%** |
| `FE-03b` | `is_new_city` | City unseen in {home_city} U prior 90d | 5,500 | 174 | 5,326 | 0 | **100.00%** | **100.00%** |
| `FE-04a` | `hour_of_day` | Authorization hour in IST (UTC+05:30) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-04b` | `is_night` | 00:00 <= time < 05:00 IST (0 <= hour < 5) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-05a (Reconciled)` | `has_recent_failures (Gateway Reconciled)` | Reconciled max(outcomes_24h, tx.failed_transactions) >= 3 | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-05b (Raw Outcomes)` | `has_recent_failures (Raw Outcomes Only)` | Settled payment_outcomes FAILED in 24h >= 3 (excludes switch pre-auth retries) | 5,500 | 0 | 5,148 | 352 | **93.60%** | **93.60%** |
| `FE-06a` | `account_age` | Days since customer.created_at | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-06b` | `is_new_account` | account_age < 30 days | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-07a` | `txn_count_last_1h` | Prior customer txns in [t - 1h, t) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-07b` | `txn_amount_last_24h_paise` | Integer paise spend in [t - 24h, t) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-08a (Warmup Prior)` | `mcc_target_encoded_rate (Frozen Warmup Prior)` | Client v1.0.0 as-of 2024-11-30 warmup prior (0.012 high-risk / 0.004 base) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-08b (Dynamic Bayesian)` | `mcc_target_encoded_rate (Dynamic Mature PIT)` | Continuous (fraud + 0.6)/(total + 100) gated by label_available_at <= t | 5,500 | 0 | 0 | 5,500 | **0.00%** | **0.00%** |
| `FE-09` | `distinct_customers_per_device_7d` | Distinct customers on device in [t - 7d, t) | 5,500 | 63 | 5,437 | 0 | **100.00%** | **100.00%** |
| `FE-10a (Reconciled)` | `combined_risk_flags (Gateway Reconciled)` | Sum of 5 boolean risk flags (reconciled FE-05) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `FE-10b (Raw Outcomes)` | `combined_risk_flags (Raw Outcomes Only)` | Sum of 5 boolean risk flags (raw outcomes FE-05) | 5,500 | 0 | 5,148 | 352 | **93.60%** | **93.60%** |
| `R01-R08 (Reconciled)` | `reason_codes (Gateway Reconciled)` | JSON array of triggered R01..R08 codes (reconciled) | 5,500 | 0 | 5,500 | 0 | **100.00%** | **100.00%** |
| `R01-R08 (Raw Outcomes)` | `reason_codes (Raw Outcomes Only)` | JSON array of triggered R01..R08 codes (raw outcomes FE-05) | 5,500 | 0 | 5,148 | 352 | **93.60%** | **93.60%** |

## 2. Split-by-Split Parity Breakdown (`warmup`, `train`, `validation`, `holdout`)

| Split | Rows | `FE-01` Ratio (%) | `FE-04` IST Hour (%) | `FE-05` Reconciled (%) | `FE-05` Raw Outcomes (%) | `FE-08` Warmup Prior (%) | `FE-08` Dynamic (+/-50 bps) | `R01-R08` Reconciled (%) | `R01-R08` Raw Outcomes (%) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `warmup` | 653 | 100.00% | 100.00% | 100.00% | 94.18% | 100.00% | 0.00% | 100.00% | 94.18% |
| `train` | 3,344 | 100.00% | 100.00% | 100.00% | 93.54% | 100.00% | 0.00% | 100.00% | 93.54% |
| `validation` | 672 | 100.00% | 100.00% | 100.00% | 93.45% | 100.00% | 0.00% | 100.00% | 93.45% |
| `holdout` | 831 | 100.00% | 100.00% | 100.00% | 93.50% | 100.00% | 0.00% | 100.00% | 93.50% |

## 3. Forensic Root-Cause Analysis of Raw Event-Log vs. Client Table Divergences

### Divergence A: `FE-05` (`has_recent_failures`) — Settled Outcomes vs. Gateway Switch Telemetry (`93.60%` vs. `100.00%`)
* **Observation**: Computing 24-hour failed transactions solely by joining settled `payment_outcomes` (`status == 'FAILED'` in `[t - 24h, t)`) yields a **93.60%** match rate (`352` divergent rows out of `5,500`), whereas reconciling with the gateway header counter `transactions.failed_transactions` achieves **100.00%** parity across `has_recent_failures`, `combined_risk_flags` (`FE-10`), and `R04_FAILED_ATTEMPTS`.
* **Root Cause**: During coordinated mule-ring attacks (`is_ring_attack`), rapid pre-authorization UPI PIN / OTP retries at the NPCI/acquirer switch increment `transactions.failed_transactions` (>= 3) before terminal authorization rows settle into `payment_outcomes`. Reconciling `max(outcomes_failed_24h, transactions.failed_transactions)` restores 100.00% parity on `has_recent_failures`, `combined_risk_flags`, and `R04_FAILED_ATTEMPTS`.
* **Fraud Impact**: The `352` divergent transactions exhibit a **100.00%** fraud rate (coordinated ring attacks), confirming that dropping gateway switch pre-auth retry counts would blind the model to active credential-stuffing / PIN-brute-force bursts.

### Divergence B: `FE-08` (`mcc_target_encoded_rate`) — Frozen `2024-11-30` Warmup Prior vs. Continuous 60-Day Mature Bayesian Smoothing
* **Observation**: The client reference table (`sentinel-features-1.0.0`) records `mcc_encoding_as_of = 2024-11-30T18:30:00+00:00` (`mcc_encoding_count = 100`) and assigns a two-tier rate (`0.012` for high-risk MCCs, `0.004` for standard MCCs) across all splits (`100.00%` match against our warmup prior column).
* **Root Cause**: Client table `sentinel-features-1.0.0` froze `mcc_target_encoded_rate` at `mcc_encoding_as_of = 2024-11-30T18:30:00+00:00` (30 days after `2024-11-01` start). Because chargeback labels require 60 days to mature (`label_available_at = transaction_time + 60d`), exactly 0 labels were mature at `2024-11-30`, so the client encoder emitted its m=100 prior (`0.012` for high-risk MCCs, `0.004` for standard MCCs). Dynamic point-in-time Bayesian smoothing `(fraud_count + 0.6) / (total_count + 100)` begins updating only after `2024-12-31` when warmup labels mature, preventing 60-day look-ahead label leakage.
* **Leakage Guardrail**: Naive target encoding that ignores `label_available_at <= t_current` shifts MCC fraud rates by an average of **146.95 bps**, leaking future chargeback outcomes across `train`, `validation`, and `holdout`.

## 4. Pipeline Anti-Pattern & Bug Sensitivity Benchmark

| Pipeline Anti-Pattern / Bug | Affected Feature | Buggy Match Rate (%) | Mismatched Rows | Corrected PIT Match (%) | Root Cause & Production Impact |
| :--- | :--- | ---: | ---: | ---: | :--- |
| BUG-01: UTC Timezone Instead of IST (UTC+05:30) | FE-04a (`hour_of_day`) | **0.00%** | 5,500 | **100.00%** | Evaluating `cur_time.hour` in UTC shifts every timestamp by -5h 30m, causing 100% hour mismatch. |
| BUG-01b: UTC Night Window Instead of IST Night Window | FE-04b (`is_night` / `R03_ODD_HOUR`) | **60.47%** | 2,174 | **100.00%** | 00:00-04:59 UTC corresponds to 05:30-10:29 IST (morning peak in India), corrupting R03_ODD_HOUR. |
| BUG-02a: Inclusive Window `t_prior <= t_current` (Self-Inclusion) | FE-02 (`is_new_device`) | **43.67%** | 3,098 | **100.00%** | Including current transaction in 90d history makes `did in seen_devs` always True (0% new devices). |
| BUG-02b: Inclusive Window `t_prior <= t_current` (Self-Inclusion) | FE-07a (`txn_count_last_1h`) | **0.00%** | 5,500 | **100.00%** | Including current transaction inflates 1h velocity count by +1 on 100% of rows. |
| BUG-02c: Inclusive Window `t_prior <= t_current` (Self-Inclusion) | FE-09 (`distinct_customers_per_device_7d`) | **6.73%** | 5,130 | **100.00%** | Including current customer inflates 7d distinct device users by +1 whenever customer is new on device. |
| BUG-03: Planar Euclidean Distance Instead of Spherical Haversine | FE-03a (`customer_merchant_distance_km`) | **3.40%** | 5,313 | **100.00%** | Ignoring `cos(lat)` longitude scaling across Indian latitudes (12N-28N) distorts inter-city distances. |

## 5. Reason Code (`R01`-`R08`) Trigger Frequency, Fraud Precision & Risk Lift

| Reason Code | Deterministic Trigger Rule | Triggered Txns | Trigger Rate (%) | Fraud Hits | Fraud Precision (%) | Fraud Recall (%) | Lift vs. Baseline |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `R01_NEW_DEVICE` | `is_new_device == True (Device unseen for customer in prior 90d)` | 3,098 | 56.33% | 707 | **22.82%** | 92.30% | **1.64x** |
| `R02_GEO_MISMATCH` | `is_new_city == True OR customer_merchant_distance_km > 500.0 km` | 4,253 | 77.33% | 714 | **16.79%** | 93.21% | **1.21x** |
| `R03_ODD_HOUR` | `is_night == True (Transaction between 00:00 and 04:59 IST)` | 1,077 | 19.58% | 321 | **29.81%** | 41.91% | **2.14x** |
| `R04_FAILED_ATTEMPTS` | `has_recent_failures == True (failed_transactions >= 3 in prior 24h)` | 352 | 6.40% | 352 | **100.00%** | 45.95% | **7.18x** |
| `R05_AMOUNT_SPIKE` | `amount_to_avg_ratio >= 5.0 (5x spike over customer 90d average)` | 193 | 3.51% | 190 | **98.45%** | 24.80% | **7.07x** |
| `R06_NEW_ACCOUNT` | `is_new_account == True (account_age < 30 days at authorization)` | 2 | 0.04% | 1 | **50.00%** | 0.13% | **3.59x** |
| `R07_VELOCITY` | `txn_count_last_1h >= 4 (High-frequency burst in prior 60 minutes)` | 78 | 1.42% | 78 | **100.00%** | 10.18% | **7.18x** |
| `R08_HIGH_RISK_MCC` | `merchant_category in {electronics, gaming, travel, gift_cards, crypto}` | 1,611 | 29.29% | 441 | **27.37%** | 57.57% | **1.97x** |
