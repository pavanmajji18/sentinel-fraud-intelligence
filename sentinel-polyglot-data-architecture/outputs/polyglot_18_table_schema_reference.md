# Project SENTINEL — Polyglot 18-Table Schema & Point-in-Time Architecture Reference

> **Architecture Specification**: Tier-1 RBI-Regulated Digital Payment Gateway (UPI / Cards / Wallets / Net-Banking)  
> **Schema Name**: `sentinel_demo`  
> **Governance Standard**: DPDP Act 2023 & PCI-DSS v4.0 Zero-PII Compliance  

---

## 1. Complete 18-Table Schema Specifications (`sentinel_demo`)

| # | Table Name | Domain | Exact Grain (1 Row Represents...) | Primary Key (PK) | Foreign Keys (FK) | Full Scale Rows | Sample Replica Rows | Key Columns |
| :-: | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **01** | `customers` | Core Entity | One registered payer account | `customer_id` (`C_<16-hex>`) | None | 25,000 | 2,500 | `customer_id`, `created_at`, `customer_age`, `home_city`, `region`, `is_synthetic` |
| **02** | `merchants` | Core Entity | One onboarded merchant terminal | `merchant_id` (`M_<16-hex>`) | None | 2,000 | 200 | `merchant_id`, `synthetic_name`, `merchant_category`, `merchant_location` (JSONB `{lat, lon, city}`), `is_synthetic` |
| **03** | `devices` | Core Entity | One unique hardware device fingerprint | `device_id` (`D_<16-hex>`) | None | ~100,000 | 10,120 | `device_id`, `device_kind` (`mobile_android`, `mobile_ios`, `desktop_web`), `is_synthetic` |
| **04** | `transactions` | Core Telemetry | One payment authorization attempt (including declines) | `transaction_id` (`TX_<10-digit>`) | `customer_id` -> `customers`, `merchant_id` -> `merchants`, `device_id` -> `devices` (nullable) | 1,100,000 (1M main + 100k warmup) | 100,000 | `amount_paise` (`bigint`), `transaction_amount` (`numeric(14,2)`), `payment_channel`, `merchant_category`, `transaction_time` (UTC), `customer_location` (nullable), `merchant_location`, `account_age`, `previous_transactions`, `average_transaction_value` (nullable), `failed_transactions`, `split`, `is_synthetic` |
| **05** | `payment_outcomes` | Core Telemetry | One terminal gateway authorization status (1:1 with `transactions`) | `transaction_id` | `transaction_id` -> `transactions` | 1,100,000 | 100,000 | `transaction_id`, `outcome_time`, `status` (`SUCCESS` / `FAILED`), `failure_code` (nullable), `is_synthetic` |
| **06** | `labels` | Core Telemetry | One ground-truth fraud verdict after 60-day maturation (1:1 with `transactions`) | `transaction_id` | `transaction_id` -> `transactions` | 1,100,000 (0.60% fraud = ~6,600 pos) | 100,000 (~600 pos) | `transaction_id`, `fraud_label` (`0` / `1`), `label_source` (`investigator_verdict`, `chargeback_feed`, `auto_maturity`), `label_available_at` (`transaction_time + 60 days`), `split`, `eligible_at_next_split_start`, `is_synthetic` |
| **07** | `features` | Core Telemetry | One point-in-time feature vector computed at `feature_as_of = transaction_time` (1:1 with `transactions`) | `transaction_id` | `transaction_id` -> `transactions` | 1,100,000 | 100,000 | `transaction_id`, `feature_as_of`, `amount_to_avg_ratio`, `is_new_device`, `customer_merchant_distance_km`, `is_new_city`, `hour_of_day`, `is_night`, `has_recent_failures`, `is_new_account`, `txn_count_last_1h`, `txn_amount_last_24h_paise`, `distinct_customers_per_device_7d`, `combined_risk_flags`, `mcc_target_encoded_rate`, `reason_codes` (JSONB), `feature_version` |
| **08** | `legacy_cases` | Unstructured & RAG | One historical closed fraud investigation docket | `legacy_case_id` | `customer_id` -> `customers` | 2,000 | 2 | `legacy_case_id`, `customer_id`, `transaction_time`, `closed_at`, `amount_paise`, `payment_channel`, `fraud_label`, `evidence`, `is_synthetic` |
| **09** | `documents` | Unstructured & RAG | One full policy, playbook, regulatory circular, or case narrative document | `document_id` | `transaction_id` -> `transactions` (nullable), `legacy_case_id` -> `legacy_cases` (nullable) | 39,006 | 5,256 | `document_id`, `kind` (`rbi_circular`, `npci_guideline`, `playbooks`, `case_narrative`), `title`, `split`, `content`, `metadata` (JSONB), `is_synthetic` |
| **10** | `document_chunks` | Unstructured & RAG | One overlapping text passage (180 words, 40-word overlap) for 3072-d vector indexing | `chunk_id` | `document_id` -> `documents` | ~120,000+ | Subset (~16,000) | `chunk_id`, `document_id`, `chunk_index`, `content`, `word_count`, `split`, `is_synthetic` |
| **11** | `gold_explanations` | Alignment & Audit | One verified reference JSON investigator explanation for Supervised Fine-Tuning (SFT) | `example_id` | `transaction_id` -> `transactions` | 8,000 | 8,500 | `example_id`, `transaction_id`, `split`, `prompt`, `target` (strict JSON), `fraud_label`, `is_synthetic` |
| **12** | `preference_pairs` | Alignment & Audit | One `(chosen, rejected)` explanation pair for Direct Preference Optimization (DPO) | `pair_id` | `transaction_id` -> `transactions` | 5,000 | 5,000 | `pair_id`, `transaction_id`, `split`, `prompt`, `chosen`, `rejected`, `rejection_reason` (`hallucinated_amount`, `wrong_policy_citation`, `leaky_future_fact`), `is_synthetic` |
| **13** | `decision_audit` | Alignment & Audit | One online scoring event & latency audit log record | `transaction_id` | `transaction_id` -> `transactions` | 1,000,000 | Subset | `transaction_id`, `scored_at`, `fraud_probability`, `decision` (`APPROVE`, `REVIEW`, `BLOCK`), `risk_level`, `reason_codes`, `model_version`, `latency_ms` |
| **14** | `threshold_sets` | Governance & Config | One probability cutoff configuration per MCC risk tier | `threshold_id` | None | Config rows | 1+ | `threshold_id`, `mcc_group`, `medium_from` (`0.30`), `high_from` (`0.60`), `critical_from` (`0.85`) |
| **15** | `hard_rules` | Governance & Config | One deterministic compliance/block override rule | `rule_id` | None | Config rows | 1+ | `rule_id`, `code`, `expression`, `action` (`BLOCK` / `REVIEW`), `priority`, `enabled` |
| **16** | `list_entries` | Governance & Config | One entity allowlist or blocklist entry (`C_*`, `M_*`, `D_*`) | `entry_id` | None (polymorphic `entity_id`) | 136 | 1+ | `entry_id`, `list_type` (`BLOCKLIST` / `ALLOWLIST`), `entity_id`, `effective_from`, `reason` |
| **17** | `config_changes` | Governance & Config | One immutable audit trail record of rule or threshold mutation | `change_id` | None (polymorphic `entity_id`) | Config rows | Subset | `change_id`, `entity`, `entity_id`, `before_value`, `after_value`, `changed_at` |
| **18** | `reviewer_feedback` | Governance & Config | One human investigator verdict & corrected explanation on a `REVIEW` transaction | `feedback_id` | `transaction_id` -> `transactions` | Fixtures | Subset | `feedback_id`, `transaction_id`, `reviewed_at`, `verdict` (`CONFIRMED_FRAUD` / `FALSE_POSITIVE`), `corrected_explanation` |

---

## 2. 60-Day `label_available_at` Maturation Window Diagram & Anti-Leakage Guardrails

In UPI and Card networks, fraud dispute resolution requires up to 60 days for bank chargeback notification cycles:

$$\text{label\_available\_at} = \text{transaction\_time} + 60\text{ days}$$

```text
Timeline & The 60-Day Chargeback Maturation Window (Validation Split Cutoff T_val = 2025-11-01 UTC)
====================================================================================================
2024-11-01          2025-01-01                     2025-09-02            2025-11-01          2026-01-01
    |-- WARMUP (61d) --|-------- MATURE TRAIN --------|-- IMMATURE ZONE --|-- VALIDATION --|-- HOLDOUT --|
                                                      ^                   ^
                                                      |                   |-- T_val (Model Fit Cutoff)
                                                      |-- T_val - 60 days (2025-09-02T00:00:00Z)
```

### The Look-Ahead Leakage Trap:
If a training pipeline filters naively on `WHERE split = 'train'`, transactions authorized between `2025-09-02` and `2025-10-31` have `label_available_at > 2025-11-01`. Including these unmatured labels leaks future chargebacks into model weights and merchant target encodings, falsely inflating offline PR-AUC by `+0.04` to `+0.07`.

### Production Leakage-Free Training Query:
```sql
SELECT t.*, l.fraud_label
FROM sentinel_demo.transactions t
JOIN sentinel_demo.labels l ON t.transaction_id = l.transaction_id
WHERE t.split = 'train'
  AND t.transaction_time < TIMESTAMPTZ '2025-11-01T00:00:00Z'
  AND l.label_available_at <= TIMESTAMPTZ '2025-11-01T00:00:00Z';
```

---

## 3. Worked Numerical Trace: `FE-01` to `FE-10` & `R01` to `R08`

### Test Benchmark Transaction: `TX_0000842910`
- **Timestamp (`t`)**: `2025-11-04T20:45:00Z` (UTC) -> `2025-11-05T02:15:00+05:30` (IST)
- **Entities**: `customer_id = C_8f91a2c4b0e3712d` (`home_city = Bengaluru`), `merchant_id = M_4d20e7b19a53c88f` (`electronics`, `Delhi`), `device_id = D_99c3e1047a8b21f0`
- **Amount**: `transaction_amount = Rs. 48,500.00` (`amount_paise = 4,850,000`)
- **Coordinates**: Bengaluru `(12.9716 N, 77.5946 E)` to Delhi `(28.6139 N, 77.2090 E)` -> Haversine distance `1,739.80 km`
- **Prior Point-in-Time History (`t_prior < t`)**:
  - 90-day customer average spend: `Rs. 5,000.00`
  - Trailing 24h: 5 attempts (`Rs. 21,500.00`), including **3 failed attempts**
  - Trailing 1h: **4 prior attempts**
  - Trailing 7d on `D_99c3e1047a8b21f0`: **4 distinct customer IDs**
  - Mature training stats for `electronics`: `144` frauds out of `11,900` transactions

### Exact Feature Derivations (`FE-01` to `FE-10`):

| Feature Code | Name | Point-in-Time Formula | Worked Value on `TX_0000842910` | Final Value |
| :--- | :--- | :--- | :--- | :--- |
| **`FE-01`** | `amount_to_avg_ratio` | `amount / avg_txn_value_90d` | `48500.00 / 5000.00` | **`9.7000`** |
| **`FE-02`** | `is_new_device` | `device_id` unseen for customer in 90d | `D_99c3e1047a8b21f0` not in `{D_11a0b2234c5d67e8}` | **`True`** |
| **`FE-03`** | `customer_merchant_distance_km` | Haversine distance (`R = 6371 km`) | Bengaluru to Delhi | **`1739.80 km`** (`is_new_city = False`) |
| **`FE-04`** | `hour_of_day`, `is_night` | Convert UTC to IST (`UTC+05:30`); `0 <= hour < 5` | `20:45 UTC + 05:30 = 02:15 IST` -> `hour = 2` | **`2`**, `is_night = ` **`True`** |
| **`FE-05`** | `failed_transactions`, `has_recent_failures` | Count `FAILED` in 24h; `>= 3` | `3 >= 3` | **`3`**, `has_recent_failures = ` **`True`** |
| **`FE-06`** | `account_age`, `is_new_account` | `floor((t - created_at) / 86400s)`; `< 30d` | `2025-11-04 - 2025-10-20 = 15 days` | **`15`**, `is_new_account = ` **`True`** |
| **`FE-07`** | `txn_count_last_1h`, `txn_amount_last_24h_paise` | Prior 1h count; prior 24h paise sum | `4` txns; `2,150,000` paise | **`4`**, **`2,150,000` paise** |
| **`FE-08`** | `mcc_target_encoded_rate` | `(fraud_mature + 0.6) / (total_mature + 100)` | `(144 + 0.6) / (11900 + 100) = 144.6 / 12000` | **`0.01205`** (`1.205%`) |
| **`FE-09`** | `distinct_customers_per_device_7d` | Distinct customers on device in 7d | `4` prior distinct customers | **`4`** |
| **`FE-10`** | `combined_risk_flags` | Sum of 5 boolean flags | `1 + 1 + 1 + 1 + 1` | **`5`** |

### Deterministic Reason Codes (`R01` to `R08`):

| Reason Code | Trigger Invariant | Evaluation on `TX_0000842910` | Status |
| :--- | :--- | :--- | :---: |
| **`R01_NEW_DEVICE`** | `is_new_device == True` | `is_new_device = True` | **FIRED** |
| **`R02_GEO_MISMATCH`** | `is_new_city == True` OR `distance_km > 500.0` | `1739.80 km > 500.0 km` | **FIRED** |
| **`R03_ODD_HOUR`** | `is_night == True` (`00:00` to `04:59` IST) | `02:15 IST` (`hour = 2`) | **FIRED** |
| **`R04_FAILED_ATTEMPTS`** | `has_recent_failures == True` (`failed_txns >= 3`) | `3 >= 3` | **FIRED** |
| **`R05_AMOUNT_SPIKE`** | `amount_to_avg_ratio >= 5.0` | `9.7000 >= 5.0` | **FIRED** |
| **`R06_NEW_ACCOUNT`** | `is_new_account == True` (`account_age < 30` days) | `15 < 30` | **FIRED** |
| **`R07_VELOCITY`** | `txn_count_last_1h >= 4` | `4 >= 4` | **FIRED** |
| **`R08_HIGH_RISK_MCC`** | `category in {electronics, gaming, travel, gift_cards, crypto}` | `"electronics"` in set | **FIRED** |

