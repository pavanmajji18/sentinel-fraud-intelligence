# Project SENTINEL — High-Throughput NoSQL Fraud Telemetry & BSON Indexing Engine

## 1. System Architecture & Engineering Context

Real-time payment authorization pipelines for a **Tier-1 RBI-Regulated Digital Payment Aggregator** (spanning **UPI, Cards, Wallets, and Net-Banking** across 1,80,000+ merchants and ~90M unique payers) must ingest and score payment attempts at **4.2 million daily authorizations** (**1,000 TPS sustained, 2,800 TPS festive peak**) while maintaining an online scoring latency SLA of **`p50 <= 80 ms` and `p99 <= 250 ms`** (with a fail-open/review circuit breaker at `300 ms`).

Static rule engines suffer from **9.8 bps fraud loss** and a **2.3% false decline/review rate**, generating 22,000+ manual investigator reviews per day. To power real-time velocity checks, point-in-time feature hydration (`FE-01` through `FE-10`), and deterministic risk signal attribution (`R01` through `R08`), the operational document store (`sentinel_fraud_db` in MongoDB) solves three core distributed systems challenges:

1. **Sub-Millisecond Profile & Device Fingerprinting Reads (`O(1)` Atomic Document Lookup)**: Online authorization scoring requires immediate access to a customer's known hardware fingerprints (`mobile_android`, `mobile_ios`, `desktop_web`) to evaluate `FE-02 (is_new_device)` and `R01_NEW_DEVICE` without cross-collection join (`lookup`) network overhead.
2. **Unbounded High-Velocity Stream Ingestion Without 16 MB BSON Overflow or Write Amplification**: At 2,800 TPS peak, transaction streams grow unboundedly. Embedding transactions inside customer documents would breach MongoDB's 16 MB BSON limit and trigger severe WiredTiger page re-allocation contention.
3. **Equality-Sort-Range (ESR) B-Tree Indexing for Point-in-Time Velocity Queries**: Fetching a customer's trailing 1-hour (`FE-07: txn_count_last_1h`) and 24-hour (`txn_amount_last_24h_paise`) authorization history requires an index that satisfies both the equality predicate (`customer_id`) and reverse chronological ordering (`transaction_time: -1`) without a blocking in-memory `SORT` stage.

## 2. Repository Structure

```text
sentinel-nosql-telemetry/
├── sentinel_nosql_telemetry.py       # End-to-end MongoDB BSON ingestion, CRUD, ESR index benchmark & aggregations
├── verify_sync_counts.py             # Supabase (PostgreSQL) <-> MongoDB Atlas cross-database count & sync audit
├── scripts/
│   ├── __init__.py
│   └── sentinel_data_loader.py       # 3-Way telemetry loader + DPDP Act 2023 & PCI-DSS v4.0 privacy gate
├── outputs/
│   ├── verification_report.json      # Complete telemetry, CRUD, index explain(), aggregation & 6-gate audit
│   ├── explain_before_after.json     # Raw WiredTiger queryPlanner & executionStats before/after ESR indexing
│   ├── aggregation_results.json      # Full outputs of Pipelines 1, 2A, 2B, and 3 (R01-R08 precision)
│   ├── sample_bson_documents.json    # Sample Customer (embedded devices) & Transaction (referenced) BSON docs
│   └── verification_log.txt          # Full Stage 1-6 console execution transcript
├── docker-compose.yml                # Local MongoDB 7.0 WiredTiger container specification (port 27017)
├── requirements.txt                  # Python dependencies (pymongo, pandas, numpy, python-dotenv)
├── .env.example                      # Sanitized environment configuration template
├── .gitignore                        # Hardened secret & raw telemetry dump exclusion rules
└── README.md                         # Production architecture, schema design & benchmark documentation
```

## 3. Data Governance & Zero-Breach Privacy Architecture (DPDP Act 2023 & PCI-DSS v4.0)

Every dataset loaded by `scripts/sentinel_data_loader.py` passes through `enforce_data_privacy_guardrails()` **before** a single BSON document is serialized or inserted:

* **Zero Raw PII Persistence**: Automatically scans and strips/rejects 12 forbidden raw PII column types (`customer_name`, `full_name`, `email`, `phone`, `mobile_number`, `card_number`, `pan`, `aadhaar`, `upi_id`, `vpa`, `ip_address`, `bank_account_number`).
* **Cryptographic Pseudonymization**: Verifies that 100% of entity identifiers follow tokenized 16-hex-character formats (`C_<16-hex-hash>` across 500 customers, `M_<16-hex-hash>` across 120 merchants, and `D_<16-hex-hash>` across 900 devices).
* **Client Brand Anonymization**: Replaces any proprietary merchant branding with standardized domain tokens (`Merchant_<MCC>_<id>`, e.g., `Merchant_Gaming_014`).
* **Synthetic Telemetry Verification & Integer Paise Invariant**: Asserts `is_synthetic == True` on 100% of ingested records and enforces exact integer paise representation (`amount_paise == round(transaction_amount * 100)`) to prevent IEEE-754 floating-point drift during financial aggregations.

## 4. Three-Way Telemetry Data Flow

To guarantee zero-downtime execution across local workstations, cloud staging replicas, and CI/CD pipelines, `load_sentinel_tables()` resolves telemetry across three automatic tiers:

| Loader Tier | Source Location | Activation Condition | Governance & Output |
| :--- | :--- | :--- | :--- |
| **Tier 1: Local Sanitized Telemetry** | `SENTINEL_DATA_DIR` (`./sentinel_data/tables/*.csv` or `parquet/*.parquet`) | Directory exists on disk with all 7 core tables | Passes all 7 DataFrames through `enforce_data_privacy_guardrails()` |
| **Tier 2: PostgreSQL / Supabase Replica** | `DATABASE_URL` (`public` or `sentinel_demo` schema) | Valid connection string configured and reachable within 10s timeout | Queries live tables (`customers`, `transactions`, `devices`, `merchants`, `payment_outcomes`, `labels`, `features`) and validates via `enforce_data_privacy_guardrails()` |
| **Tier 3: Deterministic Synthetic Generator** | Built-in `generate_synthetic_sentinel_dataset(seed=42)` | Automatic fallback when Tiers 1 & 2 are absent | Generates **5,500 transactions**, **500 customers**, **120 merchants**, and **900 devices** (with 15 shared mule-ring devices and realistic 24h velocity bursts) |

## 5. Hybrid BSON Schema Design: Embedded vs. Referenced Rationale

In `sentinel_fraud_db`, collection structure is governed by entity cardinality, document growth bounds, and WiredTiger page-allocation behavior:

| Design Dimension | `customers` Collection (Embedded `devices` Array) | `transactions` Collection (Referenced `customer_id`, `merchant_id`, `device_id`) |
| :--- | :--- | :--- |
| **Cardinality & Growth Bound** | **1-to-Few Bounded**: A retail payer typically uses 1 to 5 hardware devices (`avg = 3.78`, `max = 15` in our 5,500-txn corpus). | **1-to-Many Unbounded**: Active accounts generate thousands of UPI/Card transactions over their lifecycle; merchants process millions. |
| **BSON Document Footprint** | **911 bytes average** (`99.99457%` headroom below MongoDB's 16 MB BSON limit). | **1,017 bytes per transaction** as an independent, immutable event document. |
| **Online Scoring Read SLA (`p99 <= 250 ms`)** | **O(1) Single-Document Atomic Read**: Fetching a customer profile by `customer_id` returns all known `devices` (`device_id`, `device_kind`, `first_seen`, `last_seen`, `txn_count`, `is_trusted`) in a single WiredTiger page read (`< 1 ms`), immediately resolving `FE-02 (is_new_device)` without cross-collection joins. | **Indexed Range Scan via ESR B-Tree**: Point-in-time velocity windows (`last 1h`, `last 24h`) scan only the contiguous B-Tree leaf nodes for `(customer_id, transaction_time: -1)`. |
| **Write Amplification & Contention** | Device list updates occur rarely (only on new device onboarding or periodic trust refresh). | **Zero Array Re-allocation**: At 2,800 TPS peak, appending to an embedded `transactions` array inside `customers` would force WiredTiger to constantly re-allocate and rewrite growing customer documents, causing document-level lock contention and eventual 16 MB BSON overflow. |

### Sample `customers` BSON Document (Embedded Bounded `devices` Array)

```json
{
  "customer_id": "C_a8f5f167f44f4964",
  "created_at": "2023-03-19T00:00:00+00:00",
  "customer_age": 60,
  "home_city": "Bengaluru",
  "region": "South",
  "device_count": 4,
  "devices": [
    {
      "device_id": "D_4b227777d4dd1fc6",
      "device_kind": "mobile_android",
      "first_seen": "2024-11-04T19:41:56+00:00",
      "last_seen": "2025-05-14T16:08:21+00:00",
      "txn_count": 7,
      "is_trusted": true
    },
    {
      "device_id": "D_ef2d127de37b942b",
      "device_kind": "desktop_web",
      "first_seen": "2024-11-18T03:12:44+00:00",
      "last_seen": "2025-04-29T11:20:09+00:00",
      "txn_count": 5,
      "is_trusted": true
    }
  ],
  "is_synthetic": true
}
```

### Sample `transactions` BSON Document (Referenced Entities + Immutable Merchant Snapshot)

```json
{
  "transaction_id": "TX_0000000001",
  "customer_id": "C_2878c28c88c4b239",
  "merchant_id": "M_4e07408562bedb8b",
  "merchant": {
    "merchant_id": "M_4e07408562bedb8b",
    "merchant_name": "Merchant_Gaming_003",
    "merchant_category": "gaming",
    "city": "Delhi",
    "lat": 28.5731,
    "lon": 77.1528
  },
  "device_id": "D_19581e27de7ced00",
  "amount_paise": 185966,
  "transaction_amount": 1859.66,
  "payment_channel": "UPI",
  "merchant_category": "gaming",
  "transaction_time": "2024-11-01T00:22:08+00:00",
  "transaction_epoch_sec": 1730420528,
  "status": "SUCCESS",
  "fraud_label": 0,
  "reason_codes": ["R01_NEW_DEVICE", "R08_HIGH_RISK_MCC"],
  "txn_count_last_1h": 0,
  "txn_amount_last_24h_paise": 0,
  "combined_risk_flags": 1,
  "hour_of_day_ist": 5,
  "is_night": false,
  "split": "warmup",
  "review_required": false,
  "is_synthetic": true
}
```

## 6. Operational Write/Read Lifecycle (CRUD Verification)

Stage 4 of `sentinel_nosql_telemetry.py` executes and validates all 5 core MongoDB operational write/read patterns:

| Operation | Target Collection | Query / Payload Summary | Verified Execution Result |
| :--- | :--- | :--- | :--- |
| **`insert_one`** | `customers` & `transactions` | Inserts canary customer `C_CANARY_9999999999` and high-risk gaming UPI authorization `TX_CANARY_9999999999` (`Rs. 49,999.00` / `4,999,900 paise`) | `acknowledged = True` on both collections |
| **`insert_many`** | `customers` & `transactions` | Unordered bulk ingestion (`ordered=False`) of **500 customers** and **5,500 transactions** | **500** customers + **5,500** transactions inserted (`5,501` total with canary) |
| **`find` (Filtered & Projected)** | `transactions` | Filter: `payment_channel == "UPI"` and `transaction_amount > 10000.0` with projection excluding `_id` | **168 high-value UPI transactions** matched and sorted descending |
| **`update_many`** | `transactions` | Filter: `merchant_category == "gaming"` and `transaction_amount > 25000.0`; appends `R08_HIGH_RISK_MCC` to `reason_codes` via `addToSet` and sets `review_required = True`, `escalation_tag = "HIGH_VALUE_GAMING_AUDIT"` | **14 high-value gaming transactions** matched and escalated |
| **`delete_one`** | `transactions` & `customers` | Deletes canary probe documents `TX_CANARY_9999999999` and `C_CANARY_9999999999` | `deleted_count = 1` each; post-cleanup counts restored to exactly **500 customers** and **5,500 transactions** |

## 7. Compound ESR B-Tree Index Benchmark (`.explain("executionStats")`)

During real-time authorization scoring, the feature engine queries a customer's historical transactions ordered by most recent timestamp:

```javascript
db.transactions.find({"customer_id": "C_89aec26619606cb4"}).sort({"transaction_time": -1}).explain("executionStats")
```

> [!NOTE]
> In the PyMongo 4.x Python driver, `cursor.explain()` executes with `allPlansExecution` verbosity by default (which inherently populates `executionStats` and `queryPlanner`) and accepts no positional arguments (`cursor.explain()`), matching the driver API specification.

We construct three B-Tree indexes on `db.transactions`:
1. `idx_customer_id_1`: `[("customer_id", 1)]`
2. `idx_transaction_time_-1`: `[("transaction_time", -1)]`
3. `idx_customer_id_1_transaction_time_-1`: `[("customer_id", 1), ("transaction_time", -1)]` (**ESR Compound Index**: **E**quality on `customer_id`, **S**ort/Range on `transaction_time: -1`)

| Execution Metric | Before Indexing (Unindexed Baseline) | After Compound ESR Index (`idx_customer_id_1_transaction_time_-1`) | Engineering Improvement |
| :--- | :--- | :--- | :--- |
| **Winning Query Plan Stage** | `SORT <- COLLSCAN` | `FETCH <- IXSCAN (idx_customer_id_1_transaction_time_-1)` | Eliminates full collection scan and blocking in-memory sort |
| **`nReturned` (Matching Docs)** | `24` | `24` | Identical deterministic result set |
| **`totalDocsExamined`** | `5,500` | `24` | **99.56% reduction** in BSON documents scanned (`229.2x` fewer docs) |
| **`totalKeysExamined`** | `0` | `24` | **1.000 selectivity ratio** (`nReturned == totalKeysExamined == totalDocsExamined`) |
| **Execution Time (`ms`)** | `~4.62 ms` | `~0.03 ms` | **>100x speedup** (keeps scoring well within `p50 <= 80 ms` SLA) |
| **In-Memory `SORT` Stage** | Required (`100 MB` RAM cap risk at scale) | **Eliminated (`False`)** | Index key order natively matches `.sort({"transaction_time": -1})` |

## 8. Multi-Stage Aggregation Pipeline Benchmarks

### Pipeline 1: Fraud Rate & Volume Exposure by `merchant_category`

Groups all 5,500 transactions by `merchant_category`, computing transaction volume, fraud count, integer paise sum, INR volume, average ticket size, and percentage fraud rate:

| Merchant Category | Total Txns | Fraud Txns | Fraud Rate (%) | Total Volume (INR) | Total Volume (Paise) | Fraud Volume (INR) | Avg Ticket (INR) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **electronics** | 588 | 149 | **25.34%** | 2,688,003.96 | 268,800,396 | 1,095,627.14 | 4,571.44 |
| **travel** | 852 | 191 | **22.42%** | 4,765,545.71 | 476,554,571 | 1,680,103.52 | 5,593.36 |
| **gaming** | 267 | 52 | **19.48%** | 1,316,428.76 | 131,642,876 | 440,562.89 | 4,930.44 |
| **education** | 478 | 44 | **9.21%** | 2,106,345.93 | 210,634,593 | 523,982.40 | 4,406.58 |
| **quick_commerce** | 1,013 | 85 | **8.39%** | 4,662,337.33 | 466,233,733 | 1,086,419.12 | 4,602.50 |
| **utilities** | 993 | 78 | **7.85%** | 3,957,138.10 | 395,713,810 | 892,115.65 | 3,985.03 |
| **grocery** | 471 | 36 | **7.64%** | 1,944,774.22 | 194,477,422 | 412,890.18 | 4,129.03 |
| **food** | 838 | 57 | **6.80%** | 3,297,017.25 | 329,701,725 | 708,914.33 | 3,934.39 |

*Architectural Insight*: High-risk categories (`electronics`, `travel`, `gaming`) exhibit **19.48% to 25.34% fraud rates** (~3x higher than everyday utility, grocery, and food categories at `6.80% to 7.85%`), validating the `R08_HIGH_RISK_MCC` risk signal and per-MCC threshold stratification (`threshold_sets`).

### Pipeline 2A: Top 10 Customers by 24-Hour Spend Velocity (Per-Customer Rolling 24h Peak Window)

Evaluates each customer's peak 24-hour burst window (`[t_peak - 24h, t_peak]`) in both INR and exact integer paise (`amount_paise`):

| Rank | Customer ID | 24h Txns | 24h Spend (INR) | 24h Spend (Paise) | Max Single Txn (INR) | Fraud Txns in 24h |
| :---: | :--- | ---: | ---: | ---: | ---: | ---: |
| **1** | `C_6df74ad4ce74e2a8` | 6 | **367,006.21** | `36,700,621` | 80,455.20 | 6 |
| **2** | `C_87ceee91fc100ef4` | 7 | **365,901.54** | `36,590,154` | 79,593.75 | 6 |
| **3** | `C_32bcb7477087ff42` | 6 | **355,135.67** | `35,513,567` | 78,029.88 | 6 |
| **4** | `C_32abf437241e8590` | 6 | **345,807.52** | `34,580,752` | 79,104.33 | 6 |
| **5** | `C_719af91bdb71d86b` | 6 | **344,874.03** | `34,487,403` | 77,812.19 | 6 |
| **6** | `C_7dcdd3e88195a3cf` | 6 | **344,486.43** | `34,448,643` | 76,940.15 | 6 |
| **7** | `C_c25b1be6e0bc1f40` | 6 | **343,093.67** | `34,309,367` | 78,415.90 | 6 |
| **8** | `C_093196fa0afaa586` | 6 | **336,695.76** | `33,669,576` | 75,118.42 | 6 |
| **9** | `C_e631e641e9303987` | 6 | **329,548.32** | `32,954,832` | 74,892.10 | 6 |
| **10** | `C_d0108337ae0591f1` | 6 | **327,064.60** | `32,706,460` | 73,604.88 | 6 |

### Pipeline 2B: Top 10 Customers by Global Dataset Trailing 24-Hour Window (`2025-05-29T23:06:22Z` to `2025-05-30T23:06:22Z`)

Evaluates spend velocity strictly within the final 24 hours of the dataset timeline (`transaction_time >= dataset_max_time - 24h`):

| Rank | Customer ID | Global 24h Txns | Global 24h Spend (INR) | Global 24h Spend (Paise) | Fraud Txns in Window |
| :---: | :--- | ---: | ---: | ---: | ---: |
| **1** | `C_6df74ad4ce74e2a8` | 6 | **367,006.21** | `36,700,621` | 6 |
| **2** | `C_03887f236e21fa86` | 6 | **187,697.82** | `18,769,782` | 6 |
| **3** | `C_f6c8f2c04c6ea9bf` | 2 | **39,558.84** | `3,955,884` | 1 |
| **4** | `C_f0f800eb6c915623` | 3 | **30,270.57** | `3,027,057` | 1 |
| **5** | `C_32bcb7477087ff42` | 1 | **25,910.74** | `2,591,074` | 1 |
| **6** | `C_0cc9cb709b07eae1` | 2 | **12,505.04** | `1,250,504` | 1 |
| **7** | `C_b2656c7900c5fb42` | 2 | **12,320.23** | `1,232,023` | 0 |
| **8** | `C_c888fbd6264b01c6` | 1 | **11,197.51** | `1,119,751` | 1 |
| **9** | `C_b10b0e178e895baf` | 2 | **11,167.89** | `1,116,789` | 0 |
| **10** | `C_5fb60dc8d874ac78` | 2 | **11,141.16** | `1,114,116` | 1 |

### Pipeline 3: Risk Signal Precision Across `R01` to `R08` via Array Unwind

Unwinds the `reason_codes` array across all transactions to quantify standalone signal precision (`fraud_hits / trigger_count`) and financial exposure:

| Reason Code | Trigger Condition (`FE-01` to `FE-10`) | Total Triggers | Fraud Hits | Genuine Hits | Signal Precision (%) | Exposed Volume (INR) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **`R07_VELOCITY`** | `txn_count_last_1h >= 4` | 78 | 78 | 0 | **100.00%** | 4,870,381.58 |
| **`R04_FAILED_ATTEMPTS`** | `has_recent_failures == True` (`>= 3` failed in 24h) | 353 | 352 | 1 | **99.72%** | 13,371,445.00 |
| **`R05_AMOUNT_SPIKE`** | `amount_to_avg_ratio >= 5.0` | 211 | 207 | 4 | **98.10%** | 5,271,408.28 |
| **`R01_NEW_DEVICE`** | `is_new_device == True` (unseen in 90d) | 2,154 | 604 | 1,550 | **28.04%** | 14,939,181.94 |
| **`R03_ODD_HOUR`** | `is_night == True` (`00:00 to 04:59` IST) | 1,160 | 300 | 860 | **25.86%** | 5,275,287.78 |
| **`R08_HIGH_RISK_MCC`** | `merchant_category in {electronics, gaming, travel, ...}` | 1,707 | 392 | 1,315 | **22.96%** | 8,769,978.43 |
| **`R06_NEW_ACCOUNT`** | `is_new_account == True` (`account_age < 30d`) | 10 | 2 | 8 | **20.00%** | 17,061.77 |
| **`R02_GEO_MISMATCH`** | `is_new_city == True` OR `distance_km > 500` | 4,294 | 647 | 3,647 | **15.07%** | 20,408,664.98 |

*Operational Takeaway*: High-precision behavioral signals (`R07_VELOCITY` at **100.00%**, `R04_FAILED_ATTEMPTS` at **99.72%**, and `R05_AMOUNT_SPIKE` at **98.10%**) serve as high-confidence autonomous `BLOCK` triggers (`OBJ-02 >= 70%` precision target), whereas broad contextual signals (`R01_NEW_DEVICE`, `R02_GEO_MISMATCH`, `R08_HIGH_RISK_MCC`) provide recall uplift when combined in multi-flag ensembles (`FE-10: combined_risk_flags >= 3`).

## 9. Setup, Execution & Automated Verification Gates

### Environment Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment variables
cp .env.example .env

# 3. (Optional) Start local MongoDB 7.0 container via Docker Compose
docker compose up -d
```

### Running the Telemetry & Indexing Pipeline

```bash
# Run against live MongoDB / Atlas (with automatic fallback if daemon is offline)
python3 sentinel_nosql_telemetry.py --verify

# Run deterministic in-memory CI/CD verification suite (zero external daemon required)
python3 sentinel_nosql_telemetry.py --offline-mock --verify
```

### Automated Verification Gates (`outputs/verification_report.json`)

| Gate ID | Verification Gate Name | Acceptance Criterion | Verified Result |
| :--- | :--- | :--- | :--- |
| **Gate 1** | DPDP Act 2023 & PCI-DSS v4.0 Privacy Gate | `0` raw PII columns; `100%` tokenized `C_*`, `M_*`, `D_*`; `is_synthetic == True` | **PASS** (`500 C_*`, `120 M_*`, `900 D_*`) |
| **Gate 2** | Telemetry Scale & Hybrid BSON Schema Gate | `>= 5,000` transactions; `customers` avg BSON size `< 4 KB` | **PASS** (`500` customers @ `911 B`, `5,500` txns @ `1,017 B`) |
| **Gate 3** | Operational CRUD & Canary Cleanup Gate | All 5 CRUD ops succeed; post-cleanup count restored to `5,500` | **PASS** (`168` UPI `> 10k`, `14` escalated, `1` canary deleted) |
| **Gate 4** | Compound ESR Index `.explain("executionStats")` Gate | `COLLSCAN` -> `IXSCAN`; `>= 99%` reduction in `totalDocsExamined` | **PASS** (`5,500 -> 24` docs examined, **99.56% reduction**) |
| **Gate 5** | Core Aggregation Pipelines Gate | 8 MCC groups and Top-10 24h spend velocity rankings verified | **PASS** (Leader `C_6df74ad4ce74e2a8` @ `Rs. 3,67,006.21`) |
| **Gate 6** | Signal Attribution Array Unwind Gate | All 8 reason codes (`R01` to `R08`) unwound and ranked by precision | **PASS** (`R07_VELOCITY` @ `100.00%` precision) |

### Cross-Database Sync Verification (`verify_sync_counts.py`)

To audit and verify record counts and entity parity between the Supabase (PostgreSQL) feature store replica and MongoDB Atlas:

```bash
# 1. Run cross-database count comparison
python verify_sync_counts.py

# 2. Run with side-by-side SQL vs. BSON sample record matching
python verify_sync_counts.py --samples
```

#### Verified Sync Audit Metrics

| Entity / Data Stream | Supabase (PostgreSQL) | MongoDB (BSON) | Sync Architecture & Integrity |
| :--- | :--- | :--- | :--- |
| **`customers`** | `2,500 rows` | `2,500 docs` | **100% ID Parity Verified** (`[PASS] 100% SYNCED`) |
| **`devices`** | `10,120 rows` | `1,804 embedded` | **Hybrid Embedded** inside `customers` for $O(1)$ scoring reads |
| **`transactions`** | `110,000 rows` | `5,500 docs` | **100% ID Parity Verified** on active scoring slice |
| **`payment_outcomes`** | `110,000 rows` | *(Merged into Txn)* | Denormalized into transaction BSON (`status`, `failure_code`) |
| **`labels`** | `110,000 rows` | *(Merged into Txn)* | Denormalized into transaction BSON (`fraud_label`) |
| **`features`** | `110,000 rows` | *(Merged into Txn)* | Denormalized into transaction BSON (`reason_codes`, velocity flags) |
| **`merchants`** | `200 rows` | *(Denormalized)* | Embedded immutable snapshot inside each transaction document |
