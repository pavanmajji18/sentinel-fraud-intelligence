# Project SENTINEL — Polyglot FastAPI RAG Investigator Assistant (`sentinel_rag_investigator`)

**Real-Time Payment Fraud Investigator Assistant Orchestrating Point-in-Time Relational/NoSQL Features, Neo4j 2-Hop Fraud Ring Topology, and Qdrant 3072-d Dense Policy Retrieval (`p99 <= 250 ms`, `100% Hit@5`)**

## 1. System Design & Production Engineering Context

Tier-1 RBI-regulated digital payment aggregators process **4.2 million daily authorizations** (~49 TPS average, 1,000 TPS sustained, **2,800 TPS festive peak**) across **UPI (57.95%)**, **Cards (31.02%)**, **Wallets (6.97%)**, and **Net Banking (4.06%)**. Legacy static rule engines suffer from **9.8 bps fraud loss** and a **2.3% false review/decline rate**, generating **22,000 daily manual review alerts** with no automated correlation across relational velocity counters, device-sharing graph rings, and regulatory standard operating playbooks.

This repository (`sentinel_rag_investigator`) provides the production **Polyglot FastAPI RAG Investigator Assistant (`POST /explain`)**. For every flagged or audited authorization, the service orchestrates three specialized storage engines in parallel, computes calibrated risk probabilities (`0.30 / 0.60 / 0.85` decision cutoffs), retrieves the top-5 relevant RBI/NPCI/SOP policy chunks in 3072-dimensional vector space, and synthesizes a strictly grounded, schema-validated JSON explanation inside a **250 ms p99 latency SLA**.

### Production SLA & Governance Verification Summary

| Engineering Gate | Target SLA / Requirement | Observed Benchmark | Status |
| :--- | :--- | :--- | :--- |
| **Gate 1: DPDP Act 2023 & PCI-DSS v4.0 Privacy Gate** | Strip 12 raw PII columns; 100% `C_*/M_*/D_*` tokenized IDs; `is_synthetic=True` | 12/12 PII columns checked; 500 `C_*`, 120 `M_*`, 900 `D_*` verified | **PASS** |
| **Gate 2: 3072-d Policy Corpus & Qdrant Indexing** | >= 200 RBI/NPCI/SOP documents (`180w` chunk, `40w` overlap, `3072-d` Cosine) | **220 documents / 238 chunks** indexed at `dim=3072` (`Distance.COSINE`) | **PASS** |
| **Gate 3: Validation Split Batch Evaluation** | 10 validation transactions (`5` confirmed fraud, `5` genuine) saved to JSON | **5/5 Fraud -> `BLOCK` (`CRITICAL`)**; **5/5 Genuine -> `ALLOW` (`LOW`)** | **PASS** |
| **Gate 4: JSON Schema & Factual Grounding (`OBJ-05/06`)** | >= 90% factual grounding; 100% Pydantic JSON schema compliance | **100.0% (10/10)** entity, paise, and policy citation grounding | **PASS** |
| **Gate 5: 10-Query Policy Retrieval Benchmark** | `Hit@5 >= 90.0%` across 10 domain investigator queries | **Hit@1 = 100.0%**, **Hit@3 = 100.0%**, **Hit@5 = 100.0%** (10/10) | **PASS** |
| **Gate 6: End-to-End Scoring & RAG Latency (`OBJ-10`)** | `p50 <= 80 ms`, `p99 <= 250 ms` (300 ms fail-open circuit breaker) | **p50 = 2.44 ms**, **p95 = 4.72 ms**, **p99 = 4.78 ms** | **PASS** |

## 2. Polyglot Storage & RAG Architecture

![Polyglot Architecture Diagram](architecture/polyglot_architecture_diagram.png)

```mermaid
flowchart LR
    subgraph Ingress["1. Payment Switch & Privacy Gate"]
        A["Payment Gateway Caller\nPOST /explain {transaction_id}"] --> B["DPDP Act 2023 & PCI-DSS v4.0 Gate\nenforce_data_privacy_guardrails()"]
    end

    subgraph Orchestrator["2. FastAPI Context Orchestrator (app/main.py)"]
        B --> C["SentinelInvestigatorOrchestrator\nParallel 3-Store Context Fan-Out"]
    end

    subgraph PolyglotStores["3. Polyglot Persistence Layer"]
        C -->|"O(1) PK Lookup (<1.5ms)"| S1[("Store 1: PostgreSQL / Supabase & MongoDB\nPoint-in-Time FE-01..FE-10 & R01..R08\nInteger Paise Invariant")]
        C -->|"Bolt Cypher 2-Hop (<2.0ms)"| S2[("Store 2: Neo4j Fraud Graph\n[:USED_DEVICE|PAID*1..2]\n7d Shared Device Rings (>=3 Cust)")]
        C -->|"3072-d Cosine Top-5 (<1.2ms)"| S3[("Store 3: Qdrant Vector Store\nsentinel_policy_corpus (220 Docs / 238 Chunks)\nGemini Embedding 2 (3072-d)")]
    end

    subgraph Synthesis["4. Dual-Gateway RAG & Verification"]
        S1 & S2 & S3 --> D["Dual-Gateway Synthesizer\nEuron EURI / Gemini 2.5 Flash\n+ FR-API-06 Deterministic Fallback"]
        D --> E["Grounding & Schema Verifier\nExplainResponse Pydantic Model\n300ms Circuit-Breaker Fail-Open"]
    end
```

### Polyglot Persistence Trade-Offs at 2,800 TPS Peak

| Store Tier | Engine | Workload & Access Pattern | Architectural Justification |
| :--- | :--- | :--- | :--- |
| **Store 1: Telemetry & Feature Store** | PostgreSQL (`sentinel_demo`) + MongoDB (`sentinel_fraud_db`) | Point-in-time feature vectors (`FE-01`..`FE-10`), exact integer paise ledger (`amount_paise = round(transaction_amount * 100)`), and bounded customer device arrays via ESR compound index `(customer_id, transaction_time)` | Multi-hop graph self-joins and 3072-d cosine similarity scans degrade relational B-tree buffer pools and violate `p99 <= 250 ms` under high write throughput. |
| **Store 2: Fraud Ring Graph Engine** | Neo4j 5 (`[:USED_DEVICE\|PAID*1..2]`) | Index-free adjacency traversal detecting mule devices shared by `>= 3` distinct customers within a 7-day sliding window (`FE-09`) and 2-hop confirmed fraud contagion | Relational join tables over high-degree shared devices suffer combinatorial join fan-out during coordinated mule-ring attacks. |
| **Store 3: Dense Policy Vector Index** | Qdrant (`sentinel_policy_corpus`, `Distance.COSINE`) | Top-5 semantic retrieval over 3072-d unit-normalized chunks (`180w` size, `40w` overlap) with payload filtering on `document_type` | Keyword search misses semantic equivalents across RBI circulars, NPCI UPI risk guidelines, and investigator SOP playbooks. |

## 3. Data Governance, Schema & Point-in-Time Feature Specification

### Pre-Ingestion DPDP Act 2023 & PCI-DSS v4.0 Gate (`enforce_data_privacy_guardrails`)

Every data-loading tier (`Tier 1` local CSV/Parquet, `Tier 2` PostgreSQL/Supabase replica, and `Tier 3` deterministic synthetic telemetry generator) executes `enforce_data_privacy_guardrails()` prior to indexing or inference:
- **Zero Raw PII Storage**: Automatically strips or rejects 12 forbidden PII attributes (`customer_name`, `full_name`, `email`, `phone`, `mobile_number`, `card_number`, `pan`, `aadhaar`, `upi_id`, `vpa`, `ip_address`, `bank_account_number`).
- **Cryptographic Pseudonymization**: Verifies that 100% of entity keys follow tokenized format (`C_<16-hex-hash>`, `M_<16-hex-hash>`, `D_<16-hex-hash>`) and merchant names follow generic `Merchant_<MCC>_<id>` anonymization.
- **Temporal Leakage Prevention**: Ground-truth fraud labels mature 60 days post-authorization (`label_available_at = transaction_time + 60 days`). All point-in-time features (`FE-01`..`FE-10`) evaluate strictly over `t_prior < t_current`.

### Point-in-Time Features (`FE-01` to `FE-10`) & Deterministic Reason Codes (`R01` to `R08`)

| Feature / Code | Formula & Trigger Threshold | Operational Meaning |
| :--- | :--- | :--- |
| `FE-01` / `R05_AMOUNT_SPIKE` | `amount_to_avg_ratio = transaction_amount / average_transaction_value >= 5.0` | Authorization is >= 5.0x the customer's trailing 90-day mean spend (`null` on cold start). |
| `FE-02` / `R01_NEW_DEVICE` | `is_new_device == True` | Hardware fingerprint (`device_id`) was unseen for `customer_id` in the trailing 90 days (`null` if missing). |
| `FE-03` / `R02_GEO_MISMATCH` | `is_new_city == True` or `customer_merchant_distance_km > 500.0` | Great-circle Haversine distance exceeds 500 km or city is unseen in 90 days (`null` when GPS is masked). |
| `FE-04` / `R03_ODD_HOUR` | `is_night == True` (`0 <= hour_of_day < 5` in IST, `UTC+05:30`) | Nocturnal authorization between `00:00` and `04:59` IST when account holders are typically asleep. |
| `FE-05` / `R04_FAILED_ATTEMPTS` | `has_recent_failures == True` (`failed_transactions >= 3` in 24h) | Three or more `AUTH_DECLINED` outcomes in the prior 24 hours indicating PIN/MPIN or OTP retry attacks. |
| `FE-06` / `R06_NEW_ACCOUNT` | `is_new_account == True` (`account_age < 30` days) | Newly onboarded customer account transacting within the first 30 days of KYC registration. |
| `FE-07` / `R07_VELOCITY` | `txn_count_last_1h >= 4` and `txn_amount_last_24h_paise` | High-frequency burst of >= 4 attempts within 60 minutes paired with 24-hour cumulative paise volume. |
| `FE-08` / `R08_HIGH_RISK_MCC` | `merchant_category in {"electronics", "gaming", "travel", "gift_cards", "crypto"}` | High-liquidity merchant category; Bayesian smoothed rate `(fraud_count + 0.6) / (total_count + 100)`. |
| `FE-09` (Graph Ring Signal) | `distinct_customers_per_device_7d >= 3` | Hardware `device_id` shared by 3 or more distinct customer accounts within a trailing 7-day window. |
| `FE-10` (Composite Risk) | `combined_risk_flags` | Integer sum of active boolean anomaly flags across device, geography, odd-hour, failure, and account age. |

## 4. Repository Structure

```text
sentinel_rag_investigator/
├── app/
│   ├── __init__.py
│   ├── main.py                      # FastAPI service (POST /explain, GET /health, GET /metrics)
│   ├── orchestrator.py              # 3-Store Context Orchestrator, Scorer & Grounding Verifier
│   └── policy_corpus.py             # 220-doc RBI/NPCI/SOP corpus chunker & 3072-d Qdrant retriever
├── scripts/
│   ├── __init__.py
│   └── sentinel_data_loader.py      # 3-Way Telemetry Loader & DPDP Act 2023 / PCI-DSS Privacy Gate
├── architecture/
│   ├── polyglot_architecture_diagram.pdf  # 1-Page Landscape Architecture Blueprint (PDF)
│   └── polyglot_architecture_diagram.png  # 300-DPI Architecture Diagram (PNG)
├── outputs/
│   ├── validation_10_explanations.json    # Saved JSON outputs for 10 validation txns (5 fraud, 5 genuine)
│   ├── hit_at_5_retrieval_benchmark.json  # Raw 10-query Hit@1 / Hit@3 / Hit@5 benchmark results
│   ├── hit_at_5_retrieval_benchmark.md    # Formatted Markdown retrieval benchmark report
│   └── privacy_and_grounding_audit.json   # Gate 1..6 verification & DPDP compliance audit
├── tests/
│   └── test_api.py                  # Automated FastAPI endpoint lifecycle & schema test suite
├── run_investigator_rag_pipeline.py # End-to-end 6-stage CLI verification & benchmark runner
├── docker-compose.yml               # Multi-container orchestration (FastAPI + Qdrant + Neo4j 5)
├── requirements.txt                 # Pinned Python dependencies
├── .env.example                     # Sanitized environment template (EURI, Gemini, Qdrant, Neo4j, DB)
└── .gitignore                       # Hardened DPDP/PCI-DSS secret and telemetry blocker
```

## 5. Validation Set Evaluation (10 Transactions: 5 Confirmed Fraud, 5 Genuine)

Complete schema-validated JSON payloads (containing `cited_sources`, `graph_context`, `feature_snapshot`, and `grounding_verification`) are saved in `outputs/validation_10_explanations.json`.

| Transaction ID | Split | Ground Truth | Fraud Prob | Risk Level | Recommended Action | Fired Reason Codes | 7d Shared Ring (`FE-09`) | 2-Hop Fraud Exposure | Top Cited Policy Doc | Latency (ms) |
| :--- | :--- | :--- | ---: | :--- | :--- | :--- | :---: | ---: | :--- | ---: |
| `TX_0000004270` | `validation` | **FRAUD (1)** | `0.9900` | `CRITICAL` | **BLOCK** | `R01, R02, R03, R04, R05, R08` | Yes (`5` custs) | `4` txns | `DOC_PLAYBOOK_R08_008` | `4.64` |
| `TX_0000004344` | `validation` | **FRAUD (1)** | `0.9900` | `CRITICAL` | **BLOCK** | `R01, R02, R03, R04, R05, R08` | Yes (`7` custs) | `4` txns | `DOC_PLAYBOOK_R08_008` | `4.79` |
| `TX_0000004901` | `validation` | **FRAUD (1)** | `0.9900` | `CRITICAL` | **BLOCK** | `R01, R02, R04, R05, R07, R08` | No (`2` custs) | `8` txns | `DOC_PLAYBOOK_R08_008` | `3.94` |
| `TX_0000004905` | `validation` | **FRAUD (1)** | `0.9900` | `CRITICAL` | **BLOCK** | `R01, R02, R04, R05, R07, R08` | No (`1` cust) | `8` txns | `DOC_PLAYBOOK_R08_008` | `4.08` |
| `TX_0000004204` | `validation` | **FRAUD (1)** | `0.9900` | `CRITICAL` | **BLOCK** | `R01, R02, R03, R05, R06` | No (`0` custs) | `1` txn | `DOC_PLAYBOOK_R06_006` | `3.74` |
| `TX_0000004132` | `validation` | **GENUINE (0)** | `0.0400` | `LOW` | **ALLOW** | `None` (`[]`) | No (`1` cust) | `0` txns | `DOC_RBI_SLA_010` | `0.94` |
| `TX_0000004133` | `validation` | **GENUINE (0)** | `0.0400` | `LOW` | **ALLOW** | `None` (`[]`) | No (`1` cust) | `0` txns | `DOC_RBI_SLA_010` | `1.14` |
| `TX_0000004143` | `validation` | **GENUINE (0)** | `0.0400` | `LOW` | **ALLOW** | `None` (`[]`) | No (`1` cust) | `0` txns | `DOC_RBI_SLA_010` | `1.13` |
| `TX_0000004145` | `validation` | **GENUINE (0)** | `0.0400` | `LOW` | **ALLOW** | `None` (`[]`) | No (`1` cust) | `0` txns | `DOC_RBI_SLA_010` | `0.97` |
| `TX_0000004147` | `validation` | **GENUINE (0)** | `0.0400` | `LOW` | **ALLOW** | `None` (`[]`) | No (`1` cust) | `0` txns | `DOC_RBI_SLA_010` | `1.10` |

## 6. 10-Question Policy Retrieval Benchmark (`Hit@1 = 100%`, `Hit@3 = 100%`, `Hit@5 = 100%`)

Detailed retrieval evaluation metrics are saved in `outputs/hit_at_5_retrieval_benchmark.md` and `outputs/hit_at_5_retrieval_benchmark.json`.

| Query ID | Investigator Benchmark Question | Expected Authoritative Doc | Top-1 Retrieved Doc | Cosine Score | Rank | Hit@1 | Hit@3 | Hit@5 |
| :--- | :--- | :--- | :--- | ---: | ---: | :---: | :---: | :---: |
| `Q01` | Step-up authentication / escalation when `R01_NEW_DEVICE` triggers on unseen hardware in 90d | `DOC_PLAYBOOK_R01_001` | `DOC_PLAYBOOK_R01_001` | `0.8617` | `1` | Yes | Yes | Yes |
| `Q02` | Handling `R02_GEO_MISMATCH` when Haversine distance > 500 km or `customer_location` is null | `DOC_PLAYBOOK_R02_002` | `DOC_PLAYBOOK_R02_002` | `0.8655` | `1` | Yes | Yes | Yes |
| `Q03` | Nocturnal transactions between `00:00` and `04:59` IST flagged with `R03_ODD_HOUR` (`UTC+05:30`) | `DOC_PLAYBOOK_R03_003` | `DOC_PLAYBOOK_R03_003` | `0.8703` | `1` | Yes | Yes | Yes |
| `Q04` | Investigator SOP for `R04_FAILED_ATTEMPTS` when >= 3 `AUTH_DECLINED` failures occur in 24h | `DOC_PLAYBOOK_R04_004` | `DOC_PLAYBOOK_R04_004` | `0.8648` | `1` | Yes | Yes | Yes |
| `Q05` | Evaluating `R05_AMOUNT_SPIKE` (`amount_to_avg_ratio >= 5.0x`) while preserving integer paise | `DOC_PLAYBOOK_R05_005` | `DOC_PLAYBOOK_R05_005` | `0.8920` | `1` | Yes | Yes | Yes |
| `Q06` | Synthetic identity & onboarding velocity checks under `R06_NEW_ACCOUNT` (`account_age < 30d`) | `DOC_PLAYBOOK_R06_006` | `DOC_PLAYBOOK_R06_006` | `0.8687` | `1` | Yes | Yes | Yes |
| `Q07` | Adjudicating `R07_VELOCITY` bursts when `txn_count_last_1h >= 4` within 60 minutes | `DOC_PLAYBOOK_R07_007` | `DOC_PLAYBOOK_R07_007` | `0.8672` | `1` | Yes | Yes | Yes |
| `Q08` | Liquid merchant categories triggering `R08_HIGH_RISK_MCC` & smoothed Bayesian target encoding | `DOC_PLAYBOOK_R08_008` | `DOC_PLAYBOOK_R08_008` | `0.8805` | `1` | Yes | Yes | Yes |
| `Q09` | `NPCI-UPI-RING-2025` shared-device mule ring (`>= 3` customers in 7d) & 2-hop Neo4j contagion | `DOC_NPCI_GRAPH_009` | `DOC_NPCI_GRAPH_009` | `0.8913` | `1` | Yes | Yes | Yes |
| `Q10` | RBI real-time scoring latency SLA (`p99 <= 250 ms`) & 300 ms fail-open Rs. 10,000 threshold | `DOC_RBI_SLA_010` | `DOC_RBI_SLA_010` | `0.8617` | `1` | Yes | Yes | Yes |

## 7. Setup, Execution & Verification Instructions

### Environment Configuration

Copy `.env.example` to `.env` to configure live external endpoints (PostgreSQL/Supabase, MongoDB, Neo4j Aura/Bolt, Qdrant Cloud, and Euron EURI or Google Gemini API keys). When no external keys are configured or `--offline-mock` is passed, the service automatically activates its hermetic deterministic mode (`Tier 3` 5,500-transaction telemetry generator, in-memory 2-hop graph topology index, and 3072-d unit-sphere policy vector index).

```bash
cp .env.example .env
pip install -r requirements.txt
```

### Automated 6-Gate Verification Suite

Run the end-to-end verification and benchmark pipeline to execute all 6 production engineering stages and regenerate all artifacts under `outputs/`:

```bash
python3 run_investigator_rag_pipeline.py --offline-mock --verify
```

### Running the FastAPI Service & Querying `POST /explain`

```bash
# Start the FastAPI server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2

# Health & readiness probe
curl -s http://localhost:8000/health | python3 -m json.tool

# Request a grounded RAG investigator explanation for a flagged authorization
curl -s -X POST http://localhost:8000/explain \
  -H "Content-Type: application/json" \
  -d '{"transaction_id": "TX_0000004270"}' | python3 -m json.tool

# Inspect p50/p95/p99 latency and grounding SLA metrics
curl -s http://localhost:8000/metrics | python3 -m json.tool
```
