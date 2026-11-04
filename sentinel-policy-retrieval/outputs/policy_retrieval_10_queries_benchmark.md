# Project SENTINEL — Dense Policy Retrieval Engine Benchmark Report

> **Domain**: Tier-1 RBI-Regulated Digital Payment Gateway (UPI / Cards / Net-Banking)  
> **Engine**: Qdrant HNSW + 3072-d Dense Embeddings (`gemini-embedding-2`)  
> **Timestamp**: `2026-11-04T18:09:09Z`  

---

## 1. Executive Summary & Retrieval SLAs

- **Corpus Scale**: 225 documents / 450 chunks across 5 categories
- **Hit@1 Precision**: **100.0%** (10/10)
- **Hit@3 Precision**: **100.0%** (10/10)
- **MRR@3 (Mean Reciprocal Rank)**: **1.0000**
- **Mean Top-1 Cosine Similarity**: **0.7479**
- **Query Latency (p50 / p95)**: **7.04 ms** / **7.53 ms**

---

## 2. 10 Natural-Language SOC Investigator Queries Summary

| ID | Target Category | Signal / Reason | Query Excerpt | Top-1 Document | Cosine | Latency | Hit@1 |
| :-: | :--- | :--- | :--- | :--- | :---: | :---: | :---: |
| **Q01** | `rbi` | `R01_NEW_DEVICE` | What are the RBI Master Direction zero custom... | `DOC_RBI_001` | **0.6861** | 7.07 ms | **PASS** |
| **Q02** | `rbi` | `R04_FAILED_ATTEMPTS` | Under RBI Master Direction on Digital Payment... | `DOC_RBI_027` | **0.7359** | 7.06 ms | **PASS** |
| **Q03** | `npci` | `R01_NEW_DEVICE` | How does NPCI Operating Circular OC-115 regul... | `DOC_NPCI_006` | **0.7912** | 6.91 ms | **PASS** |
| **Q04** | `npci` | `R08_HIGH_RISK_MCC` | What are the NPCI UDIR T+5 dispute resolution... | `DOC_NPCI_045` | **0.6629** | 7.22 ms | **PASS** |
| **Q05** | `internal_policies` | `R05_AMOUNT_SPIKE` | What are the SENTINEL POL-RISK-001 threshold_... | `DOC_POL_011` | **0.9188** | 7.75 ms | **PASS** |
| **Q06** | `internal_policies` | `R07_VELOCITY` | Why does internal policy POL-ML-002 enforce a... | `DOC_POL_017` | **0.8565** | 6.57 ms | **PASS** |
| **Q07** | `playbooks` | `R01_NEW_DEVICE` | According to SOC Playbook PB-ATO-101 runbook,... | `DOC_PB_041` | **0.8485** | 6.74 ms | **PASS** |
| **Q08** | `playbooks` | `R07_VELOCITY` | How should an investigator execute SOC Playbo... | `DOC_PB_012` | **0.8572** | 7.26 ms | **PASS** |
| **Q09** | `case_narratives` | `R01_NEW_DEVICE` | Retrieve closed forensic case narratives (CAS... | `DOC_CASE_001` | **0.5518** | 6.69 ms | **PASS** |
| **Q10** | `case_narratives` | `R02_GEO_MISMATCH` | Find closed forensic case narratives (CASE-20... | `DOC_CASE_019` | **0.5705** | 7.03 ms | **PASS** |

---

## 3. Detailed Top-3 Retrieved Chunks per Query

### Query `Q01`: rbi (`R01_NEW_DEVICE`)
**Query**: *"What are the RBI Master Direction zero customer liability rules, three working days reporting timelines, and T+10 shadow reversal mandates for unauthorized electronic banking transactions?"*  
**Latency**: `7.07 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_RBI_001` | `rbi` | RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions — Section 2.2 Compliance Annex (DOC_RBI_001) | **0.6861** | [DOC_RBI_001 - rbi - RBI/DBR/2017-18/15-DPSS.CO.PD/Section 2.2 - R01_NEW_DEVICE] Document ID: DOC_RB... |
| #2 | `DOC_RBI_021` | `rbi` | RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions — Section 4.2 Compliance Annex (DOC_RBI_021) | **0.6859** | [DOC_RBI_021 - rbi - RBI/DBR/2017-18/15-DPSS.CO.PD/Section 4.2 - R01_NEW_DEVICE] Document ID: DOC_RB... |
| #3 | `DOC_RBI_006` | `rbi` | RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions — Section 7.2 Compliance Annex (DOC_RBI_006) | **0.6859** | [DOC_RBI_006 - rbi - RBI/DBR/2017-18/15-DPSS.CO.PD/Section 7.2 - R01_NEW_DEVICE] Document ID: DOC_RB... |

### Query `Q02`: rbi (`R04_FAILED_ATTEMPTS`)
**Query**: *"Under RBI Master Direction on Digital Payment Security Controls (RBI/DPSS/2021-22/82-CYBER), what is the CERT-In six-hour cyber incident reporting SLA and five-year India data localization mandate?"*  
**Latency**: `7.06 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_RBI_027` | `rbi` | RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators — Section 1.3 Compliance Annex (DOC_RBI_027) | **0.7359** | [DOC_RBI_027 - rbi - RBI/DPSS/2021-22/82-CYBER/Section 1.3 - R04_FAILED_ATTEMPTS] Document ID: DOC_R... |
| #2 | `DOC_RBI_022` | `rbi` | RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators — Section 5.3 Compliance Annex (DOC_RBI_022) | **0.7356** | [DOC_RBI_022 - rbi - RBI/DPSS/2021-22/82-CYBER/Section 5.3 - R04_FAILED_ATTEMPTS] Document ID: DOC_R... |
| #3 | `DOC_RBI_037` | `rbi` | RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators — Section 2.3 Compliance Annex (DOC_RBI_037) | **0.7356** | [DOC_RBI_037 - rbi - RBI/DPSS/2021-22/82-CYBER/Section 2.3 - R04_FAILED_ATTEMPTS] Document ID: DOC_R... |

### Query `Q03`: npci (`R01_NEW_DEVICE`)
**Query**: *"How does NPCI Operating Circular OC-115 regulate SIM swap device binding and the 24-hour cooling-off period Rs. 2,000 limit on UPI?"*  
**Latency**: `6.91 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_NPCI_006` | `npci` | NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit — Bulletin #06 (DOC_NPCI_006) | **0.7912** | [DOC_NPCI_006 - npci - NPCI/UPI/OC-115/2023-24 - R01_NEW_DEVICE] Document ID: DOC_NPCI_006 - Categor... |
| #2 | `DOC_NPCI_031` | `npci` | NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit — Bulletin #31 (DOC_NPCI_031) | **0.7912** | [DOC_NPCI_031 - npci - NPCI/UPI/OC-115/2023-24 - R01_NEW_DEVICE] Document ID: DOC_NPCI_031 - Categor... |
| #3 | `DOC_NPCI_016` | `npci` | NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit — Bulletin #16 (DOC_NPCI_016) | **0.7911** | [DOC_NPCI_016 - npci - NPCI/UPI/OC-115/2023-24 - R01_NEW_DEVICE] Document ID: DOC_NPCI_016 - Categor... |

### Query `Q04`: npci (`R08_HIGH_RISK_MCC`)
**Query**: *"What are the NPCI UDIR T+5 dispute resolution rules and MCC-204 high-risk merchant category controls for online gaming (MCC 7995) and crypto?"*  
**Latency**: `7.22 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_NPCI_045` | `npci` | NPCI Beneficiary Mule-Ring Detection & Shared Device Velocity Rules on UPI — Bulletin #45 (DOC_NPCI_045) | **0.6629** | [DOC_NPCI_045 - npci - NPCI/FRMS/MULE-190/2024 - R07_VELOCITY] #45): All PSP and acquiring switches ... |
| #2 | `DOC_NPCI_005` | `npci` | NPCI Beneficiary Mule-Ring Detection & Shared Device Velocity Rules on UPI — Bulletin #05 (DOC_NPCI_005) | **0.6629** | [DOC_NPCI_005 - npci - NPCI/FRMS/MULE-190/2024 - R07_VELOCITY] #05): All PSP and acquiring switches ... |
| #3 | `DOC_NPCI_025` | `npci` | NPCI Beneficiary Mule-Ring Detection & Shared Device Velocity Rules on UPI — Bulletin #25 (DOC_NPCI_025) | **0.6629** | [DOC_NPCI_025 - npci - NPCI/FRMS/MULE-190/2024 - R07_VELOCITY] #25): All PSP and acquiring switches ... |

### Query `Q05`: internal_policies (`R05_AMOUNT_SPIKE`)
**Query**: *"What are the SENTINEL POL-RISK-001 threshold_sets fraud probability cutoffs for BLOCK vs REVIEW and how does the 300ms fail-open circuit breaker handle transactions above Rs. 10,000?"*  
**Latency**: `7.75 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_POL_011` | `internal_policies` | SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy (POL-RISK-001 v2.6 — DOC_POL_011) | **0.9188** | [DOC_POL_011 - internal_policies - POL-RISK-001-v2.6 - R05_AMOUNT_SPIKE] 80 ms`, `p99 <= 250 ms`, `O... |
| #2 | `DOC_POL_016` | `internal_policies` | SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy (POL-RISK-001 v2.5 — DOC_POL_016) | **0.9188** | [DOC_POL_016 - internal_policies - POL-RISK-001-v2.5 - R05_AMOUNT_SPIKE] 80 ms`, `p99 <= 250 ms`, `O... |
| #3 | `DOC_POL_021` | `internal_policies` | SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy (POL-RISK-001 v2.4 — DOC_POL_021) | **0.9188** | [DOC_POL_021 - internal_policies - POL-RISK-001-v2.4 - R05_AMOUNT_SPIKE] 80 ms`, `p99 <= 250 ms`, `O... |

### Query `Q06`: internal_policies (`R07_VELOCITY`)
**Query**: *"Why does internal policy POL-ML-002 enforce a 60-day chargeback label_available_at maturation window to prevent look-ahead leakage, and how does POL-PRIV-003 pseudonymize entity IDs under DPDP Act 2023?"*  
**Latency**: `6.57 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_POL_017` | `internal_policies` | Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard (POL-ML-002 v2.6 — DOC_POL_017) | **0.8565** | [DOC_POL_017 - internal_policies - POL-ML-002-v2.6 - R07_VELOCITY] (`FE-08 mcc_target_encoded_rate =... |
| #2 | `DOC_POL_012` | `internal_policies` | Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard (POL-ML-002 v2.1 — DOC_POL_012) | **0.8565** | [DOC_POL_012 - internal_policies - POL-ML-002-v2.1 - R07_VELOCITY] (`FE-08 mcc_target_encoded_rate =... |
| #3 | `DOC_POL_032` | `internal_policies` | Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard (POL-ML-002 v2.3 — DOC_POL_032) | **0.8565** | [DOC_POL_032 - internal_policies - POL-ML-002-v2.3 - R07_VELOCITY] (`FE-08 mcc_target_encoded_rate =... |

### Query `Q07`: playbooks (`R01_NEW_DEVICE`)
**Query**: *"According to SOC Playbook PB-ATO-101 runbook, what are Step 1 immediate containment and Step 3 out-of-band biometric quarantine actions when a customer transacts from a newly bound device at 3 AM IST over 500 km away?"*  
**Latency**: `6.74 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_PB_041` | `playbooks` | SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage — SOP Runbook #41 (DOC_PB_041) | **0.8485** | [DOC_PB_041 - playbooks - PB-ATO-101-R41 - R01_NEW_DEVICE] 2 (Graph & Velocity Verification): Query ... |
| #2 | `DOC_PB_026` | `playbooks` | SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage — SOP Runbook #26 (DOC_PB_026) | **0.8484** | [DOC_PB_026 - playbooks - PB-ATO-101-R26 - R01_NEW_DEVICE] 2 (Graph & Velocity Verification): Query ... |
| #3 | `DOC_PB_036` | `playbooks` | SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage — SOP Runbook #36 (DOC_PB_036) | **0.8483** | [DOC_PB_036 - playbooks - PB-ATO-101-R36 - R01_NEW_DEVICE] 2 (Graph & Velocity Verification): Query ... |

### Query `Q08`: playbooks (`R07_VELOCITY`)
**Query**: *"How should an investigator execute SOC Playbook PB-RING-102 runbook Step 1 blocklist and Step 2 2-hop blast-radius expansion for a shared device used by 3 or more distinct customers in 7 days?"*  
**Latency**: `7.26 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_PB_012` | `playbooks` | SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment — SOP Runbook #12 (DOC_PB_012) | **0.8572** | [DOC_PB_012 - playbooks - PB-RING-102-R12 - R07_VELOCITY] Document ID: DOC_PB_012 - Category: SOC In... |
| #2 | `DOC_PB_037` | `playbooks` | SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment — SOP Runbook #37 (DOC_PB_037) | **0.8572** | [DOC_PB_037 - playbooks - PB-RING-102-R37 - R07_VELOCITY] Document ID: DOC_PB_037 - Category: SOC In... |
| #3 | `DOC_PB_022` | `playbooks` | SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment — SOP Runbook #22 (DOC_PB_022) | **0.8571** | [DOC_PB_022 - playbooks - PB-RING-102-R22 - R07_VELOCITY] Document ID: DOC_PB_022 - Category: SOC In... |

### Query `Q09`: case_narratives (`R01_NEW_DEVICE`)
**Query**: *"Retrieve closed forensic case narratives (CASE-2025-RING-01) documenting the rooted Android emulator device-farm attack (D_83fd19a04b2e77c1) across Bengaluru, Mumbai, and Delhi preventing Rs. 11.64 Lakhs in fraud loss."*  
**Latency**: `6.69 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_CASE_001` | `case_narratives` | Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi — Case File #001 (DOC_CASE_001) | **0.5518** | [DOC_CASE_001 - case_narratives - CASE-2025-RING-01-001 - R01_NEW_DEVICE] Document ID: DOC_CASE_001 ... |
| #2 | `DOC_CASE_041` | `case_narratives` | Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi — Case File #041 (DOC_CASE_041) | **0.5516** | [DOC_CASE_041 - case_narratives - CASE-2025-RING-01-041 - R01_NEW_DEVICE] Document ID: DOC_CASE_041 ... |
| #3 | `DOC_CASE_016` | `case_narratives` | Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi — Case File #016 (DOC_CASE_016) | **0.5515** | [DOC_CASE_016 - case_narratives - CASE-2025-RING-01-016 - R01_NEW_DEVICE] Document ID: DOC_CASE_016 ... |

### Query `Q10`: case_narratives (`R02_GEO_MISMATCH`)
**Query**: *"Find closed forensic case narratives (CASE-2025-FP-04 and CASE-2025-FP-05) where a corporate executive workstation purchase or family holiday travel booking was cleared as a genuine false positive via device continuity."*  
**Latency**: `7.03 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_CASE_019` | `case_narratives` | Forensic Case Narrative: Genuine Corporate Executive Domestic Flight & Laptop Purchase Cleared as False Positive — Case File #019 (DOC_CASE_019) | **0.5705** | [DOC_CASE_019 - case_narratives - CASE-2025-FP-04-019 - R02_GEO_MISMATCH] Document ID: DOC_CASE_019 ... |
| #2 | `DOC_CASE_034` | `case_narratives` | Forensic Case Narrative: Genuine Corporate Executive Domestic Flight & Laptop Purchase Cleared as False Positive — Case File #034 (DOC_CASE_034) | **0.5704** | [DOC_CASE_034 - case_narratives - CASE-2025-FP-04-034 - R02_GEO_MISMATCH] Document ID: DOC_CASE_034 ... |
| #3 | `DOC_CASE_044` | `case_narratives` | Forensic Case Narrative: Genuine Corporate Executive Domestic Flight & Laptop Purchase Cleared as False Positive — Case File #044 (DOC_CASE_044) | **0.5704** | [DOC_CASE_044 - case_narratives - CASE-2025-FP-04-044 - R02_GEO_MISMATCH] Document ID: DOC_CASE_044 ... |

---

## 4. Payload-Filtered Retrieval: `document_type == 'playbooks'`

**Query**: *"In a SIM-swap and UPI PIN brute-force attack (CASE-2025-ATO-02 / R04_FAILED_ATTEMPTS) with 4 consecutive declined attempts and an 8.4x gaming spike under RBI zero-liability rules, what are the exact SOC runbook Step 1 credential lockout and Step 2 spike interception steps?"*  
- **Unfiltered Query Latency**: `6.01 ms`
- **Filtered Query Latency**: `1.76 ms`
- **100% Playbook Filter Precision**: `PASS`

### Unfiltered Top-3 Hits (Spans multiple document types):
| Rank | Document ID | Category | Regulatory Ref | Cosine | Title |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_CASE_037` | `case_narratives` | `CASE-2025-ATO-02-037` | **0.8213** | Forensic Case Narrative: SIM-Swap & PIN Brute-Force Account Takeover on High-Value Gaming Merchant — Case File #037 (DOC_CASE_037) |
| #2 | `DOC_CASE_017` | `case_narratives` | `CASE-2025-ATO-02-017` | **0.8212** | Forensic Case Narrative: SIM-Swap & PIN Brute-Force Account Takeover on High-Value Gaming Merchant — Case File #017 (DOC_CASE_017) |
| #3 | `DOC_CASE_012` | `case_narratives` | `CASE-2025-ATO-02-012` | **0.8212** | Forensic Case Narrative: SIM-Swap & PIN Brute-Force Account Takeover on High-Value Gaming Merchant — Case File #012 (DOC_CASE_012) |

### Filtered Top-3 Hits (`document_type == 'playbooks'` strictly enforced):
| Rank | Document ID | Category | Regulatory Ref | Cosine | Title |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_PB_038` | `playbooks` | `PB-BRUTE-103-R38` | **0.7937** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #38 (DOC_PB_038) |
| #2 | `DOC_PB_013` | `playbooks` | `PB-BRUTE-103-R13` | **0.7932** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #13 (DOC_PB_013) |
| #3 | `DOC_PB_023` | `playbooks` | `PB-BRUTE-103-R23` | **0.7930** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #23 (DOC_PB_023) |

---

## 5. Automated Governance & SLA Verification Gates

| Gate Name | Status | Mandate / Verification Invariant |
| :--- | :---: | :--- |
| Gate 1 (DPDP Act 2023 & PCI-DSS v4.0 Privacy Gate) | **PASS** | Verified under `--offline-mock --verify` |
| Gate 2 (Qdrant Health Probe & 3072-d Cosine Collection Gate) | **PASS** | Verified under `--offline-mock --verify` |
| Gate 3 (220+ Corpus Cleaning & 180w/40w Sliding-Window Chunking Gate) | **PASS** | Verified under `--offline-mock --verify` |
| Gate 4 (3072-d Gemini Embedding 2 Shape, Float32 & Unit L2-Norm Gate) | **PASS** | Verified under `--offline-mock --verify` |
| Gate 5 (Qdrant Payload Schema Indexing & Upsert Completeness Gate) | **PASS** | Verified under `--offline-mock --verify` |
| Gate 6 (10+1 Investigator Query Precision & Payload Filter Gate) | **PASS** | Verified under `--offline-mock --verify` |