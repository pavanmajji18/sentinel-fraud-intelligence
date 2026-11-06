# Project SENTINEL — Dense Policy Retrieval Engine Benchmark Report

> **Domain**: Tier-1 RBI-Regulated Digital Payment Gateway (UPI / Cards / Net-Banking)  
> **Engine**: Qdrant HNSW + 3072-d Dense Embeddings (`gemini-embedding-2`)  
> **Timestamp**: `2026-11-06T10:42:34Z`  

---

## 1. Executive Summary & Retrieval SLAs

- **Corpus Scale**: 225 documents / 450 chunks across 5 categories
- **Hit@1 Precision**: **100.0%** (10/10)
- **Hit@3 Precision**: **100.0%** (10/10)
- **MRR@3 (Mean Reciprocal Rank)**: **1.0000**
- **Mean Top-1 Cosine Similarity**: **0.8209**
- **Query Latency (p50 / p95)**: **1611.87 ms** / **1884.82 ms**

---

## 2. 10 Natural-Language SOC Investigator Queries Summary

| ID | Target Category | Signal / Reason | Query Excerpt | Top-1 Document | Cosine | Latency | Hit@1 |
| :-: | :--- | :--- | :--- | :--- | :---: | :---: | :---: |
| **Q01** | `rbi` | `R01_NEW_DEVICE` | What are the RBI Master Direction zero custom... | `DOC_RBI_001` | **0.8048** | 1539.45 ms | **PASS** |
| **Q02** | `rbi` | `R04_FAILED_ATTEMPTS` | Under RBI Master Direction on Digital Payment... | `DOC_RBI_027` | **0.8071** | 1790.82 ms | **PASS** |
| **Q03** | `npci` | `R01_NEW_DEVICE` | How does NPCI Operating Circular OC-115 regul... | `DOC_NPCI_011` | **0.8202** | 1961.72 ms | **PASS** |
| **Q04** | `npci` | `R08_HIGH_RISK_MCC` | What are the NPCI UDIR T+5 dispute resolution... | `DOC_NPCI_004` | **0.8344** | 1306.95 ms | **PASS** |
| **Q05** | `internal_policies` | `R05_AMOUNT_SPIKE` | What are the SENTINEL POL-RISK-001 threshold_... | `DOC_POL_036` | **0.8426** | 1705.34 ms | **PASS** |
| **Q06** | `internal_policies` | `R07_VELOCITY` | Why does internal policy POL-ML-002 enforce a... | `DOC_POL_017` | **0.7913** | 1422.05 ms | **PASS** |
| **Q07** | `playbooks` | `R01_NEW_DEVICE` | According to SOC Playbook PB-ATO-101 runbook,... | `DOC_PB_001` | **0.8435** | 1588.34 ms | **PASS** |
| **Q08** | `playbooks` | `R07_VELOCITY` | How should an investigator execute SOC Playbo... | `DOC_PB_002` | **0.8250** | 1380.21 ms | **PASS** |
| **Q09** | `case_narratives` | `R01_NEW_DEVICE` | Retrieve closed forensic case narratives (CAS... | `DOC_CASE_001` | **0.8271** | 1718.61 ms | **PASS** |
| **Q10** | `case_narratives` | `R02_GEO_MISMATCH` | Find closed forensic case narratives (CASE-20... | `DOC_CASE_025` | **0.8128** | 1635.40 ms | **PASS** |

---

## 3. Detailed Top-5 Retrieved Chunks per Query

### Query `Q01`: rbi (`R01_NEW_DEVICE`)
**Query**: *"What are the RBI Master Direction zero customer liability rules, three working days reporting timelines, and T+10 shadow reversal mandates for unauthorized electronic banking transactions?"*  
**Latency**: `1539.45 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_RBI_001` | `rbi` | RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions — Section 2.2 Compliance Annex (DOC_RBI_001) | **0.8048** | [DOC_RBI_001 - rbi - RBI/DBR/2017-18/15-DPSS.CO.PD/Section 2.2 - R01_NEW_DEVICE] Document ID: DOC_RB... |
| #2 | `DOC_RBI_011` | `rbi` | RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions — Section 3.2 Compliance Annex (DOC_RBI_011) | **0.8028** | [DOC_RBI_011 - rbi - RBI/DBR/2017-18/15-DPSS.CO.PD/Section 3.2 - R01_NEW_DEVICE] Document ID: DOC_RB... |
| #3 | `DOC_RBI_031` | `rbi` | RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions — Section 5.2 Compliance Annex (DOC_RBI_031) | **0.8024** | [DOC_RBI_031 - rbi - RBI/DBR/2017-18/15-DPSS.CO.PD/Section 5.2 - R01_NEW_DEVICE] Document ID: DOC_RB... |
| #4 | `DOC_RBI_036` | `rbi` | RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions — Section 1.2 Compliance Annex (DOC_RBI_036) | **0.8021** | [DOC_RBI_036 - rbi - RBI/DBR/2017-18/15-DPSS.CO.PD/Section 1.2 - R01_NEW_DEVICE] Document ID: DOC_RB... |
| #5 | `DOC_RBI_021` | `rbi` | RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions — Section 4.2 Compliance Annex (DOC_RBI_021) | **0.7992** | [DOC_RBI_021 - rbi - RBI/DBR/2017-18/15-DPSS.CO.PD/Section 4.2 - R01_NEW_DEVICE] Document ID: DOC_RB... |

### Query `Q02`: rbi (`R04_FAILED_ATTEMPTS`)
**Query**: *"Under RBI Master Direction on Digital Payment Security Controls (RBI/DPSS/2021-22/82-CYBER), what is the CERT-In six-hour cyber incident reporting SLA and five-year India data localization mandate?"*  
**Latency**: `1790.82 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_RBI_027` | `rbi` | RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators — Section 1.3 Compliance Annex (DOC_RBI_027) | **0.8071** | [DOC_RBI_027 - rbi - RBI/DPSS/2021-22/82-CYBER/Section 1.3 - R04_FAILED_ATTEMPTS] Document ID: DOC_R... |
| #2 | `DOC_RBI_042` | `rbi` | RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators — Section 7.3 Compliance Annex (DOC_RBI_042) | **0.8009** | [DOC_RBI_042 - rbi - RBI/DPSS/2021-22/82-CYBER/Section 7.3 - R04_FAILED_ATTEMPTS] Document ID: DOC_R... |
| #3 | `DOC_RBI_007` | `rbi` | RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators — Section 8.3 Compliance Annex (DOC_RBI_007) | **0.7989** | [DOC_RBI_007 - rbi - RBI/DPSS/2021-22/82-CYBER/Section 8.3 - R04_FAILED_ATTEMPTS] Document ID: DOC_R... |
| #4 | `DOC_RBI_037` | `rbi` | RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators — Section 2.3 Compliance Annex (DOC_RBI_037) | **0.7979** | [DOC_RBI_037 - rbi - RBI/DPSS/2021-22/82-CYBER/Section 2.3 - R04_FAILED_ATTEMPTS] Document ID: DOC_R... |
| #5 | `DOC_RBI_032` | `rbi` | RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators — Section 6.3 Compliance Annex (DOC_RBI_032) | **0.7972** | [DOC_RBI_032 - rbi - RBI/DPSS/2021-22/82-CYBER/Section 6.3 - R04_FAILED_ATTEMPTS] Document ID: DOC_R... |

### Query `Q03`: npci (`R01_NEW_DEVICE`)
**Query**: *"How does NPCI Operating Circular OC-115 regulate SIM swap device binding and the 24-hour cooling-off period Rs. 2,000 limit on UPI?"*  
**Latency**: `1961.72 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_NPCI_011` | `npci` | NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit — Bulletin #11 (DOC_NPCI_011) | **0.8202** | [DOC_NPCI_011 - npci - NPCI/UPI/OC-115/2023-24 - R01_NEW_DEVICE] Document ID: DOC_NPCI_011 - Categor... |
| #2 | `DOC_NPCI_041` | `npci` | NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit — Bulletin #41 (DOC_NPCI_041) | **0.8183** | [DOC_NPCI_041 - npci - NPCI/UPI/OC-115/2023-24 - R01_NEW_DEVICE] Document ID: DOC_NPCI_041 - Categor... |
| #3 | `DOC_NPCI_036` | `npci` | NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit — Bulletin #36 (DOC_NPCI_036) | **0.8174** | [DOC_NPCI_036 - npci - NPCI/UPI/OC-115/2023-24 - R01_NEW_DEVICE] Document ID: DOC_NPCI_036 - Categor... |
| #4 | `DOC_NPCI_016` | `npci` | NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit — Bulletin #16 (DOC_NPCI_016) | **0.8171** | [DOC_NPCI_016 - npci - NPCI/UPI/OC-115/2023-24 - R01_NEW_DEVICE] Document ID: DOC_NPCI_016 - Categor... |
| #5 | `DOC_NPCI_001` | `npci` | NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit — Bulletin #01 (DOC_NPCI_001) | **0.8169** | [DOC_NPCI_001 - npci - NPCI/UPI/OC-115/2023-24 - R01_NEW_DEVICE] Document ID: DOC_NPCI_001 - Categor... |

### Query `Q04`: npci (`R08_HIGH_RISK_MCC`)
**Query**: *"What are the NPCI UDIR T+5 dispute resolution rules and MCC-204 high-risk merchant category controls for online gaming (MCC 7995) and crypto?"*  
**Latency**: `1306.95 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_NPCI_004` | `npci` | NPCI High-Risk Merchant Category Code (MCC) Controls for Gaming, Crypto & Digital Goods — Bulletin #04 (DOC_NPCI_004) | **0.8344** | [DOC_NPCI_004 - npci - NPCI/RISK/MCC-204/2024 - R08_HIGH_RISK_MCC] Document ID: DOC_NPCI_004 - Categ... |
| #2 | `DOC_NPCI_044` | `npci` | NPCI High-Risk Merchant Category Code (MCC) Controls for Gaming, Crypto & Digital Goods — Bulletin #44 (DOC_NPCI_044) | **0.8344** | [DOC_NPCI_044 - npci - NPCI/RISK/MCC-204/2024 - R08_HIGH_RISK_MCC] Document ID: DOC_NPCI_044 - Categ... |
| #3 | `DOC_NPCI_009` | `npci` | NPCI High-Risk Merchant Category Code (MCC) Controls for Gaming, Crypto & Digital Goods — Bulletin #09 (DOC_NPCI_009) | **0.8326** | [DOC_NPCI_009 - npci - NPCI/RISK/MCC-204/2024 - R08_HIGH_RISK_MCC] Document ID: DOC_NPCI_009 - Categ... |
| #4 | `DOC_NPCI_024` | `npci` | NPCI High-Risk Merchant Category Code (MCC) Controls for Gaming, Crypto & Digital Goods — Bulletin #24 (DOC_NPCI_024) | **0.8309** | [DOC_NPCI_024 - npci - NPCI/RISK/MCC-204/2024 - R08_HIGH_RISK_MCC] Document ID: DOC_NPCI_024 - Categ... |
| #5 | `DOC_NPCI_029` | `npci` | NPCI High-Risk Merchant Category Code (MCC) Controls for Gaming, Crypto & Digital Goods — Bulletin #29 (DOC_NPCI_029) | **0.8301** | [DOC_NPCI_029 - npci - NPCI/RISK/MCC-204/2024 - R08_HIGH_RISK_MCC] Document ID: DOC_NPCI_029 - Categ... |

### Query `Q05`: internal_policies (`R05_AMOUNT_SPIKE`)
**Query**: *"What are the SENTINEL POL-RISK-001 threshold_sets fraud probability cutoffs for BLOCK vs REVIEW and how does the 300ms fail-open circuit breaker handle transactions above Rs. 10,000?"*  
**Latency**: `1705.34 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_POL_036` | `internal_policies` | SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy (POL-RISK-001 v2.1 — DOC_POL_036) | **0.8426** | [DOC_POL_036 - internal_policies - POL-RISK-001-v2.1 - R05_AMOUNT_SPIKE] Document ID: DOC_POL_036 - ... |
| #2 | `DOC_POL_031` | `internal_policies` | SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy (POL-RISK-001 v2.2 — DOC_POL_031) | **0.8393** | [DOC_POL_031 - internal_policies - POL-RISK-001-v2.2 - R05_AMOUNT_SPIKE] Document ID: DOC_POL_031 - ... |
| #3 | `DOC_POL_011` | `internal_policies` | SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy (POL-RISK-001 v2.6 — DOC_POL_011) | **0.8392** | [DOC_POL_011 - internal_policies - POL-RISK-001-v2.6 - R05_AMOUNT_SPIKE] Document ID: DOC_POL_011 - ... |
| #4 | `DOC_POL_026` | `internal_policies` | SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy (POL-RISK-001 v2.3 — DOC_POL_026) | **0.8381** | [DOC_POL_026 - internal_policies - POL-RISK-001-v2.3 - R05_AMOUNT_SPIKE] Document ID: DOC_POL_026 - ... |
| #5 | `DOC_POL_016` | `internal_policies` | SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy (POL-RISK-001 v2.5 — DOC_POL_016) | **0.8375** | [DOC_POL_016 - internal_policies - POL-RISK-001-v2.5 - R05_AMOUNT_SPIKE] Document ID: DOC_POL_016 - ... |

### Query `Q06`: internal_policies (`R07_VELOCITY`)
**Query**: *"Why does internal policy POL-ML-002 enforce a 60-day chargeback label_available_at maturation window to prevent look-ahead leakage, and how does POL-PRIV-003 pseudonymize entity IDs under DPDP Act 2023?"*  
**Latency**: `1422.05 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_POL_017` | `internal_policies` | Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard (POL-ML-002 v2.6 — DOC_POL_017) | **0.7913** | [DOC_POL_017 - internal_policies - POL-ML-002-v2.6 - R07_VELOCITY] Document ID: DOC_POL_017 - Catego... |
| #2 | `DOC_POL_037` | `internal_policies` | Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard (POL-ML-002 v2.2 — DOC_POL_037) | **0.7869** | [DOC_POL_037 - internal_policies - POL-ML-002-v2.2 - R07_VELOCITY] Document ID: DOC_POL_037 - Catego... |
| #3 | `DOC_POL_027` | `internal_policies` | Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard (POL-ML-002 v2.4 — DOC_POL_027) | **0.7859** | [DOC_POL_027 - internal_policies - POL-ML-002-v2.4 - R07_VELOCITY] Document ID: DOC_POL_027 - Catego... |
| #4 | `DOC_POL_022` | `internal_policies` | Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard (POL-ML-002 v2.5 — DOC_POL_022) | **0.7818** | [DOC_POL_022 - internal_policies - POL-ML-002-v2.5 - R07_VELOCITY] Document ID: DOC_POL_022 - Catego... |
| #5 | `DOC_POL_042` | `internal_policies` | Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard (POL-ML-002 v2.1 — DOC_POL_042) | **0.7810** | [DOC_POL_042 - internal_policies - POL-ML-002-v2.1 - R07_VELOCITY] Document ID: DOC_POL_042 - Catego... |

### Query `Q07`: playbooks (`R01_NEW_DEVICE`)
**Query**: *"According to SOC Playbook PB-ATO-101 runbook, what are Step 1 immediate containment and Step 3 out-of-band biometric quarantine actions when a customer transacts from a newly bound device at 3 AM IST over 500 km away?"*  
**Latency**: `1588.34 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_PB_001` | `playbooks` | SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage — SOP Runbook #01 (DOC_PB_001) | **0.8435** | [DOC_PB_001 - playbooks - PB-ATO-101-R01 - R01_NEW_DEVICE] Document ID: DOC_PB_001 - Category: SOC I... |
| #2 | `DOC_PB_031` | `playbooks` | SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage — SOP Runbook #31 (DOC_PB_031) | **0.8412** | [DOC_PB_031 - playbooks - PB-ATO-101-R31 - R01_NEW_DEVICE] Document ID: DOC_PB_031 - Category: SOC I... |
| #3 | `DOC_PB_021` | `playbooks` | SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage — SOP Runbook #21 (DOC_PB_021) | **0.8409** | [DOC_PB_021 - playbooks - PB-ATO-101-R21 - R01_NEW_DEVICE] Document ID: DOC_PB_021 - Category: SOC I... |
| #4 | `DOC_PB_016` | `playbooks` | SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage — SOP Runbook #16 (DOC_PB_016) | **0.8384** | [DOC_PB_016 - playbooks - PB-ATO-101-R16 - R01_NEW_DEVICE] Document ID: DOC_PB_016 - Category: SOC I... |
| #5 | `DOC_PB_041` | `playbooks` | SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage — SOP Runbook #41 (DOC_PB_041) | **0.8384** | [DOC_PB_041 - playbooks - PB-ATO-101-R41 - R01_NEW_DEVICE] Document ID: DOC_PB_041 - Category: SOC I... |

### Query `Q08`: playbooks (`R07_VELOCITY`)
**Query**: *"How should an investigator execute SOC Playbook PB-RING-102 runbook Step 1 blocklist and Step 2 2-hop blast-radius expansion for a shared device used by 3 or more distinct customers in 7 days?"*  
**Latency**: `1380.21 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_PB_002` | `playbooks` | SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment — SOP Runbook #02 (DOC_PB_002) | **0.8250** | [DOC_PB_002 - playbooks - PB-RING-102-R02 - R07_VELOCITY] Document ID: DOC_PB_002 - Category: SOC In... |
| #2 | `DOC_PB_017` | `playbooks` | SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment — SOP Runbook #17 (DOC_PB_017) | **0.8213** | [DOC_PB_017 - playbooks - PB-RING-102-R17 - R07_VELOCITY] Document ID: DOC_PB_017 - Category: SOC In... |
| #3 | `DOC_PB_037` | `playbooks` | SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment — SOP Runbook #37 (DOC_PB_037) | **0.8202** | [DOC_PB_037 - playbooks - PB-RING-102-R37 - R07_VELOCITY] Document ID: DOC_PB_037 - Category: SOC In... |
| #4 | `DOC_PB_022` | `playbooks` | SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment — SOP Runbook #22 (DOC_PB_022) | **0.8187** | [DOC_PB_022 - playbooks - PB-RING-102-R22 - R07_VELOCITY] Document ID: DOC_PB_022 - Category: SOC In... |
| #5 | `DOC_PB_012` | `playbooks` | SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment — SOP Runbook #12 (DOC_PB_012) | **0.8170** | [DOC_PB_012 - playbooks - PB-RING-102-R12 - R07_VELOCITY] Document ID: DOC_PB_012 - Category: SOC In... |

### Query `Q09`: case_narratives (`R01_NEW_DEVICE`)
**Query**: *"Retrieve closed forensic case narratives (CASE-2025-RING-01) documenting the rooted Android emulator device-farm attack (D_83fd19a04b2e77c1) across Bengaluru, Mumbai, and Delhi preventing Rs. 11.64 Lakhs in fraud loss."*  
**Latency**: `1718.61 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_CASE_001` | `case_narratives` | Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi — Case File #001 (DOC_CASE_001) | **0.8271** | [DOC_CASE_001 - case_narratives - CASE-2025-RING-01-001 - R01_NEW_DEVICE] Document ID: DOC_CASE_001 ... |
| #2 | `DOC_CASE_011` | `case_narratives` | Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi — Case File #011 (DOC_CASE_011) | **0.8219** | [DOC_CASE_011 - case_narratives - CASE-2025-RING-01-011 - R01_NEW_DEVICE] Document ID: DOC_CASE_011 ... |
| #3 | `DOC_CASE_021` | `case_narratives` | Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi — Case File #021 (DOC_CASE_021) | **0.8211** | [DOC_CASE_021 - case_narratives - CASE-2025-RING-01-021 - R01_NEW_DEVICE] Document ID: DOC_CASE_021 ... |
| #4 | `DOC_CASE_016` | `case_narratives` | Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi — Case File #016 (DOC_CASE_016) | **0.8187** | [DOC_CASE_016 - case_narratives - CASE-2025-RING-01-016 - R01_NEW_DEVICE] Document ID: DOC_CASE_016 ... |
| #5 | `DOC_CASE_006` | `case_narratives` | Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi — Case File #006 (DOC_CASE_006) | **0.8182** | [DOC_CASE_006 - case_narratives - CASE-2025-RING-01-006 - R01_NEW_DEVICE] Document ID: DOC_CASE_006 ... |

### Query `Q10`: case_narratives (`R02_GEO_MISMATCH`)
**Query**: *"Find closed forensic case narratives (CASE-2025-FP-04 and CASE-2025-FP-05) where a corporate executive workstation purchase or family holiday travel booking was cleared as a genuine false positive via device continuity."*  
**Latency**: `1635.40 ms` | **MRR**: `1.0000`

| Rank | Document ID | Category | Title | Cosine | Excerpt |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_CASE_025` | `case_narratives` | Forensic Case Narrative: Genuine Family Holiday Travel Booking Across Chennai & Delhi Cleared via Device Continuity — Case File #025 (DOC_CASE_025) | **0.8128** | [DOC_CASE_025 - case_narratives - CASE-2025-FP-05-025 - R08_HIGH_RISK_MCC] Document ID: DOC_CASE_025... |
| #2 | `DOC_CASE_015` | `case_narratives` | Forensic Case Narrative: Genuine Family Holiday Travel Booking Across Chennai & Delhi Cleared via Device Continuity — Case File #015 (DOC_CASE_015) | **0.8087** | [DOC_CASE_015 - case_narratives - CASE-2025-FP-05-015 - R08_HIGH_RISK_MCC] Document ID: DOC_CASE_015... |
| #3 | `DOC_CASE_005` | `case_narratives` | Forensic Case Narrative: Genuine Family Holiday Travel Booking Across Chennai & Delhi Cleared via Device Continuity — Case File #005 (DOC_CASE_005) | **0.8083** | [DOC_CASE_005 - case_narratives - CASE-2025-FP-05-005 - R08_HIGH_RISK_MCC] Document ID: DOC_CASE_005... |
| #4 | `DOC_CASE_020` | `case_narratives` | Forensic Case Narrative: Genuine Family Holiday Travel Booking Across Chennai & Delhi Cleared via Device Continuity — Case File #020 (DOC_CASE_020) | **0.8074** | [DOC_CASE_020 - case_narratives - CASE-2025-FP-05-020 - R08_HIGH_RISK_MCC] Document ID: DOC_CASE_020... |
| #5 | `DOC_CASE_010` | `case_narratives` | Forensic Case Narrative: Genuine Family Holiday Travel Booking Across Chennai & Delhi Cleared via Device Continuity — Case File #010 (DOC_CASE_010) | **0.8072** | [DOC_CASE_010 - case_narratives - CASE-2025-FP-05-010 - R08_HIGH_RISK_MCC] Document ID: DOC_CASE_010... |

---

## 4. Payload-Filtered Retrieval: `document_type == 'playbooks'`

**Query**: *"In a SIM-swap and UPI PIN brute-force attack (CASE-2025-ATO-02 / R04_FAILED_ATTEMPTS) with 4 consecutive declined attempts and an 8.4x gaming spike under RBI zero-liability rules, what are the exact SOC runbook Step 1 credential lockout and Step 2 spike interception steps?"*  
- **Unfiltered Query Latency**: `163.66 ms`
- **Filtered Query Latency**: `169.71 ms`
- **100% Playbook Filter Precision**: `PASS`

### Unfiltered Top-5 Hits (Spans multiple document types):
| Rank | Document ID | Category | Regulatory Ref | Cosine | Title |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_PB_008` | `playbooks` | `PB-BRUTE-103-R08` | **0.8407** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #08 (DOC_PB_008) |
| #2 | `DOC_PB_023` | `playbooks` | `PB-BRUTE-103-R23` | **0.8373** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #23 (DOC_PB_023) |
| #3 | `DOC_PB_003` | `playbooks` | `PB-BRUTE-103-R03` | **0.8346** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #03 (DOC_PB_003) |
| #4 | `DOC_PB_028` | `playbooks` | `PB-BRUTE-103-R28` | **0.8327** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #28 (DOC_PB_028) |
| #5 | `DOC_PB_013` | `playbooks` | `PB-BRUTE-103-R13` | **0.8320** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #13 (DOC_PB_013) |

### Filtered Top-5 Hits (`document_type == 'playbooks'` strictly enforced):
| Rank | Document ID | Category | Regulatory Ref | Cosine | Title |
| :-: | :--- | :--- | :--- | :---: | :--- |
| #1 | `DOC_PB_008` | `playbooks` | `PB-BRUTE-103-R08` | **0.8407** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #08 (DOC_PB_008) |
| #2 | `DOC_PB_023` | `playbooks` | `PB-BRUTE-103-R23` | **0.8373** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #23 (DOC_PB_023) |
| #3 | `DOC_PB_003` | `playbooks` | `PB-BRUTE-103-R03` | **0.8346** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #03 (DOC_PB_003) |
| #4 | `DOC_PB_028` | `playbooks` | `PB-BRUTE-103-R28` | **0.8327** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #28 (DOC_PB_028) |
| #5 | `DOC_PB_013` | `playbooks` | `PB-BRUTE-103-R13` | **0.8320** | SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike — SOP Runbook #13 (DOC_PB_013) |

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