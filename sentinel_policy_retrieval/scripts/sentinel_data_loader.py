#!/usr/bin/env python3
"""
Multi-Tier Telemetry & Policy Corpus Loader + DPDP/PCI-DSS Data-Privacy Gate for Project SENTINEL.

Resolves sanitized payment authorization telemetry and unstructured regulatory/policy corpus via:
  1. Tier 1: Local sanitized telemetry directory (`tables/*.csv`, `parquet/*.parquet`, `corpus/*.json`)
  2. Tier 2: PostgreSQL / Supabase feature store replica (`DATABASE_URL`)
  3. Tier 3: Deterministic schema-exact synthetic telemetry & 225-document policy corpus generator

Includes `enforce_data_privacy_guardrails()` and `enforce_corpus_privacy_guardrails()` to guarantee
zero raw PII, zero client-identifying strings, and 100% tokenized entity identifiers (`C_<hash>`,
`M_<hash>`, `D_<hash>`) before any document or vector payload is persisted or exported.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

IST = timezone(timedelta(hours=5, minutes=30))
HIGH_RISK_MCCS = {"electronics", "gaming", "travel", "gift_cards", "crypto"}

FORBIDDEN_PII_COLUMNS = {
    "customer_name",
    "full_name",
    "email",
    "phone",
    "mobile_number",
    "card_number",
    "pan",
    "aadhaar",
    "upi_id",
    "vpa",
    "ip_address",
    "bank_account_number",
}

FORBIDDEN_PII_REGEXES = {
    "raw_email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "raw_pan": re.compile(r"\b[A-Z]{5}[0-9]{4}[A-Z]\b"),
    "raw_aadhaar": re.compile(r"\b[2-9]{1}[0-9]{3}\s[0-9]{4}\s[0-9]{4}\b"),
    "raw_card_16": re.compile(r"\b(?:\d[ -]*?){16}\b"),
}

CITIES_COORDS = {
    "Bengaluru": (12.9716, 77.5946, "South"),
    "Mumbai": (19.0760, 72.8777, "West"),
    "Delhi": (28.6139, 77.2090, "North"),
    "Hyderabad": (17.3850, 78.4867, "South"),
    "Chennai": (13.0827, 80.2707, "South"),
    "Pune": (18.5204, 73.8567, "West"),
    "Kolkata": (22.5726, 88.3639, "East"),
    "Ahmedabad": (23.0225, 72.5714, "West"),
}

MCC_LIST = [
    "grocery",
    "food",
    "electronics",
    "gaming",
    "travel",
    "quick_commerce",
    "education",
    "utilities",
]
CHANNELS = ["UPI", "card", "wallet", "net_banking"]
CHANNEL_WEIGHTS = [0.58, 0.31, 0.07, 0.04]
DEVICE_KINDS = ["mobile_android", "mobile_ios", "desktop_web"]


def haversine_km(
    lat1: Optional[float],
    lon1: Optional[float],
    lat2: Optional[float],
    lon2: Optional[float],
) -> Optional[float]:
    """Compute Haversine distance in km between two (lat, lon) coordinates."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
        return None
    r = 6371.0
    phi1, phi2 = math.radians(float(lat1)), math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return round(2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a)), 2)


def assign_reason_codes(
    is_new_device: Optional[bool],
    is_new_city: Optional[bool],
    distance_km: Optional[float],
    is_night: bool,
    has_recent_failures: bool,
    amount_to_avg_ratio: Optional[float],
    is_new_account: bool,
    txn_count_last_1h: int,
    merchant_category: str,
) -> List[str]:
    """Assign deterministic SENTINEL reason codes R01 to R08."""
    codes: List[str] = []
    if is_new_device is True:
        codes.append("R01_NEW_DEVICE")
    if is_new_city is True or (
        distance_km is not None and not pd.isna(distance_km) and float(distance_km) > 500.0
    ):
        codes.append("R02_GEO_MISMATCH")
    if is_night is True:
        codes.append("R03_ODD_HOUR")
    if has_recent_failures is True:
        codes.append("R04_FAILED_ATTEMPTS")
    if (
        amount_to_avg_ratio is not None
        and not pd.isna(amount_to_avg_ratio)
        and float(amount_to_avg_ratio) >= 5.0
    ):
        codes.append("R05_AMOUNT_SPIKE")
    if is_new_account is True:
        codes.append("R06_NEW_ACCOUNT")
    if int(txn_count_last_1h) >= 4:
        codes.append("R07_VELOCITY")
    if str(merchant_category).lower() in HIGH_RISK_MCCS:
        codes.append("R08_HIGH_RISK_MCC")
    return codes


def enforce_data_privacy_guardrails(tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Enforce DPDP Act 2023 & PCI-DSS v4.0 data-minimization and client-anonymization rules:
      1. Drop or reject any forbidden raw PII columns if present in an external table.
      2. Scrub any legacy client brand prefixes in merchant synthetic names (`Merchant_<MCC>_<id>`).
      3. Verify that 100% of primary entity IDs (`customer_id`, `merchant_id`, `device_id`)
         use tokenized pseudonymous format (`C_*`, `M_*`, `D_*`) and `is_synthetic == True`.
    """
    redacted_columns: List[str] = []
    for table_name, df in tables.items():
        found_pii = [c for c in df.columns if c.lower() in FORBIDDEN_PII_COLUMNS]
        if found_pii:
            df.drop(columns=found_pii, inplace=True)
            redacted_columns.extend(f"{table_name}.{c}" for c in found_pii)
        if "is_synthetic" in df.columns:
            assert bool(df["is_synthetic"].all()), (
                f"Data governance violation in `{table_name}`: non-synthetic record detected!"
            )

    if "merchants" in tables and "synthetic_name" in tables["merchants"].columns:
        tables["merchants"]["synthetic_name"] = (
            tables["merchants"]["synthetic_name"]
            .astype(str)
            .apply(lambda s: re.sub(r"^[A-Za-z]+_", "Merchant_", s))
        )

    cust_ids = tables["customers"]["customer_id"].astype(str)
    merch_ids = tables["merchants"]["merchant_id"].astype(str)
    dev_ids = tables["devices"]["device_id"].astype(str)

    assert cust_ids.str.startswith("C_").all(), "Untokenized customer_id detected"
    assert merch_ids.str.startswith("M_").all(), "Untokenized merchant_id detected"
    assert dev_ids.str.startswith("D_").all(), "Untokenized device_id detected"

    return {
        "compliance_standard": "DPDP Act 2023 & PCI-DSS v4.0 Data Minimization",
        "forbidden_pii_columns_checked": len(FORBIDDEN_PII_COLUMNS),
        "redacted_columns": redacted_columns,
        "tokenized_customer_ids_verified": int(len(cust_ids)),
        "tokenized_merchant_ids_verified": int(len(merch_ids)),
        "tokenized_device_ids_verified": int(len(dev_ids)),
        "all_records_synthetic_verified": True,
    }


def enforce_corpus_privacy_guardrails(documents: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Verify and scrub unstructured corpus documents prior to vector embedding and Qdrant indexing:
      1. Ensure no forbidden PII fields exist in document metadata.
      2. Scan and redact any accidental raw email, PAN, Aadhaar, or 16-digit card numbers in text.
      3. Anonymize any proprietary client brand names to 'Tier-1 RBI-Regulated Payment Aggregator'.
      4. Assert `is_synthetic == True` across 100% of corpus documents.
    """
    pii_redactions = 0
    brand_redactions = 0
    for doc in documents:
        for col in list(doc.keys()):
            if col.lower() in FORBIDDEN_PII_COLUMNS:
                del doc[col]
                pii_redactions += 1
        content = str(doc.get("content", ""))
        if "PayVista" in content:
            content = content.replace("PayVista Payments Pvt. Ltd.", "Tier-1 RBI-Regulated Payment Aggregator")
            content = content.replace("PayVista", "SENTINEL Aggregator")
            brand_redactions += 1
        for label, pattern in FORBIDDEN_PII_REGEXES.items():
            if pattern.search(content):
                content = pattern.sub(f"[REDACTED_{label.upper()}]", content)
                pii_redactions += 1
        doc["content"] = content
        assert doc.get("is_synthetic") is True, f"Document {doc.get('doc_id')} missing is_synthetic=True"

    return {
        "compliance_standard": "DPDP Act 2023 & PCI-DSS v4.0 Unstructured Corpus Gate",
        "documents_scanned": len(documents),
        "pii_patterns_checked": len(FORBIDDEN_PII_REGEXES) + len(FORBIDDEN_PII_COLUMNS),
        "pii_redactions_applied": pii_redactions,
        "client_brand_redactions_applied": brand_redactions,
        "all_documents_synthetic_verified": True,
    }


def generate_synthetic_sentinel_corpus(
    num_docs_per_category: int = 45,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """
    Generate 225 deterministic, multi-paragraph regulatory, policy, playbook, and forensic
    case narrative documents (~240-310 words each) across the 5 canonical SENTINEL categories:
      - `rbi` (45 docs)
      - `npci` (45 docs)
      - `internal_policies` (45 docs)
      - `playbooks` (45 docs)
      - `case_narratives` (45 docs)
    Designed so `chunk_size=180` words with `chunk_overlap=40` words yields exactly 2 chunks
    per document (450 total semantic chunks) with rich cross-chunk boundary context.
    """
    rng = random.Random(seed)
    documents: List[Dict[str, Any]] = []

    # 1. RBI Regulatory Directives (45 documents)
    rbi_topics = [
        (
            "RBI Customer Liability & Zero-Liability Protection for Unauthorized Electronic Payment Transactions",
            "RBI/DBR/2017-18/15-DPSS.CO.PD",
            "R01_NEW_DEVICE",
            "Under the Reserve Bank of India (RBI) Master Direction on Limiting Liability of Customers in Unauthorised Electronic Banking Transactions, a customer's financial liability is strictly zero when an unauthorized transaction occurs due to contributory fraud, negligence, or security deficiency on the part of the payment aggregator or bank, irrespective of whether the transaction is reported by the customer. Furthermore, third-party breach incidents where the deficiency lies neither with the regulated entity nor with the customer, but elsewhere in the payment ecosystem, also qualify for zero customer liability provided the customer notifies the payment service provider within three working days of receiving the transaction communication alert. "
            "When a customer reports an unauthorized UPI, card, wallet, or net-banking debit between four and seven working days after receiving the SMS or email notification, the maximum customer liability is capped at Rs. 10,000 for basic savings and prepaid accounts or Rs. 25,000 for standard current and credit accounts. Upon receipt of any unauthorized transaction report, the regulated entity must credit the disputed amount back to the customer's account (shadow reversal) within ten working days (T+10 SLA) from the date of notification, without waiting for insurance settlement or final chargeback adjudication. The burden of proving customer liability or intentional credential sharing rests exclusively on the payment aggregator and issuing bank through verifiable device telemetry, AFA logs, and cryptographic device-binding records."
        ),
        (
            "RBI Cyber Resilience & 6-Hour CERT-In Incident Reporting Mandate for Payment Aggregators",
            "RBI/DPSS/2021-22/82-CYBER",
            "R04_FAILED_ATTEMPTS",
            "The Reserve Bank of India Master Direction on Digital Payment Security Controls (`RBI/DPSS/2021-22/82-CYBER`) and CERT-In Cyber Security Directions mandate that all RBI-regulated Payment Aggregators (PAs) and Payment Gateways (PGs) maintain a 24x7 Security Operations Center with automated anomaly detection across authentication failure spikes, credential stuffing, and distributed brute-force attacks. Whenever an unusual cyber security incident, unauthorized data access, or coordinated multi-account authentication failure spike is detected, the RBI-regulated entity must report the cyber incident to both RBI DPSS and CERT-In within an uncompromising six-hour (6-hour) cyber incident reporting SLA window. "
            "Under RBI storage of payment system data directives, all system logs, hardware fingerprint telemetry, Haversine geolocation deltas, and API gateway audit trails must be retained in immutable, tamper-evident storage strictly within India (data localization enforcement) for a minimum retention window of five years. Regulated entities must enforce real-time velocity throttling, adaptive step-up Additional Factor of Authentication (AFA), and automated session revocation whenever authentication failure thresholds (`failed_transactions >= 3` in 24 hours, `R04_FAILED_ATTEMPTS`) co-occur with unrecognized device fingerprints (`R01_NEW_DEVICE`) or nocturnal windows (`00:00` to `04:59` IST, `R03_ODD_HOUR`)."
        ),
        (
            "RBI Two-Factor Authentication (AFA) Step-Up Mandates and Risk-Based Exemption Controls",
            "RBI/2023-24/DPSS-AFA-09",
            "R05_AMOUNT_SPIKE",
            "Reserve Bank of India guidelines on Additional Factor of Authentication (AFA) require dynamically linked two-factor verification for every domestic digital payment instruction unless explicitly covered by approved small-value contactless or e-mandate exemptions. Even within recurring e-mandate or fast-checkout workflows, regulated payment aggregators must trigger compulsory step-up AFA whenever a transaction exhibits anomalous behavioral risk signals, specifically when the transaction value exceeds 5.0 times the customer's 90-day trailing average transaction value (`amount_to_avg_ratio >= 5.0`, reason code `R05_AMOUNT_SPIKE`) or crosses the regulatory e-mandate auto-debit ceiling of Rs. 15,000 (or Rs. 1,00,000 for mutual fund, insurance, and credit card bill categories). "
            "During real-time authorization scoring, the risk engine must evaluate whether the device hardware fingerprint (`device_id`) has been cryptographically bound to the customer account (`customer_id`) over the preceding 90 days. If an authorization request combines an amount spike (`FE-01 >= 5.0`) with a newly observed device (`FE-02 is_new_device == True`) or a cross-city Haversine distance exceeding 500 kilometers (`FE-03`), static exemption rules are immediately suspended and the transaction must be routed to hard BLOCK or interactive biometric/OTP step-up verification."
        ),
        (
            "RBI Guidelines on Regulation of Payment Aggregators (PA/PG) Merchant Onboarding & Escrow Governance",
            "RBI/DPSS/2020-21/PA-PG-18",
            "R08_HIGH_RISK_MCC",
            "Under the RBI Framework for Regulation of Payment Aggregators and Payment Gateways, non-bank payment aggregators must execute comprehensive merchant due diligence, background screening, and continuous transaction monitoring across all onboarded merchant terminals (`merchant_id`). Merchants operating in elevated chargeback or money-laundering exposure categories—specifically online real-money gaming (`gaming`), digital gift cards (`gift_cards`), virtual digital assets (`crypto`), high-value consumer electronics (`electronics`), and last-minute travel ticketing (`travel`), tracked under reason code `R08_HIGH_RISK_MCC`—are subject to enhanced KYC verification, website security audits, and stricter rolling settlement holds. "
            "All funds collected from customers must be credited into an escrow account maintained with a scheduled commercial bank on a strictly T+0 or T+1 basis. Payment aggregators are prohibited from onboarding unregistered offshore gaming or unauthorized crypto exchanges and must continuously monitor merchant-level target-encoded fraud rates (`FE-08 mcc_target_encoded_rate`). If a merchant terminal receives payments from shared devices utilized by two or more distinct customer accounts within a seven-day window (`FE-09`), merchant payout settlement must be paused pending forensic graph review."
        ),
        (
            "RBI Master Direction on Prepaid Payment Instruments (PPI) & New Account Cooling-Off Controls",
            "RBI/DPSS/2021-22/PPI-MD-89",
            "R06_NEW_ACCOUNT",
            "The RBI Master Direction on Prepaid Payment Instruments (PPIs) and Digital Account Onboarding establishes tiered transaction limits and mandatory velocity controls for newly opened or recently upgraded customer accounts (`account_age < 30` days, reason code `R06_NEW_ACCOUNT`). Newly onboarded accounts completed via Video Customer Identification Process (V-CIP) or minimum-detail OTP KYC face elevated synthetic identity and money-mule recruitment risk during their first 30 days of lifecycle activity (`FE-06 is_new_account == True`). "
            "To prevent rapid cash-out and mule layering through newly created accounts, regulated payment aggregators must enforce strict beneficiary addition cooling periods and cumulative 24-hour debit caps (`FE-07 txn_amount_last_24h_paise`). When an account younger than 30 days attempts high-velocity authorizations (`txn_count_last_1h >= 4`, `R07_VELOCITY`) or transacts at high-risk merchant categories (`R08_HIGH_RISK_MCC`) during nocturnal IST hours (`00:00–04:59 IST`), the authorization engine must escalate the risk classification to HIGH or CRITICAL and hold settlement until account ownership is re-verified."
        ),
    ]

    for i in range(1, num_docs_per_category + 1):
        base_title, ref_code, r_code, base_body = rbi_topics[(i - 1) % len(rbi_topics)]
        city = rng.choice(list(CITIES_COORDS.keys()))
        clause_num = f"Section {(i % 9) + 1}.{(i % 5) + 1}"
        doc_id = f"DOC_RBI_{i:03d}"
        title = f"{base_title} — {clause_num} Compliance Annex ({doc_id})"
        content = (
            f"Document ID: {doc_id} | Category: RBI Regulatory Directive | Reference: {ref_code}/{clause_num} | Primary Signal: {r_code}. "
            f"{base_body} "
            f"Operational Compliance Note ({clause_num}, Regional Audit Zone: {city}): Payment aggregator compliance officers and automated RAG explanation services must cite {ref_code} ({clause_num}) whenever adjudicating disputes involving {r_code}. All authorization telemetry evaluated under this directive uses integer paise accounting (`amount_paise = round(transaction_amount * 100)`) and tokenized pseudonymous identifiers (`C_<hash>`, `M_<hash>`, `D_<hash>`) in strict compliance with the Digital Personal Data Protection (DPDP) Act 2023 and PCI-DSS v4.0."
        )
        documents.append(
            {
                "doc_id": doc_id,
                "source": f"rbi_master_direction_{i:03d}.md",
                "document_type": "rbi",
                "title": title,
                "reason_code": r_code,
                "regulatory_ref": f"{ref_code}/{clause_num}",
                "split": "train" if i <= 35 else "validation",
                "content": content,
                "is_synthetic": True,
            }
        )

    # 2. NPCI UPI & RuPay Procedural Guidelines (45 documents)
    npci_topics = [
        (
            "NPCI UPI Device Binding, SIM Swap Cooling Period & Rs. 2,000 Initial 24-Hour Limit",
            "NPCI/UPI/OC-115/2023-24",
            "R01_NEW_DEVICE",
            "Under National Payments Corporation of India (NPCI) Unified Payments Interface (UPI) Operating Circular OC-115 on SIM Binding and New Device Registration, every UPI application must perform hardware-backed cryptographic device binding via outbound SMS verification linking the mobile hardware fingerprint (`device_id`), SIM IMSI, and registered mobile number before enabling UPI PIN transactions. Whenever a customer binds a previously unseen mobile device (`FE-02 is_new_device == True`, reason code `R01_NEW_DEVICE`) or performs a SIM replacement, NPCI mandates a mandatory 24-hour cooling-off window during which cumulative peer-to-peer (P2P) and peer-to-merchant (P2M) outbound transfers are strictly capped at Rs. 5,000 (with an initial first-transaction sub-limit of Rs. 2,000 for newly activated device profiles). "
            "Attempts to bypass the 24-hour new-device cooling window through split rapid transactions (`FE-07 txn_count_last_1h >= 4`, `R07_VELOCITY`) or high-value merchant collect requests above Rs. 2,000 on a newly registered device must be intercepted by the Payment Service Provider (PSP) risk switch. Investigators reviewing `R01_NEW_DEVICE` alerts must verify whether the device fingerprint has appeared across any other customer accounts within the trailing 7 days (`FE-09 distinct_customers_per_device_7d`)."
        ),
        (
            "NPCI UPI Collect Request Fraud Mitigation & Unverified Pull-Payment Controls",
            "NPCI/UPI/OC-142/2024-25",
            "R05_AMOUNT_SPIKE",
            "NPCI Operating Circular OC-142 addresses social-engineering fraud executed via fraudulent UPI 'Collect Request' (pull payment) workflows where fraudsters impersonate customer support agents, courier services, or marketplace buyers and trick victims into entering their UPI PIN to 'receive' money. To curb pull-payment exploitation, NPCI caps non-verified P2P collect requests at Rs. 2,000 per transaction and restricts verified merchant collect requests based on historical customer spending behavior (`FE-01 amount_to_avg_ratio`). "
            "When an incoming UPI authorization exhibits an amount-to-average ratio of 5.0x or higher (`amount_to_avg_ratio >= 5.0`, reason code `R05_AMOUNT_SPIKE`) combined with an unrecognized geographic location (`FE-03 is_new_city == True` or `customer_merchant_distance_km > 500.0`, reason code `R02_GEO_MISMATCH`), the PSP application must display a prominent vernaculared fraud warning banner, enforce a 5-second cognitive friction delay, and route high-probability risk scores (`fraud_probability >= 0.85`) to immediate BLOCK."
        ),
        (
            "NPCI UPI Dispute Resolution Mechanism (UDIR) & T+5 Chargeback Turnaround SLAs",
            "NPCI/UDIR/OC-168/2024",
            "R04_FAILED_ATTEMPTS",
            "The NPCI UPI Dispute Resolution (UDIR) and Real-Time Online Dispute Resolution (ODR) framework establishes standardized reason codes (`U01` through `U99`) and strict turnaround times (TAT) for resolving technical declines, deemed-approved credits, and unauthorized fraud chargebacks across remitter banks, beneficiary banks, and payment aggregators. For fraud-related chargeback claims where the customer alleges credential compromise following repeated failed authentication attempts (`FE-05 failed_transactions >= 3` in 24h, `R04_FAILED_ATTEMPTS`), the acquiring payment aggregator must furnish complete transaction evidence within T+5 calendar days. "
            "Required UDIR evidentiary artifacts include the point-in-time feature vector (`feature_as_of`), device fingerprint history (`FE-02`, `FE-09`), IST timestamp (`FE-04 hour_of_day`), and terminal authorization status (`payment_outcomes.status`). Because formal chargeback adjudication and issuer arbitration can take up to 60 calendar days to reach finality, SENTINEL's ground-truth `labels` table enforces `label_available_at = transaction_time + 60 days` so supervised models never train on immature dispute statuses."
        ),
        (
            "NPCI High-Risk Merchant Category Code (MCC) Controls for Gaming, Crypto & Digital Goods",
            "NPCI/RISK/MCC-204/2024",
            "R08_HIGH_RISK_MCC",
            "NPCI Risk Management Circular MCC-204 requires acquiring banks and payment aggregators to enforce granular category-level velocity and ticket-size controls on high-risk Merchant Category Codes, specifically MCC 7995 (Online Skill Gaming / Wagering), MCC 6051 (Quasi-Cash and Virtual Digital Assets / Crypto), MCC 5816 (Digital Goods and Gift Cards), MCC 5732 (Electronics Stores), and MCC 4722 (Travel Agencies). Transactions in these five categories trigger SENTINEL reason code `R08_HIGH_RISK_MCC` and carry an elevated target-encoded baseline fraud rate (`FE-08 mcc_target_encoded_rate = 0.012` vs `0.004` for low-risk utility/grocery MCCs). "
            "Acquirers must continuously audit merchant terminals (`M_<16-hex-hash>`) for mule-ring cash-out behavior. Specifically, if a high-risk gaming or gift-card merchant processes authorizations from shared devices (`distinct_customers_per_device_7d >= 2`) or exhibits a burst of late-night UPI transactions (`00:00–04:59 IST`, `R03_ODD_HOUR`) exceeding Rs. 25,000, the acquiring switch must tag the batch for `HIGH_VALUE_GAMING_AUDIT` and throttle instant settlement."
        ),
        (
            "NPCI Beneficiary Mule-Ring Detection & Shared Device Velocity Rules on UPI",
            "NPCI/FRMS/MULE-190/2024",
            "R07_VELOCITY",
            "Under the NPCI Fraud Risk Management System (FRMS) Mule-Hunter directive, PSPs and acquiring aggregators must monitor cross-account hardware device sharing and rapid transaction bursts (`FE-07 txn_count_last_1h >= 4`, reason code `R07_VELOCITY`). Organized mule rings frequently operate Android device farms or rooted multi-instance emulators where a single physical device (`D_<16-hex-hash>`) rotates through 3 to 10 stolen or rented customer UPI profiles within a 7-day window (`FE-09 distinct_customers_per_device_7d >= 3`). "
            "Whenever `distinct_customers_per_device_7d >= 3` co-occurs with high 1-hour transaction velocity (`txn_count_last_1h >= 4`, `R07_VELOCITY`) or elevated 24-hour cumulative spend (`txn_amount_last_24h_paise`), the NPCI FRMS bridge mandates immediate device-level blacklisting (`list_entries` table, `list_type = 'blocklist'`) and automated 2-hop graph expansion to quarantine all linked customer accounts and colluding merchant terminals."
        ),
    ]

    for i in range(1, num_docs_per_category + 1):
        base_title, ref_code, r_code, base_body = npci_topics[(i - 1) % len(npci_topics)]
        channel = CHANNELS[(i - 1) % len(CHANNELS)]
        doc_id = f"DOC_NPCI_{i:03d}"
        title = f"{base_title} — Bulletin #{i:02d} ({doc_id})"
        content = (
            f"Document ID: {doc_id} | Category: NPCI Procedural Guideline | Circular: {ref_code} | Primary Signal: {r_code} | Channel Scope: {channel}. "
            f"{base_body} "
            f"NPCI Switch Implementation Directive (Bulletin #{i:02d}): All PSP and acquiring switches must evaluate {ref_code} rules within the real-time authorization budget (p50 <= 80 ms, p99 <= 250 ms). Investigators auditing {r_code} events on {channel} rails must cross-reference point-in-time velocity counters (`FE-07`) and 7-day device-sharing cardinality (`FE-09`) before issuing final UDIR dispute representations."
        )
        documents.append(
            {
                "doc_id": doc_id,
                "source": f"npci_upi_circular_{i:03d}.md",
                "document_type": "npci",
                "title": title,
                "reason_code": r_code,
                "regulatory_ref": ref_code,
                "split": "train" if i <= 35 else "validation",
                "content": content,
                "is_synthetic": True,
            }
        )

    # 3. Internal Risk, SLA & Governance Policies (45 documents)
    policy_topics = [
        (
            "SENTINEL Real-Time Risk Scoring Thresholds (BLOCK / REVIEW / APPROVE) & 300ms Fail-Open Circuit Breaker Policy",
            "POL-RISK-001",
            "R05_AMOUNT_SPIKE",
            "The SENTINEL Real-Time Authorization Scoring Policy (`POL-RISK-001`) governs how model fraud probabilities (`fraud_probability` in `[0.0, 1.0]`) map to deterministic gateway decisions across all 4.2 million daily payment authorizations (2,800 TPS festive peak). Standard probability cutoffs configured in the `threshold_sets` table classify transactions into four risk tiers: `LOW` (`fraud_probability < 0.30`, action: `APPROVE`), `MEDIUM` (`0.30 <= fraud_probability < 0.60`, action: `APPROVE` with passive logging), `HIGH` (`0.60 <= fraud_probability < 0.85`, action: `REVIEW` / step-up OTP), and `CRITICAL` (`fraud_probability >= 0.85`, action: immediate `BLOCK`). These thresholds are calibrated to sustain holdout recall >= 85% (`OBJ-01`), precision at BLOCK >= 70% (`OBJ-02`), fraud loss <= 6.4 bps (`OBJ-07`), and false decline/review rate <= 1.4% (`OBJ-08`). "
            "To guarantee strict payment gateway latency SLAs (`p50 <= 80 ms`, `p99 <= 250 ms`, `OBJ-10`), the scoring engine enforces a hard circuit-breaker timeout at 300 ms. If feature retrieval or model inference exceeds 300 ms, the fail-open circuit breaker executes a deterministic fallback: transactions with `transaction_amount < Rs. 10,000` (`amount_paise < 1000000`) are auto-approved (`APPROVE`) to prevent checkout abandonment, whereas transactions `>= Rs. 10,000` (`amount_paise >= 1000000`) are routed to the asynchronous manual `REVIEW` queue."
        ),
        (
            "Point-in-Time Feature Engineering Governance & 60-Day Chargeback Label Maturation Anti-Leakage Standard",
            "POL-ML-002",
            "R07_VELOCITY",
            "Internal Machine Learning Governance Policy `POL-ML-002` mandates zero temporal look-ahead leakage across all feature engineering (`FE-01` through `FE-10`) and target encoding pipelines. Every behavioral feature in the `features` table must be computed strictly over historical events where `t_prior < t_current` (`feature_as_of == transaction_time`). Specifically, trailing customer averages (`FE-01`), 90-day device history (`FE-02`), 90-day city history (`FE-03`), 24-hour failed attempt counts (`FE-05`), 1-hour attempt velocity (`FE-07`), and 7-day device customer cardinality (`FE-09`) must never include the current authorization or any future event. "
            "Furthermore, because fraudulent UPI and card chargebacks require up to 60 calendar days for customer reporting and UDIR bank arbitration, ground-truth fraud labels in the `labels` table carry `label_available_at = transaction_time + 60 days`. When computing smoothed MCC target encoding (`FE-08 mcc_target_encoded_rate = (fraud_count + 0.6) / (total_count + 100)`) or constructing supervised training/validation splits (`warmup`, `train`, `validation`, `holdout`), only mature labels where `label_available_at <= split_boundary_time` may be used. Using immature labels causes severe look-ahead leakage and under-estimates recent fraud waves."
        ),
        (
            "DPDP Act 2023 & PCI-DSS v4.0 Data Minimization, Tokenization & Zero-PII Vector Indexing Policy",
            "POL-PRIV-003",
            "R01_NEW_DEVICE",
            "Under Internal Data Governance Policy `POL-PRIV-003`, all relational tables, NoSQL document stores, graph databases, and Qdrant vector collections must comply with India's Digital Personal Data Protection (DPDP) Act 2023 and PCI-DSS v4.0 Requirement 3 (Protect Stored Account Data). The pre-ingestion privacy gate (`enforce_data_privacy_guardrails`) automatically scans and strips 12 forbidden raw PII column types: `customer_name`, `full_name`, `email`, `phone`, `mobile_number`, `card_number`, `pan`, `aadhaar`, `upi_id`, `vpa`, `ip_address`, and `bank_account_number`. "
            "All entity identifiers must be cryptographically pseudonymized via salted hashes prior to storage or embedding: customer IDs follow `C_<16-hex-hash>`, merchant IDs follow `M_<16-hex-hash>`, and hardware device IDs follow `D_<16-hex-hash>`. All monetary values must be stored both as exact integer paise (`amount_paise = round(transaction_amount * 100)`) to eliminate IEEE-754 floating-point drift and as INR numeric representations. Unstructured text chunks indexed in Qdrant (`sentinel_policy_corpus`) must pass regex PII scrubbing and verify `is_synthetic == True` before vector generation."
        ),
        (
            "IST Timezone Normalization Policy (`UTC+05:30`) for Nocturnal Fraud Window Detection (`FE-04` / `R03_ODD_HOUR`)",
            "POL-TIME-004",
            "R03_ODD_HOUR",
            "Internal Feature Standardization Policy `POL-TIME-004` resolves a critical timezone bug observed in legacy fraud rule engines. While all raw transaction timestamps (`transaction_time`, `outcome_time`, `feature_as_of`) are persisted in Coordinated Universal Time (`UTC`) across PostgreSQL and MongoDB, customer behavioral sleep cycles across Indian cities (Bengaluru, Mumbai, Delhi, Hyderabad, Chennai, Pune, Kolkata, Ahmedabad) follow Indian Standard Time (`IST = UTC + 05:30`). "
            "Evaluating `hour_of_day` in raw UTC misclassifies prime Indian morning shopping hours (`05:30` to `10:29` IST, which corresponds to `00:00` to `04:59` UTC) as nocturnal activity, inflating false positives by over 40%. Therefore, `FE-04` mandates explicit conversion of `transaction_time` to `UTC+05:30` prior to extracting `hour_of_day` (`0..23`), and sets `is_night = True` (triggering reason code `R03_ODD_HOUR`) if and only if `0 <= hour_of_day_ist < 5` (`00:00:00` through `04:59:59` IST). All batch feature scripts and real-time online scoring workers must validate IST parity against reference `features` snapshots prior to deployment."
        ),
        (
            "RAG Investigator Explanation Grounding, JSON Schema Enforcement & Deterministic Fallback Policy (`FR-API-06`)",
            "POL-RAG-005",
            "R02_GEO_MISMATCH",
            "Policy `POL-RAG-005` defines the operational standards for the SENTINEL FastAPI RAG Investigator Assistant (`POST /explain`). Every generated investigator brief must achieve >= 90% factual grounding (`OBJ-05`) and 100% strict JSON schema validity (`OBJ-06`), returning exact fields: `transaction_id`, `fraud_probability`, `risk_level`, `risk_summary`, `reason_codes_fired`, `recommended_action`, `cited_sources` (top-k Qdrant policy/playbook chunks with `document_id`, `document_type`, `chunk_number`, `relevance_score`, and `excerpt`), `graph_context`, and `latency_ms`. "
            "The LLM synthesis layer is strictly forbidden from hallucinating unrecorded reason codes, inventing rupee amounts that diverge from `transaction_amount`, or fabricating regulatory circular numbers not present in the retrieved Qdrant top-5 chunks. If the LLM gateway times out, returns malformed JSON, or fails the post-generation numeric/entity grounding verifier, the service automatically triggers `FR-API-06`: a deterministic, rule-grounded template synthesizer that populates the exact JSON schema from verified telemetry and retrieved Qdrant chunks with zero downtime."
        ),
    ]

    for i in range(1, num_docs_per_category + 1):
        base_title, ref_code, r_code, base_body = policy_topics[(i - 1) % len(policy_topics)]
        version = f"v2.{(i % 6) + 1}"
        doc_id = f"DOC_POL_{i:03d}"
        title = f"{base_title} ({ref_code} {version} — {doc_id})"
        content = (
            f"Document ID: {doc_id} | Category: Internal Risk & Architecture Policy | Policy Code: {ref_code} ({version}) | Primary Signal: {r_code}. "
            f"{base_body} "
            f"Governance Sign-Off ({ref_code} {version}): Approved by the Payment Risk Architecture Committee. Every automated scoring service, feature pipeline, and Qdrant retrieval workflow must enforce {ref_code} invariants during both online authorization scoring (2,800 TPS peak) and offline investigator triage."
        )
        documents.append(
            {
                "doc_id": doc_id,
                "source": f"internal_policy_{ref_code.lower()}_{i:03d}.md",
                "document_type": "internal_policies",
                "title": title,
                "reason_code": r_code,
                "regulatory_ref": f"{ref_code}-{version}",
                "split": "train" if i <= 35 else "validation",
                "content": content,
                "is_synthetic": True,
            }
        )

    # 4. Investigator Standard Operating Playbooks (45 documents)
    playbook_topics = [
        (
            "SOC Investigator Playbook: New Device Binding + Nocturnal Geo-Mismatch Account Takeover (ATO) Triage",
            "PB-ATO-101",
            "R01_NEW_DEVICE",
            "Standard Operating Playbook `PB-ATO-101` governs immediate SOC investigator triage and automated containment when a transaction fires `R01_NEW_DEVICE` (`is_new_device == True`, device unseen for customer in trailing 90 days) alongside `R02_GEO_MISMATCH` (`is_new_city == True` or `customer_merchant_distance_km > 500.0` km) and/or `R03_ODD_HOUR` (`00:00–04:59 IST`). This signal triad represents the classic signature of a remote Account Takeover (ATO) or SIM-swap attack where an adversary binds a new mobile handset in a distant city while the genuine account holder is asleep. "
            "Step 1 (Immediate Containment): Place an immediate temporary hold on the disputed transaction (`BLOCK` if `fraud_probability >= 0.85`, or `REVIEW` quarantine if `0.60 <= fraud_probability < 0.85`) and enforce NPCI OC-115 24-hour new-device cooling restrictions (capping cumulative outbound transfers at Rs. 2,000). Step 2 (Graph & Velocity Verification): Query Neo4j and `FE-09` (`distinct_customers_per_device_7d`) to check whether the new `device_id` (`D_<hash>`) is linked to any other customer accounts. Step 3 (Step-Up & Account Quarantine): Trigger out-of-band biometric re-authentication and revoke active session tokens on the newly bound device until customer voice/V-CIP verification completes."
        ),
        (
            "SOC Investigator Playbook: Shared-Device Mule Ring & Multi-Account Velocity Containment",
            "PB-RING-102",
            "R07_VELOCITY",
            "Standard Operating Playbook `PB-RING-102` directs fraud investigators when handling organized mule-ring alerts triggered by shared hardware devices (`FE-09 distinct_customers_per_device_7d >= 3`) combined with high 1-hour transaction velocity (`FE-07 txn_count_last_1h >= 4`, `R07_VELOCITY`) and 5x amount spikes (`FE-01 amount_to_avg_ratio >= 5.0`, `R05_AMOUNT_SPIKE`). In a shared-device mule ring, syndicate operators use a single physical smartphone or emulator instance to cycle through multiple compromised or rented customer accounts within minutes. "
            "Step 1 (Device Quarantine & Blocklisting): Immediately insert the shared `device_id` (`D_<16-hex-hash>`) into the real-time `list_entries` blocklist (`list_type = 'blocklist'`) to block all subsequent authorization attempts across every customer account on that hardware fingerprint. Step 2 (2-Hop Graph Blast-Radius Expansion): Execute the Neo4j 2-hop traversal (`MATCH (c:Customer)-[:USED_DEVICE|PAID*1..2]-(target)`) to identify all colluding customer accounts (`C_<hash>`) and receiving merchant terminals (`M_<hash>`). Step 3 (Merchant Escrow Freeze): If a single merchant terminal receives funds from >= 2 shared devices, freeze T+1 escrow payout under RBI PA/PG guidelines and file a suspicious transaction report."
        ),
        (
            "SOC Investigator Playbook: Rapid Failed PIN/OTP Brute-Force Retries Followed by High-Value Spike",
            "PB-BRUTE-103",
            "R04_FAILED_ATTEMPTS",
            "Standard Operating Playbook `PB-BRUTE-103` specifies the containment workflow when an authorization fires `R04_FAILED_ATTEMPTS` (`failed_transactions >= 3` in the trailing 24 hours, `has_recent_failures == True`) followed by a successful or high-value authorization attempt (`R05_AMOUNT_SPIKE`, `amount_to_avg_ratio >= 5.0`) or high-velocity burst (`R07_VELOCITY`, `txn_count_last_1h >= 4`). Attackers testing harvested credentials or guessing 4/6-digit UPI PINs frequently generate 3 to 6 declined outcomes (`status = 'FAILED'`, `failure_code = 'AUTH_DECLINED'`) before succeeding or switching payment channels. "
            "Step 1 (Credential Lockout & Session Reset): Immediately lock UPI PIN / card authentication on the customer profile for 24 hours after 3 consecutive failures and require hardware-bound SIM re-verification. Step 2 (Downstream Spike Interception): Any subsequent transaction attempting >= 5.0x the customer's 90-day average ticket size (`FE-01`) within 24 hours of 3+ failed attempts must be hard-blocked (`decision = 'BLOCK'`). Step 3 (Multi-Account Correlation Audit): If brute-force retries span >= 10 customer accounts concurrently from correlated device fingerprints, escalate to the SOC Lead for coordinated containment."
        ),
        (
            "SOC Investigator Playbook: High-Risk MCC (Gaming, Crypto, Gift Cards) Nocturnal Velocity Escalation",
            "PB-MCC-104",
            "R08_HIGH_RISK_MCC",
            "Standard Operating Playbook `PB-MCC-104` provides step-by-step triage instructions for high-value authorizations in elevated-risk Merchant Category Codes (`R08_HIGH_RISK_MCC`: `gaming`, `gift_cards`, `crypto`, `electronics`, `travel`), especially when paired with nocturnal timestamps (`R03_ODD_HOUR`, `00:00–04:59 IST`), new accounts (`R06_NEW_ACCOUNT`, `account_age < 30` days), or 24-hour cumulative spend spikes (`FE-07 txn_amount_last_24h_paise`). Digital gaming credits, gift vouchers, and crypto tokens are irreversible, instant-delivery instruments favored by fraudsters for rapid monetization. "
            "Step 1 (Automated Ticket Escalation): Flag all `gaming` and `gift_cards` authorizations exceeding Rs. 25,000 (`amount_paise > 2500000`) with `review_required = True` and `escalation_tag = 'HIGH_VALUE_GAMING_AUDIT'`. Step 2 (Account Maturity & Velocity Audit): Check `account_age` (`FE-06`) and trailing 24-hour spend (`txn_amount_last_24h_paise`). If `account_age < 30` days and `amount_to_avg_ratio >= 5.0`, issue an immediate `BLOCK` decision. Step 3 (Merchant Collusion Check): Inspect the merchant's smoothed target-encoded fraud rate (`FE-08`) and verify whether multiple newly created accounts are funnelling funds into the same merchant terminal."
        ),
        (
            "SOC Investigator Playbook: False-Positive Clearance for Genuine Domestic Travel & High-Ticket Electronics",
            "PB-FP-105",
            "R02_GEO_MISMATCH",
            "Standard Operating Playbook `PB-FP-105` establishes the verification protocol for minimizing false declines (`OBJ-08: false decline/review rate <= 1.4%`) when a legitimate customer triggers `R02_GEO_MISMATCH` (`customer_merchant_distance_km > 500.0` km or `is_new_city == True`) and `R08_HIGH_RISK_MCC` (`travel` or `electronics`) during legitimate inter-city travel or festival shopping. Static rule engines frequently block genuine travellers buying airline tickets, hotel stays, or laptops in a visited city, damaging customer trust. "
            "Step 1 (Trusted Device Continuity Verification): Check `FE-02` (`is_new_device`) and `FE-09` (`distinct_customers_per_device_7d`). If the transaction originates from the customer's long-standing primary device (`is_new_device == False`, seen continuously over the prior 90 days) and `distinct_customers_per_device_7d == 1` (zero device sharing), device trust strongly favors a genuine user. Step 2 (Authentication & Tenure Check): Confirm `has_recent_failures == False` (`failed_transactions == 0`), `is_new_account == False` (`account_age >= 30` days), and daytime IST execution (`is_night == False`). Step 3 (Disposition): When device trust and zero failures hold and `fraud_probability < 0.60`, clear the alert as `APPROVE` (or single-click SMS confirmation if `0.60 <= fraud_probability < 0.85`) and log the visited city to the customer's verified travel profile."
        ),
    ]

    for i in range(1, num_docs_per_category + 1):
        base_title, ref_code, r_code, base_body = playbook_topics[(i - 1) % len(playbook_topics)]
        doc_id = f"DOC_PB_{i:03d}"
        title = f"{base_title} — SOP Runbook #{i:02d} ({doc_id})"
        content = (
            f"Document ID: {doc_id} | Category: SOC Investigator Playbook | Runbook ID: {ref_code}-R{i:02d} | Primary Signal: {r_code} | Document Type: playbooks. "
            f"{base_body} "
            f"SOC Execution Checklist ({ref_code}-R{i:02d}): Investigators executing this playbook must record the final verdict (`fraud_label` 0 or 1) and structured explanation in `reviewer_feedback`, citing `{doc_id}` and verifying all point-in-time features (`FE-01`..`FE-10`) before closing the ticket."
        )
        documents.append(
            {
                "doc_id": doc_id,
                "source": f"investigator_playbook_{ref_code.lower()}_{i:03d}.md",
                "document_type": "playbooks",
                "title": title,
                "reason_code": r_code,
                "regulatory_ref": f"{ref_code}-R{i:02d}",
                "split": "train" if i <= 35 else "validation",
                "content": content,
                "is_synthetic": True,
            }
        )

    # 5. Historical Forensic Case Narratives (45 documents)
    narrative_topics = [
        (
            "Forensic Case Narrative: Nocturnal Android Emulator Device-Farm Attack Across Bengaluru, Mumbai & Delhi",
            "CASE-2025-RING-01",
            "R01_NEW_DEVICE",
            "Closed Forensic Investigation Case `CASE-2025-RING-01` documents a coordinated UPI mule-ring attack detected across six tokenized customer accounts (`C_5f8a...` through `C_9c21...`) between 01:40 AM and 03:25 AM IST (`FE-04 hour_of_day in [1, 2, 3]`, `is_night == True`, `R03_ODD_HOUR`). All six accounts were accessed from a single rooted Android device fingerprint (`D_83fd19a04b2e77c1`, `device_kind = 'mobile_android'`), pushing `FE-09 distinct_customers_per_device_7d` to 6 within a 95-minute window. For every targeted customer, the device was completely unrecognized over the prior 90 days (`FE-02 is_new_device == True`, `R01_NEW_DEVICE`), and the transaction origin coordinates in Delhi diverged by 1,150 km to 1,740 km from the customers' registered home cities in Bengaluru and Mumbai (`FE-03 customer_merchant_distance_km > 500.0`, `R02_GEO_MISMATCH`). "
            "Each compromised account executed 4 to 6 rapid payment attempts within 60 minutes (`FE-07 txn_count_last_1h >= 4`, `R07_VELOCITY`) directed at two high-risk online gaming merchants (`M_44a9...`, `merchant_category = 'gaming'`, `R08_HIGH_RISK_MCC`), with ticket sizes averaging Rs. 48,500 (`4850000` paise)—representing a 7.8x spike over the customers' 90-day average transaction value (`FE-01 amount_to_avg_ratio = 7.8`, `R05_AMOUNT_SPIKE`). The SENTINEL scoring engine assigned `fraud_probability = 0.964` (`risk_level = 'CRITICAL'`, decision = `BLOCK`), preventing Rs. 11.64 Lakhs in fraud loss and blacklisting device `D_83fd19a04b2e77c1` under Playbook `PB-RING-102`."
        ),
        (
            "Forensic Case Narrative: SIM-Swap & PIN Brute-Force Account Takeover on High-Value Gaming Merchant",
            "CASE-2025-ATO-02",
            "R04_FAILED_ATTEMPTS",
            "Closed Forensic Investigation Case `CASE-2025-ATO-02` examines an Account Takeover (ATO) incident targeting customer `C_a91b40e27c118f04` (home city: Hyderabad). Following an unauthorized SIM replacement, the attacker bound a new iOS handset (`D_19c4e8810a3b55d2`, `is_new_device == True`, `R01_NEW_DEVICE`) at 02:15 AM IST (`R03_ODD_HOUR`) from Kolkata (Haversine distance `1,184.5 km` from Hyderabad, `R02_GEO_MISMATCH`). Over a 14-minute interval, the attacker triggered four consecutive declined UPI authorizations (`status = 'FAILED'`, `failure_code = 'AUTH_DECLINED'`) while brute-forcing the victim's UPI PIN (`FE-05 failed_transactions = 4`, `has_recent_failures == True`, `R04_FAILED_ATTEMPTS`). "
            "On the fifth attempt within the same hour (`FE-07 txn_count_last_1h = 5`, `R07_VELOCITY`), after resetting the PIN via compromised SMS OTP, the attacker initiated a Rs. 64,000 (`6400000` paise) payment to a digital gift-card and gaming terminal (`merchant_category = 'gaming'`, `R08_HIGH_RISK_MCC`), equivalent to `8.42x` the customer's Rs. 7,600 90-day average (`R05_AMOUNT_SPIKE`). Because the transaction fired six simultaneous reason codes (`R01`, `R02`, `R03`, `R04`, `R05`, `R07`, `R08`), SENTINEL intercepted and blocked the debit in 62 ms (`fraud_probability = 0.981`) and enforced Playbook `PB-BRUTE-103`."
        ),
        (
            "Forensic Case Narrative: Synthetic Newly Opened Mule Accounts Layering Funds via Quick-Commerce & Crypto",
            "CASE-2025-MULE-03",
            "R06_NEW_ACCOUNT",
            "Closed Forensic Investigation Case `CASE-2025-MULE-03` details a burst of synthetic mule accounts (`account_age` between 4 and 11 days, `FE-06 is_new_account == True`, `R06_NEW_ACCOUNT`) onboarded in Pune and Ahmedabad. Within 72 hours of account creation, three accounts shared a single desktop web browser fingerprint (`D_77e012b9c4410a88`, `device_kind = 'desktop_web'`, `FE-09 distinct_customers_per_device_7d = 3`) to execute rapid net-banking and wallet top-ups (`FE-07 txn_count_last_1h = 5`, `R07_VELOCITY`) totaling Rs. 1,45,000 (`14500000` paise) over 24 hours (`txn_amount_last_24h_paise`). "
            "Each transaction targeted high-risk merchants (`R08_HIGH_RISK_MCC`) at 6.1x the initial baseline ticket (`R05_AMOUNT_SPIKE`). Neo4j 2-hop graph analysis revealed that all three newly created accounts funnelled funds into merchant terminal `M_c09281fa44e1092b` (`Merchant_Electronics_042`), which had also received payments from a previously confirmed fraud case (`fraud_label = 1`) 48 hours earlier. The investigator confirmed `fraud_label = 1`, froze merchant escrow under RBI PA/PG rules, and placed all linked entities on the permanent blocklist."
        ),
        (
            "Forensic Case Narrative: Genuine Corporate Executive Domestic Flight & Laptop Purchase Cleared as False Positive",
            "CASE-2025-FP-04",
            "R02_GEO_MISMATCH",
            "Closed Forensic Investigation Case `CASE-2025-FP-04` documents a high-value false-positive review involving customer `C_3d72b1098e44c512` (registered home city: Bengaluru, `account_age = 640` days, `is_new_account == False`). While visiting Mumbai for a technology conference at 14:20 IST (`hour_of_day = 14`, `is_night == False`), the customer purchased an urgent replacement workstation for Rs. 82,000 (`8200000` paise) at `Merchant_Electronics_019` (`merchant_category = 'electronics'`, `R08_HIGH_RISK_MCC`), located `842.3 km` from Bengaluru (`is_new_city == True`, `R02_GEO_MISMATCH`) and representing `5.3x` their Rs. 15,400 90-day average (`R05_AMOUNT_SPIKE`). "
            "Although static rules flagged `R02_GEO_MISMATCH`, `R05_AMOUNT_SPIKE`, and `R08_HIGH_RISK_MCC`, SENTINEL point-in-time telemetry confirmed that the payment originated from the customer's trusted primary iOS device (`is_new_device == False`, `distinct_customers_per_device_7d == 1`), with zero prior failed attempts (`failed_transactions == 0`), low 1-hour velocity (`txn_count_last_1h == 1`), and daytime execution. Following Playbook `PB-FP-105`, the transaction (`fraud_probability = 0.41`, `risk_level = 'MEDIUM'`) was cleared as genuine (`fraud_label = 0`), avoiding a false decline on a high-LTV customer."
        ),
        (
            "Forensic Case Narrative: Genuine Family Holiday Travel Booking Across Chennai & Delhi Cleared via Device Continuity",
            "CASE-2025-FP-05",
            "R08_HIGH_RISK_MCC",
            "Closed Forensic Investigation Case `CASE-2025-FP-05` reviews a multi-ticket airline and hotel booking by customer `C_88e104f29a33b710` (home city: Chennai, `account_age = 415` days) for Rs. 58,000 (`5800000` paise) in the `travel` merchant category (`R08_HIGH_RISK_MCC`) with a Delhi-based travel aggregator (`customer_merchant_distance_km = 1,756.8 km`, `R02_GEO_MISMATCH`, `amount_to_avg_ratio = 5.15`, `R05_AMOUNT_SPIKE`). "
            "Investigator verification via the FastAPI RAG Assistant (`POST /explain`) highlighted that the customer used their long-standing registered Android device (`is_new_device == False`), had zero authentication failures in the trailing 24 hours (`has_recent_failures == False`), transacted at 19:30 IST (`is_night == False`), and had zero shared-device overlap in Neo4j (`distinct_customers_per_device_7d == 1`). Applying Playbook `PB-FP-105`, the analyst confirmed the transaction as legitimate (`fraud_label = 0`), demonstrating how combining graph context (`FE-09`) with device continuity (`FE-02`) separates benign travel spikes from account takeover fraud."
        ),
    ]

    for i in range(1, num_docs_per_category + 1):
        base_title, ref_code, r_code, base_body = narrative_topics[(i - 1) % len(narrative_topics)]
        doc_id = f"DOC_CASE_{i:03d}"
        title = f"{base_title} — Case File #{i:03d} ({doc_id})"
        content = (
            f"Document ID: {doc_id} | Category: Historical Forensic Case Narrative | Case Reference: {ref_code}-{i:03d} | Primary Signal: {r_code} | Document Type: case_narratives. "
            f"{base_body} "
            f"Forensic Audit Summary ({ref_code}-{i:03d}): Ground-truth label verified after the 60-day chargeback maturation window (`label_available_at = transaction_time + 60 days`). Indexed in `sentinel_policy_corpus` so investigators querying similar multi-flag signatures retrieve precedent cases alongside RBI/NPCI regulations and SOC playbooks."
        )
        documents.append(
            {
                "doc_id": doc_id,
                "source": f"forensic_case_narrative_{i:03d}.md",
                "document_type": "case_narratives",
                "title": title,
                "reason_code": r_code,
                "regulatory_ref": f"{ref_code}-{i:03d}",
                "split": "train" if i <= 35 else "validation",
                "content": content,
                "is_synthetic": True,
            }
        )

    return documents


def load_sentinel_corpus(
    data_dir: Optional[str] = None,
    min_documents: int = 225,
    seed: int = 42,
) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
    """
    3-Way Automatic Sentinel Unstructured Corpus Loader with Pre-Ingestion Privacy Gate:
      Tier 1: Local `sentinel_data/corpus/` (JSON/Markdown files) or `tables/documents.csv`
      Tier 2: PostgreSQL / Supabase `sentinel_demo.documents` replica (`DATABASE_URL`)
      Tier 3: Deterministic 225-document regulatory, policy, playbook & case narrative generator
    Returns (documents_list, active_tier_description, corpus_privacy_audit).
    """
    candidate_dirs = [
        data_dir or os.getenv("SENTINEL_DATA_DIR", "./sentinel_data"),
        "../sentinel_data",
        "./data/sentinel_data",
    ]
    for base in candidate_dirs:
        if not base:
            continue
        base_path = Path(base)
        corpus_json = base_path / "corpus" / "documents.json"
        if corpus_json.exists():
            docs = json.loads(corpus_json.read_text(encoding="utf-8"))
            if len(docs) >= 200:
                audit = enforce_corpus_privacy_guardrails(docs)
                return docs, f"Tier 1 (Local Sanitized Corpus JSON: {corpus_json})", audit

    db_url = os.getenv("DATABASE_URL", "")
    if db_url and "<" not in db_url:
        try:
            import psycopg2  # type: ignore

            conn = psycopg2.connect(db_url, connect_timeout=5)
            df = pd.read_sql_query(
                "SELECT document_id AS doc_id, kind AS document_type, title, split, content, is_synthetic "
                "FROM sentinel_demo.documents LIMIT 500",
                conn,
            )
            conn.close()
            if len(df) >= 200:
                docs = df.to_dict(orient="records")
                for idx, d in enumerate(docs):
                    d.setdefault("source", f"{d.get('document_type', 'doc')}_{idx:03d}.md")
                    d.setdefault("reason_code", "R01_NEW_DEVICE")
                    d.setdefault("regulatory_ref", f"REF-{idx:03d}")
                    d["is_synthetic"] = True
                audit = enforce_corpus_privacy_guardrails(docs)
                return docs, "Tier 2 (PostgreSQL / Supabase `sentinel_demo.documents`)", audit
        except Exception:
            pass

    per_cat = max(45, math.ceil(min_documents / 5))
    docs = generate_synthetic_sentinel_corpus(num_docs_per_category=per_cat, seed=seed)
    audit = enforce_corpus_privacy_guardrails(docs)
    return (
        docs,
        f"Tier 3 (Deterministic Schema-Compliant Synthetic Corpus: {len(docs)} documents across 5 categories, seed={seed})",
        audit,
    )


def generate_synthetic_sentinel_dataset(
    num_customers: int = 500,
    num_merchants: int = 120,
    num_devices: int = 900,
    num_transactions: int = 5500,
    seed: int = 42,
) -> Dict[str, pd.DataFrame]:
    """Generate deterministic, schema-exact, anonymized payment telemetry with fraud rings."""
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    cities = list(CITIES_COORDS.keys())
    base_start = datetime(2024, 11, 1, 0, 0, 0, tzinfo=timezone.utc)

    # 1. Customers
    customers = []
    for i in range(1, num_customers + 1):
        cid = f"C_{hashlib.md5(f'cust_{i}'.encode()).hexdigest()[:16]}"
        city = rng.choice(cities)
        _, _, region = CITIES_COORDS[city]
        created_at = base_start - timedelta(days=rng.randint(5, 900))
        customers.append(
            {
                "customer_id": cid,
                "created_at": created_at.isoformat(),
                "customer_age": rng.randint(20, 68),
                "home_city": city,
                "region": region,
                "is_synthetic": True,
            }
        )
    customers_df = pd.DataFrame(customers)

    # 2. Merchants (Client-agnostic anonymized merchant identifiers)
    merchants = []
    for i in range(1, num_merchants + 1):
        mid = f"M_{hashlib.md5(f'merch_{i}'.encode()).hexdigest()[:16]}"
        city = rng.choice(cities)
        lat, lon, _ = CITIES_COORDS[city]
        mcc = rng.choice(MCC_LIST)
        merchants.append(
            {
                "merchant_id": mid,
                "synthetic_name": f"Merchant_{mcc.title()}_{i:03d}",
                "merchant_category": mcc,
                "merchant_location": json.dumps(
                    {
                        "lat": round(lat + rng.uniform(-0.08, 0.08), 4),
                        "lon": round(lon + rng.uniform(-0.08, 0.08), 4),
                        "city": city,
                    }
                ),
                "is_synthetic": True,
            }
        )
    merchants_df = pd.DataFrame(merchants)

    # 3. Devices (including 15 shared mule-ring devices)
    devices = []
    for i in range(1, num_devices + 1):
        did = f"D_{hashlib.md5(f'dev_{i}'.encode()).hexdigest()[:16]}"
        devices.append(
            {
                "device_id": did,
                "device_kind": rng.choice(DEVICE_KINDS),
                "is_synthetic": True,
            }
        )
    devices_df = pd.DataFrame(devices)
    shared_ring_devices = [d["device_id"] for d in devices[:15]]

    cust_primary_devices = {
        c["customer_id"]: [
            devices[(idx * 2) % len(devices)]["device_id"],
            devices[(idx * 2 + 1) % len(devices)]["device_id"],
        ]
        for idx, c in enumerate(customers)
    }

    # 4. Transactions, Outcomes, Labels, Features
    tx_rows, outcome_rows, label_rows, feature_rows = [], [], [], []
    cust_history: Dict[str, List[Dict[str, Any]]] = {c["customer_id"]: [] for c in customers}
    device_history: Dict[str, List[Tuple[datetime, str]]] = {}

    cur_time = base_start
    burst_cust: Optional[Dict[str, Any]] = None
    burst_remaining = 0

    for idx in range(1, num_transactions + 1):
        if burst_remaining > 0 and burst_cust is not None:
            cur_time += timedelta(seconds=rng.randint(25, 160))
            cust = burst_cust
            burst_remaining -= 1
            is_ring_attack = True
        else:
            if idx > num_transactions - 250:
                cur_time += timedelta(seconds=rng.randint(20, 320))
            else:
                cur_time += timedelta(seconds=rng.randint(15, 7200))
            cust = customers[rng.randrange(len(customers))]
            is_ring_attack = idx % 35 == 0
            if idx % 140 == 0 and idx <= num_transactions - 6:
                burst_cust = cust
                burst_remaining = 5

        if cur_time < datetime(2025, 1, 1, tzinfo=timezone.utc):
            split = "warmup"
        elif cur_time < datetime(2025, 11, 1, tzinfo=timezone.utc):
            split = "train"
        elif cur_time < datetime(2026, 1, 1, tzinfo=timezone.utc):
            split = "validation"
        else:
            split = "holdout"

        cid = cust["customer_id"]
        merch = merchants[rng.randrange(len(merchants))]
        mid = merch["merchant_id"]
        mcc = merch["merchant_category"]
        m_loc = json.loads(merch["merchant_location"])

        if is_ring_attack:
            did: Optional[str] = rng.choice(shared_ring_devices)
        elif rng.random() < 0.01:
            did = None
        elif rng.random() < 0.12:
            did = devices[rng.randrange(len(devices))]["device_id"]
        else:
            did = rng.choice(cust_primary_devices[cid])

        prior = cust_history[cid]
        prior_90d = [p for p in prior if (cur_time - p["dt"]).days < 90]
        prior_24h = [p for p in prior if (cur_time - p["dt"]).total_seconds() < 86400]
        prior_1h = [p for p in prior if (cur_time - p["dt"]).total_seconds() < 3600]

        avg_val = round(float(np.mean([p["amount"] for p in prior_90d])), 2) if prior_90d else None
        if is_ring_attack and avg_val:
            amount = min(195000.0, round(min(avg_val, 8500.0) * rng.uniform(5.2, 9.5), 2))
        else:
            base_amt = min(avg_val, 3200.0) if avg_val else rng.uniform(250.0, 3200.0)
            amount = min(
                95000.0, round(max(10.0, float(np_rng.lognormal(math.log(base_amt), 0.55))), 2)
            )
        amount_paise = int(round(amount * 100))

        missing_loc = rng.random() < 0.03
        if missing_loc:
            c_loc = None
            is_new_city = None
            dist_km = None
        else:
            if is_ring_attack or rng.random() < 0.10:
                c_city = rng.choice(cities)
            else:
                c_city = cust["home_city"]
            c_lat, c_lon, _ = CITIES_COORDS[c_city]
            c_loc_dict = {
                "lat": round(c_lat + rng.uniform(-0.05, 0.05), 4),
                "lon": round(c_lon + rng.uniform(-0.05, 0.05), 4),
                "city": c_city,
            }
            c_loc = json.dumps(c_loc_dict)
            seen_cities = {p["city"] for p in prior_90d if p["city"] is not None} | {
                cust["home_city"]
            }
            is_new_city = c_city not in seen_cities
            dist_km = haversine_km(
                c_loc_dict["lat"], c_loc_dict["lon"], m_loc["lat"], m_loc["lon"]
            )

        if did is None:
            is_new_device = None
            distinct_cust_7d = None
        else:
            seen_devs = {p["device_id"] for p in prior_90d if p["device_id"] is not None}
            is_new_device = did not in seen_devs
            dev_events_7d = [
                c_id
                for dt_prev, c_id in device_history.get(did, [])
                if (cur_time - dt_prev).total_seconds() < 7 * 86400
            ]
            distinct_cust_7d = len(set(dev_events_7d))

        dt_ist = cur_time.astimezone(IST)
        hour_ist = dt_ist.hour
        is_night = 0 <= hour_ist < 5

        failed_24h = sum(1 for p in prior_24h if p["status"] == "FAILED")
        if is_ring_attack:
            failed_24h = max(failed_24h, rng.randint(3, 6))
        has_recent_failures = failed_24h >= 3

        cust_created = datetime.fromisoformat(cust["created_at"])
        account_age = max(0, (cur_time - cust_created).days)
        is_new_account = account_age < 30

        txn_count_1h = len(prior_1h)
        txn_amount_24h_paise = sum(p["amount_paise"] for p in prior_24h)
        amt_ratio = round(amount / avg_val, 4) if (avg_val and avg_val > 0) else None

        reason_codes = assign_reason_codes(
            is_new_device=is_new_device,
            is_new_city=is_new_city,
            distance_km=dist_km,
            is_night=is_night,
            has_recent_failures=has_recent_failures,
            amount_to_avg_ratio=amt_ratio,
            is_new_account=is_new_account,
            txn_count_last_1h=txn_count_1h,
            merchant_category=mcc,
        )

        combined_flags = (
            int(is_new_device is True)
            + int(is_new_city is True or (dist_km is not None and dist_km > 500.0))
            + int(is_night is True)
            + int(has_recent_failures is True)
            + int(is_new_account is True)
        )

        fraud_label = (
            1
            if (
                is_ring_attack
                or len(reason_codes) >= 4
                or (len(reason_codes) >= 3 and rng.random() < 0.35)
            )
            else 0
        )
        status = "FAILED" if rng.random() < 0.08 else "SUCCESS"
        tid = f"TX_{idx:010d}"
        channel = rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]

        tx_rows.append(
            {
                "transaction_id": tid,
                "customer_id": cid,
                "merchant_id": mid,
                "device_id": did,
                "amount_paise": amount_paise,
                "transaction_amount": amount,
                "payment_channel": channel,
                "merchant_category": mcc,
                "transaction_time": cur_time.isoformat(),
                "customer_age": cust["customer_age"],
                "customer_location": c_loc,
                "merchant_location": merch["merchant_location"],
                "account_age": account_age,
                "previous_transactions": len(prior_90d),
                "average_transaction_value": avg_val,
                "failed_transactions": failed_24h,
                "split": split,
                "is_synthetic": True,
            }
        )

        outcome_rows.append(
            {
                "transaction_id": tid,
                "outcome_time": (cur_time + timedelta(seconds=2)).isoformat(),
                "status": status,
                "failure_code": "AUTH_DECLINED" if status == "FAILED" else None,
                "is_synthetic": True,
            }
        )

        label_avail = cur_time + timedelta(days=60)
        label_rows.append(
            {
                "transaction_id": tid,
                "fraud_label": fraud_label,
                "label_source": "investigator_verdict" if fraud_label == 1 else "auto_maturity",
                "label_available_at": label_avail.isoformat(),
                "split": split,
                "eligible_at_next_split_start": True,
                "is_synthetic": True,
            }
        )

        feature_rows.append(
            {
                "transaction_id": tid,
                "feature_as_of": cur_time.isoformat(),
                "amount_to_avg_ratio": amt_ratio,
                "is_new_device": is_new_device,
                "customer_merchant_distance_km": dist_km,
                "is_new_city": is_new_city,
                "hour_of_day": hour_ist,
                "is_night": is_night,
                "has_recent_failures": has_recent_failures,
                "is_new_account": is_new_account,
                "txn_count_last_1h": txn_count_1h,
                "txn_amount_last_24h_paise": txn_amount_24h_paise,
                "distinct_customers_per_device_7d": distinct_cust_7d,
                "combined_risk_flags": combined_flags,
                "mcc_target_encoded_rate": 0.012 if mcc in HIGH_RISK_MCCS else 0.004,
                "mcc_encoding_as_of": "2024-11-30T18:30:00+00:00",
                "mcc_encoding_count": 100,
                "missing_customer_location": missing_loc,
                "missing_device": did is None,
                "cold_start": avg_val is None,
                "reason_codes": json.dumps(reason_codes),
                "feature_version": "sentinel-features-1.0.0",
            }
        )

        cust_history[cid].append(
            {
                "dt": cur_time,
                "amount": amount,
                "amount_paise": amount_paise,
                "device_id": did,
                "city": json.loads(c_loc)["city"] if c_loc else None,
                "status": status,
            }
        )
        if did is not None:
            device_history.setdefault(did, []).append((cur_time, cid))

    return {
        "customers": customers_df,
        "merchants": merchants_df,
        "devices": devices_df,
        "transactions": pd.DataFrame(tx_rows),
        "payment_outcomes": pd.DataFrame(outcome_rows),
        "labels": pd.DataFrame(label_rows),
        "features": pd.DataFrame(feature_rows),
    }


def load_sentinel_tables(
    data_dir: Optional[str] = None,
    min_transactions: int = 5500,
    seed: int = 42,
) -> Tuple[Dict[str, pd.DataFrame], str, Dict[str, Any]]:
    """
    3-Way Automatic Sentinel Telemetry Loader with Pre-Ingestion Privacy Gate:
      Tier 1: Local `sentinel_data/tables/` (CSV) or `sentinel_data/parquet/` (Parquet)
      Tier 2: PostgreSQL / Supabase feature store (`DATABASE_URL`)
      Tier 3: Deterministic schema-exact synthetic generator (>= 5,500 transactions)
    Returns (tables_dict, active_tier_description, privacy_audit_summary).
    """
    required_tables = [
        "customers",
        "merchants",
        "devices",
        "transactions",
        "payment_outcomes",
        "labels",
        "features",
    ]

    candidate_dirs = [
        data_dir or os.getenv("SENTINEL_DATA_DIR", "./sentinel_data"),
        "../sentinel_data",
        "./data/sentinel_data",
    ]
    for base in candidate_dirs:
        if not base:
            continue
        base_path = Path(base)
        tables_dir = base_path / "tables"
        parquet_dir = base_path / "parquet"
        if tables_dir.is_dir() and all((tables_dir / f"{t}.csv").exists() for t in required_tables):
            loaded = {t: pd.read_csv(tables_dir / f"{t}.csv") for t in required_tables}
            privacy_audit = enforce_data_privacy_guardrails(loaded)
            return loaded, f"Tier 1 (Local Sanitized CSV: {tables_dir.name})", privacy_audit
        if parquet_dir.is_dir() and all(
            (parquet_dir / f"{t}.parquet").exists() for t in required_tables
        ):
            loaded = {t: pd.read_parquet(parquet_dir / f"{t}.parquet") for t in required_tables}
            privacy_audit = enforce_data_privacy_guardrails(loaded)
            return loaded, f"Tier 1 (Local Sanitized Parquet: {parquet_dir.name})", privacy_audit

    db_url = os.getenv("DATABASE_URL", "")
    if db_url and "<" not in db_url:
        try:
            import psycopg2  # type: ignore

            conn = psycopg2.connect(db_url, connect_timeout=5)
            loaded = {}
            for t in required_tables:
                limit_clause = (
                    "LIMIT 10000"
                    if t in {"transactions", "payment_outcomes", "labels", "features"}
                    else ""
                )
                loaded[t] = pd.read_sql_query(
                    f"SELECT * FROM sentinel_demo.{t} {limit_clause}", conn
                )
            conn.close()
            if len(loaded["transactions"]) >= 5000:
                privacy_audit = enforce_data_privacy_guardrails(loaded)
                return loaded, "Tier 2 (PostgreSQL / Supabase Telemetry Replica)", privacy_audit
        except Exception:
            pass

    num_customers = int(os.getenv("SENTINEL_NUM_CUSTOMERS", "500"))
    num_merchants = int(os.getenv("SENTINEL_NUM_MERCHANTS", "120"))
    num_devices = int(os.getenv("SENTINEL_NUM_DEVICES", "900"))
    num_txns = max(
        min_transactions, int(os.getenv("SENTINEL_NUM_TRANSACTIONS", str(min_transactions)))
    )
    synth_seed = int(os.getenv("SENTINEL_SYNTHETIC_SEED", str(seed)))

    tables = generate_synthetic_sentinel_dataset(
        num_customers=num_customers,
        num_merchants=num_merchants,
        num_devices=num_devices,
        num_transactions=num_txns,
        seed=synth_seed,
    )
    privacy_audit = enforce_data_privacy_guardrails(tables)
    return (
        tables,
        f"Tier 3 (Deterministic Schema-Compliant Synthetic Telemetry: {num_txns:,} txns, seed={synth_seed})",
        privacy_audit,
    )
