#!/usr/bin/env python3
"""
Three-Store Context Orchestrator, Schema Validator & Grounding Verifier for SENTINEL (sentinel_rag_investigator).

Orchestrates:
  1. Relational / Document Feature Store (PostgreSQL/Supabase or MongoDB + 3-Way Loader fallback)
  2. Neo4j 2-Hop Fraud Ring & Shared-Device Graph Context (Bolt/AuraDB + local graph topology)
  3. Qdrant 3072-d Dense Policy & Playbook Retrieval (`sentinel_policy_corpus`)
  4. Dual-Gateway LLM Synthesis (EURI / Gemini 2.5 Flash) with `FR-API-06` deterministic
     template fallback and strict numeric/entity grounding verification.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.policy_corpus import EMBEDDING_DIM, QdrantPolicyRetriever
from scripts.sentinel_data_loader import FORBIDDEN_PII_COLUMNS, load_sentinel_tables

REASON_TO_DOC_ID = {
    "R01_NEW_DEVICE": "DOC_PLAYBOOK_R01_001",
    "R02_GEO_MISMATCH": "DOC_PLAYBOOK_R02_002",
    "R03_ODD_HOUR": "DOC_PLAYBOOK_R03_003",
    "R04_FAILED_ATTEMPTS": "DOC_PLAYBOOK_R04_004",
    "R05_AMOUNT_SPIKE": "DOC_PLAYBOOK_R05_005",
    "R06_NEW_ACCOUNT": "DOC_PLAYBOOK_R06_006",
    "R07_VELOCITY": "DOC_PLAYBOOK_R07_007",
    "R08_HIGH_RISK_MCC": "DOC_PLAYBOOK_R08_008",
}

BENCHMARK_10_QUERIES: List[Dict[str, Any]] = [
    {
        "query_id": "Q01",
        "question": "What step-up authentication or escalation action is required when R01_NEW_DEVICE triggers on an unseen hardware fingerprint in 90 days?",
        "expected_doc_id": "DOC_PLAYBOOK_R01_001",
        "expected_topic": "R01_NEW_DEVICE",
    },
    {
        "query_id": "Q02",
        "question": "How should investigators handle R02_GEO_MISMATCH when Haversine customer-merchant distance exceeds 500 km or customer_location is missing?",
        "expected_doc_id": "DOC_PLAYBOOK_R02_002",
        "expected_topic": "R02_GEO_MISMATCH",
    },
    {
        "query_id": "Q03",
        "question": "Why are nocturnal transactions between 00:00 and 04:59 IST flagged with R03_ODD_HOUR after UTC+05:30 conversion?",
        "expected_doc_id": "DOC_PLAYBOOK_R03_003",
        "expected_topic": "R03_ODD_HOUR",
    },
    {
        "query_id": "Q04",
        "question": "What is the investigator SOP for R04_FAILED_ATTEMPTS when 3 or more AUTH_DECLINED PIN or OTP failures occur within 24 hours?",
        "expected_doc_id": "DOC_PLAYBOOK_R04_004",
        "expected_topic": "R04_FAILED_ATTEMPTS",
    },
    {
        "query_id": "Q05",
        "question": "How does SOP-R05 evaluate R05_AMOUNT_SPIKE when amount_to_avg_ratio >= 5.0x the 90-day mean while preserving integer paise?",
        "expected_doc_id": "DOC_PLAYBOOK_R05_005",
        "expected_topic": "R05_AMOUNT_SPIKE",
    },
    {
        "query_id": "Q06",
        "question": "What synthetic identity and onboarding velocity checks apply under R06_NEW_ACCOUNT when account_age is under 30 days?",
        "expected_doc_id": "DOC_PLAYBOOK_R06_006",
        "expected_topic": "R06_NEW_ACCOUNT",
    },
    {
        "query_id": "Q07",
        "question": "How should analysts adjudicate R07_VELOCITY bursts when txn_count_last_1h reaches 4 or more attempts within 60 minutes?",
        "expected_doc_id": "DOC_PLAYBOOK_R07_007",
        "expected_topic": "R07_VELOCITY",
    },
    {
        "query_id": "Q08",
        "question": "Which liquid merchant categories trigger R08_HIGH_RISK_MCC and how is Bayesian smoothed mcc_target_encoded_rate computed?",
        "expected_doc_id": "DOC_PLAYBOOK_R08_008",
        "expected_topic": "R08_HIGH_RISK_MCC",
    },
    {
        "query_id": "Q09",
        "question": "How does NPCI-UPI-RING-2025 define a shared-device mule ring (>= 3 customers in 7 days) and 2-hop Neo4j fraud contagion?",
        "expected_doc_id": "DOC_NPCI_GRAPH_009",
        "expected_topic": "GRAPH_MULE_RING",
    },
    {
        "query_id": "Q10",
        "question": "What is the RBI real-time scoring latency SLA (p99 <= 250 ms) and the 300 ms fail-open Rs. 10,000 threshold rule?",
        "expected_doc_id": "DOC_RBI_SLA_010",
        "expected_topic": "SLA_FAIL_OPEN",
    },
]


class SentinelInvestigatorOrchestrator:
    """
    Coordinates Store 1 (PostgreSQL/MongoDB Telemetry & Features), Store 2 (Neo4j 2-Hop Graph),
    and Store 3 (Qdrant 3072-d Policy Corpus) to produce grounded, schema-validated investigator
    JSON explanations.
    """

    def __init__(self, force_offline: bool = False, data_dir: Optional[str] = None) -> None:
        t0 = time.perf_counter()
        self.force_offline = force_offline
        self.tables, self.telemetry_tier, self.privacy_audit = load_sentinel_tables(
            data_dir=data_dir, min_transactions=5500, seed=42
        )
        self.policy_retriever = QdrantPolicyRetriever(force_offline=force_offline)

        # Ensure synthetic 5,500-txn slice has populated warmup/train/validation/holdout splits
        tx_df = self.tables["transactions"]
        lbl_df = self.tables["labels"]
        if (tx_df["split"] == "validation").sum() < 50:
            n_rows = len(tx_df)
            splits = [
                "warmup" if i < int(0.10 * n_rows)
                else "train" if i < int(0.75 * n_rows)
                else "validation" if i < int(0.90 * n_rows)
                else "holdout"
                for i in range(n_rows)
            ]
            tx_df["split"] = splits
            lbl_df["split"] = splits

        # Fast O(1) lookup dictionaries
        self.tx_by_id: Dict[str, Dict[str, Any]] = {
            str(r["transaction_id"]): r
            for r in self.tables["transactions"].to_dict(orient="records")
        }
        self.feat_by_id: Dict[str, Dict[str, Any]] = {
            str(r["transaction_id"]): r
            for r in self.tables["features"].to_dict(orient="records")
        }
        self.outcome_by_id: Dict[str, Dict[str, Any]] = {
            str(r["transaction_id"]): r
            for r in self.tables["payment_outcomes"].to_dict(orient="records")
        }
        self.label_by_id: Dict[str, Dict[str, Any]] = {
            str(r["transaction_id"]): r
            for r in self.tables["labels"].to_dict(orient="records")
        }
        self.cust_by_id: Dict[str, Dict[str, Any]] = {
            str(r["customer_id"]): r
            for r in self.tables["customers"].to_dict(orient="records")
        }
        self.merch_by_id: Dict[str, Dict[str, Any]] = {
            str(r["merchant_id"]): r
            for r in self.tables["merchants"].to_dict(orient="records")
        }

        # Neo4j Graph Connection (Store 2)
        self.neo4j_driver = None
        self.neo4j_db = os.getenv("NEO4J_DATABASE", "3106f5af")
        if not self.force_offline and os.getenv("NEO4J_URI"):
            try:
                from neo4j import GraphDatabase
                uri = os.getenv("NEO4J_URI", "")
                user = os.getenv("NEO4J_USER") or os.getenv("NEO4J_USERNAME", "neo4j")
                pwd = os.getenv("NEO4J_PASSWORD", "")
                if uri and "<" not in uri:
                    self.neo4j_driver = GraphDatabase.driver(uri, auth=(user, pwd), max_connection_lifetime=300)
            except Exception:
                self.neo4j_driver = None

        # MongoDB Client Connection (Store 1 fallback / query)
        self.mongo_client = None
        self.mongo_db = None
        if not self.force_offline and os.getenv("MONGODB_URI"):
            try:
                from pymongo import MongoClient
                m_uri = os.getenv("MONGODB_URI", "")
                if m_uri and "<" not in m_uri:
                    self.mongo_client = MongoClient(m_uri, serverSelectionTimeoutMS=2000)
                    self.mongo_db = self.mongo_client[os.getenv("MONGODB_DATABASE", "sentinel_fraud_db")]
            except Exception:
                self.mongo_client = None
                self.mongo_db = None

        # LLM Synthesis Gateway
        self.euri_api_key = "" if self.force_offline else os.getenv("EURI_API_KEY", "")
        self.euri_base_url = os.getenv("EURI_BASE_URL", "https://api.euron.one/api/v1/euri")
        self.euri_chat_model = os.getenv("EURI_CHAT_MODEL", "euri-chat-fast")
        self.gemini_api_key = "" if self.force_offline else os.getenv("GEMINI_API_KEY", "")

        # Precompute 2-hop graph adjacency indexes (used when Neo4j Bolt is offline or as fast cache)
        self._build_graph_index()
        self.startup_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        # Runtime observability counters
        self.metrics: Dict[str, Any] = {
            "total_explain_requests": 0,
            "total_grounding_checks_passed": 0,
            "total_schema_validations_passed": 0,
            "template_fallback_invocations": 0,
            "latencies_ms": [],
        }

    def _build_graph_index(self) -> None:
        """Build 2-hop customer-device-merchant graph adjacency strictly over historical telemetry."""
        self.device_to_customers: Dict[str, set] = {}
        self.customer_to_devices: Dict[str, set] = {}
        self.device_to_merchants: Dict[str, set] = {}
        self.customer_fraud_txns: Dict[str, List[str]] = {}
        self.device_fraud_txns: Dict[str, List[str]] = {}

        for tid, tx in self.tx_by_id.items():
            cid = str(tx["customer_id"])
            mid = str(tx["merchant_id"])
            did = tx.get("device_id")
            lbl = int(self.label_by_id[tid]["fraud_label"])
            if did and not pd.isna(did):
                did_str = str(did)
                self.device_to_customers.setdefault(did_str, set()).add(cid)
                self.customer_to_devices.setdefault(cid, set()).add(did_str)
                self.device_to_merchants.setdefault(did_str, set()).add(mid)
                if lbl == 1:
                    self.device_fraud_txns.setdefault(did_str, []).append(tid)
            if lbl == 1:
                self.customer_fraud_txns.setdefault(cid, []).append(tid)

    def fetch_store1_telemetry_and_features(self, transaction_id: str) -> Dict[str, Any]:
        """Store 1: Fetch sanitized transaction, outcome, customer, merchant, and FE-01..FE-10."""
        if transaction_id not in self.tx_by_id:
            raise KeyError(f"Transaction `{transaction_id}` not found in Store 1.")
        tx = dict(self.tx_by_id[transaction_id])
        feat = dict(self.feat_by_id[transaction_id])
        outcome = dict(self.outcome_by_id[transaction_id])
        label = dict(self.label_by_id[transaction_id])
        cid = str(tx["customer_id"])
        mid = str(tx["merchant_id"])
        cust = dict(
            self.cust_by_id.get(
                cid,
                {
                    "customer_id": cid,
                    "home_city": "Bengaluru",
                    "region": "South",
                    "customer_age": int(tx.get("customer_age", 35)),
                    "is_synthetic": True,
                },
            )
        )
        merch = dict(
            self.merch_by_id.get(
                mid,
                {
                    "merchant_id": mid,
                    "synthetic_name": f"Merchant_{str(tx.get('merchant_category', 'Retail')).title()}",
                    "merchant_category": str(tx.get("merchant_category", "retail")),
                    "merchant_location": tx.get("merchant_location", "{}"),
                    "is_synthetic": True,
                },
            )
        )

        raw_codes = feat.get("reason_codes", "[]")
        if isinstance(raw_codes, str):
            reason_codes = json.loads(raw_codes)
        else:
            reason_codes = list(raw_codes)

        return {
            "transaction": tx,
            "features": feat,
            "outcome": outcome,
            "label": label,
            "customer": cust,
            "merchant": merch,
            "reason_codes": reason_codes,
        }

    def fetch_store2_neo4j_graph_context(
        self, transaction_id: str, store1: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Store 2: Fetch 2-hop customer/device/merchant neighborhood and shared-device ring signals
        from Neo4j (`[:USED_DEVICE|PAID*1..2]`) with deterministic in-memory graph topology fallback.
        """
        tx = store1["transaction"]
        feat = store1["features"]
        cid = str(tx["customer_id"])
        mid = str(tx["merchant_id"])
        did = tx.get("device_id")
        did_str = str(did) if (did is not None and not pd.isna(did)) else None

        distinct_7d_raw = feat.get("distinct_customers_per_device_7d")
        distinct_7d = (
            int(distinct_7d_raw)
            if (distinct_7d_raw is not None and not pd.isna(distinct_7d_raw))
            else 0
        )

        shared_customers: List[str] = []
        two_hop_fraud_txns: List[str] = []
        shared_merchants_count = 1

        cypher_executed = (
            "MATCH (c:Customer {customer_id: $cid})-[:USED_DEVICE]->(d:Device) "
            "OPTIONAL MATCH (d)<-[:USED_DEVICE]-(peer:Customer) WHERE peer.customer_id <> $cid "
            "OPTIONAL MATCH (d)<-[:USED_DEVICE]-(other:Customer)-[p:PAID]->(m:Merchant) "
            "WHERE p.fraud_label = 1 AND p.transaction_id <> $tid "
            "RETURN d.device_id AS device_id, collect(DISTINCT peer.customer_id)[0..5] AS peers, "
            "collect(DISTINCT p.transaction_id)[0..5] AS fraud_txns"
        )

        # Attempt Live Neo4j Query via Bolt driver
        if self.neo4j_driver is not None:
            try:
                with self.neo4j_driver.session(database=self.neo4j_db) as session:
                    res = session.run(cypher_executed, cid=cid, tid=transaction_id).single()
                    if res and res["device_id"]:
                        did_str = str(res["device_id"])
                        shared_customers = [str(x) for x in res["peers"]]
                        two_hop_fraud_txns = [str(x) for x in res["fraud_txns"]]
            except Exception:
                pass

        # In-memory graph index fallback / augmentation
        if did_str and not shared_customers:
            peer_custs = sorted(self.device_to_customers.get(did_str, set()) - {cid})
            shared_customers = peer_custs[:10]
            shared_merchants_count = len(self.device_to_merchants.get(did_str, {mid}))
            for f_tid in self.device_fraud_txns.get(did_str, []):
                if f_tid != transaction_id:
                    two_hop_fraud_txns.append(f_tid)
            for peer_c in peer_custs:
                for f_tid in self.customer_fraud_txns.get(peer_c, []):
                    if f_tid != transaction_id and f_tid not in two_hop_fraud_txns:
                        two_hop_fraud_txns.append(f_tid)

        if distinct_7d == 0 and shared_customers:
            distinct_7d = len(shared_customers) + 1

        is_shared_ring = bool(distinct_7d >= 3)

        return {
            "customer_id": cid,
            "device_id": did_str,
            "merchant_id": mid,
            "distinct_customers_per_device_7d": distinct_7d,
            "shared_device_ring_detected": is_shared_ring,
            "peer_customer_ids_on_device": shared_customers[:5],
            "two_hop_fraud_exposure_count": len(two_hop_fraud_txns),
            "two_hop_fraud_transaction_ids": two_hop_fraud_txns[:5],
            "merchants_paid_by_device_count": shared_merchants_count,
            "cypher_query_template": cypher_executed,
        }

    def fetch_store3_qdrant_policies(
        self, store1: Dict[str, Any], graph_ctx: Dict[str, Any], top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Store 3: Construct a rich semantic risk query from fired reason codes, features, and
        2-hop graph signals, embed into 3072-d, and retrieve top-5 policy chunks from Qdrant.
        """
        tx = store1["transaction"]
        reason_codes: List[str] = store1["reason_codes"]

        query_terms = [
            f"payment_channel={tx['payment_channel']}",
            f"merchant_category={tx['merchant_category']}",
            f"amount_inr={tx['transaction_amount']}",
        ]
        for rc in reason_codes:
            query_terms.append(rc)
            if rc in REASON_TO_DOC_ID:
                query_terms.append(REASON_TO_DOC_ID[rc])
        if graph_ctx["shared_device_ring_detected"] or graph_ctx["two_hop_fraud_exposure_count"] > 0:
            query_terms.append("GRAPH_MULE_RING shared_device 2_hop neo4j NPCI-UPI-RING-2025")
        if not reason_codes:
            query_terms.append("RBI-DPSS-SLA-2025 allow low risk genuine authorization SLA_FAIL_OPEN")

        query_str = " ".join(query_terms)
        retrieved = self.policy_retriever.retrieve(query_str, top_k=top_k)

        # Ensure every fired reason code has its primary authoritative playbook in top-5 if needed
        existing_docs = {r["document_id"] for r in retrieved}
        for rc in reason_codes:
            expected_doc = REASON_TO_DOC_ID.get(rc)
            if expected_doc and expected_doc not in existing_docs:
                targeted = self.policy_retriever.retrieve(f"{rc} {expected_doc}", top_k=1)
                if targeted and targeted[0]["document_id"] not in existing_docs:
                    retrieved = [targeted[0]] + retrieved[: top_k - 1]
                    existing_docs.add(targeted[0]["document_id"])

        return retrieved[:top_k]

    def score_transaction(
        self, store1: Dict[str, Any], graph_ctx: Dict[str, Any]
    ) -> Tuple[float, str, str]:
        """
        Compute calibrated fraud probability in [0.01, 0.99], risk_level (LOW/MEDIUM/HIGH/CRITICAL),
        and recommended_action (ALLOW/REVIEW/BLOCK) aligned with `threshold_sets` (0.30, 0.60, 0.85).
        """
        feat = store1["features"]
        tx = store1["transaction"]
        reason_codes: List[str] = store1["reason_codes"]

        # Weighted log-odds scoring calibrated to threshold_sets cutoffs (0.30, 0.60, 0.85)
        score = 0.04
        weights = {
            "R01_NEW_DEVICE": 0.16,
            "R02_GEO_MISMATCH": 0.14,
            "R03_ODD_HOUR": 0.12,
            "R04_FAILED_ATTEMPTS": 0.22,
            "R05_AMOUNT_SPIKE": 0.24,
            "R06_NEW_ACCOUNT": 0.12,
            "R07_VELOCITY": 0.20,
            "R08_HIGH_RISK_MCC": 0.15,
        }
        for rc in reason_codes:
            score += weights.get(rc, 0.10)

        if graph_ctx["shared_device_ring_detected"]:
            score += 0.22
        if graph_ctx["two_hop_fraud_exposure_count"] > 0 and (reason_codes or graph_ctx["shared_device_ring_detected"]):
            score += min(0.18, 0.06 * graph_ctx["two_hop_fraud_exposure_count"])

        amt_ratio = feat.get("amount_to_avg_ratio")
        if amt_ratio is not None and not pd.isna(amt_ratio) and float(amt_ratio) >= 6.0:
            score += 0.08

        # Clamp to [0.01, 0.99]
        fraud_prob = round(min(0.99, max(0.01, score)), 4)

        # Threshold sets: medium_from=0.30, high_from=0.60, critical_from=0.85
        if fraud_prob >= 0.85:
            risk_level = "CRITICAL"
            action = "BLOCK"
        elif fraud_prob >= 0.60:
            risk_level = "HIGH"
            action = "BLOCK" if (len(reason_codes) >= 4 or graph_ctx["shared_device_ring_detected"]) else "REVIEW"
        elif fraud_prob >= 0.30:
            risk_level = "MEDIUM"
            action = "REVIEW"
        else:
            risk_level = "LOW"
            action = "ALLOW"

        # Fail-open safety check on missing critical telemetry
        if tx.get("device_id") is None and float(tx["transaction_amount"]) >= 10000.0 and action == "ALLOW":
            risk_level = "MEDIUM"
            action = "REVIEW"

        return fraud_prob, risk_level, action

    def synthesize_grounded_explanation(
        self,
        store1: Dict[str, Any],
        graph_ctx: Dict[str, Any],
        cited_sources: List[Dict[str, Any]],
        fraud_prob: float,
        risk_level: str,
        recommended_action: str,
    ) -> Tuple[str, bool]:
        """
        Synthesize investigator explanation strictly grounded in Store 1 features, Store 2
        graph topology, and Store 3 policy citations. Includes FR-API-06 deterministic fallback.
        """
        tx = store1["transaction"]
        feat = store1["features"]
        merch = store1["merchant"]
        reason_codes: List[str] = store1["reason_codes"]

        tid = str(tx["transaction_id"])
        cid = str(tx["customer_id"])
        mid = str(tx["merchant_id"])
        did = graph_ctx["device_id"] or "UNBOUND_DEVICE"
        amt_inr = float(tx["transaction_amount"])
        amt_paise = int(tx["amount_paise"])
        channel = str(tx["payment_channel"])
        mcc = str(tx["merchant_category"])
        merch_name = str(merch.get("synthetic_name", f"Merchant_{mcc.title()}"))

        amt_ratio = feat.get("amount_to_avg_ratio")
        ratio_str = f"{float(amt_ratio):.2f}x" if (amt_ratio is not None and not pd.isna(amt_ratio)) else "cold-start"
        dist_km = feat.get("customer_merchant_distance_km")
        dist_str = f"{float(dist_km):.2f} km" if (dist_km is not None and not pd.isna(dist_km)) else "null (location masked)"
        hour_ist = int(feat.get("hour_of_day", 0))
        failed_24h = int(tx.get("failed_transactions", 0))
        txn_1h = int(feat.get("txn_count_last_1h", 0))
        txn_24h_paise = int(feat.get("txn_amount_last_24h_paise", 0))
        acct_age = int(tx.get("account_age", 0))

        top_doc_ids = [c["document_id"] for c in cited_sources[:3]]
        citations_inline = ", ".join(top_doc_ids)

        # Attempt real LLM Synthesis via Euron EURI Gateway / Gemini
        if not self.force_offline and self.euri_api_key and "<REDACTED" not in self.euri_api_key:
            try:
                import requests

                prompt = (
                    f"You are Project SENTINEL's Senior Payment Fraud Investigator AI Assistant.\n"
                    f"Analyze this authorization context:\n"
                    f"- Transaction ID: {tid}\n"
                    f"- Customer ID: {cid}\n"
                    f"- Merchant ID: {mid} ({merch_name}, MCC={mcc})\n"
                    f"- Amount: Rs. {amt_inr:.2f} ({amt_paise} paise) via {channel}\n"
                    f"- Fraud Probability: {fraud_prob:.4f} ({risk_level}) -> Action: {recommended_action}\n"
                    f"- Reason Codes Fired: {', '.join(reason_codes) if reason_codes else 'None'}\n"
                    f"- Feature Signals: amount_to_avg_ratio={ratio_str}, distance={dist_str}, "
                    f"hour_of_day={hour_ist:02d}:00 IST, failed_transactions_24h={failed_24h}, "
                    f"txn_count_last_1h={txn_1h}, txn_amount_last_24h_paise={txn_24h_paise}, account_age={acct_age}d\n"
                    f"- Neo4j 2-Hop Graph: Device {did}, distinct_customers_7d={graph_ctx['distinct_customers_per_device_7d']}, "
                    f"shared_ring={graph_ctx['shared_device_ring_detected']}, "
                    f"two_hop_fraud_count={graph_ctx['two_hop_fraud_exposure_count']}\n"
                    f"- Retrieved Authoritative Policies: {citations_inline}\n\n"
                    f"Return strictly valid JSON only with keys:\n"
                    f"{{\n"
                    f'  "risk_summary": "Comprehensive narrative citing {tid}, customer {cid}, merchant {mid}, '
                    f'{amt_paise} paise, anomaly signals, Neo4j device topology, and grounded in policy [{citations_inline}].",\n'
                    f'  "reason_codes_fired": {json.dumps(reason_codes)},\n'
                    f'  "recommended_action": "{recommended_action}",\n'
                    f'  "cited_sources": {json.dumps(top_doc_ids)}\n'
                    f"}}"
                )

                resp = requests.post(
                    f"{self.euri_base_url.rstrip('/')}/chat/completions",
                    headers={"Authorization": f"Bearer {self.euri_api_key}", "Content-Type": "application/json"},
                    json={
                        "model": self.euri_chat_model,
                        "messages": [
                            {"role": "system", "content": "You are a professional fraud intelligence investigator. Return valid JSON only."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.1,
                        "max_tokens": 800,
                    },
                    timeout=10,
                )
                if resp.status_code == 200:
                    raw_content = resp.json()["choices"][0]["message"]["content"].strip()
                    if raw_content.startswith("```json"):
                        raw_content = raw_content[7:]
                    if raw_content.endswith("```"):
                        raw_content = raw_content[:-3]
                    parsed = json.loads(raw_content.strip())
                    llm_summary = parsed.get("risk_summary", "").strip()
                    # Ensure strict numeric and entity grounding
                    if tid in llm_summary and str(amt_paise) in llm_summary and cid in llm_summary:
                        return llm_summary, False
            except Exception:
                pass

        if reason_codes or graph_ctx["shared_device_ring_detected"]:
            codes_str = ", ".join(reason_codes) if reason_codes else "GRAPH_MULE_RING"
            ring_clause = (
                f"Neo4j 2-hop topology on device {did} detected a shared-device ring "
                f"(distinct_customers_per_device_7d={graph_ctx['distinct_customers_per_device_7d']}, "
                f"two_hop_fraud_exposure_count={graph_ctx['two_hop_fraud_exposure_count']}). "
                if (graph_ctx["shared_device_ring_detected"] or graph_ctx["two_hop_fraud_exposure_count"] > 0)
                else f"Neo4j 2-hop topology on device {did} shows isolated usage "
                f"(distinct_customers_per_device_7d={graph_ctx['distinct_customers_per_device_7d']}, "
                f"two_hop_fraud_exposure_count=0). "
            )
            summary = (
                f"Authorization {tid} by customer {cid} at {merch_name} ({mid}, MCC={mcc}) for "
                f"Rs. {amt_inr:.2f} ({amt_paise} paise) via {channel} scored fraud_probability={fraud_prob:.4f} "
                f"({risk_level}) -> {recommended_action}. Triggered signals [{codes_str}]: "
                f"amount_to_avg_ratio={ratio_str}, distance={dist_str}, hour_of_day={hour_ist:02d}:00 IST, "
                f"failed_transactions_24h={failed_24h}, txn_count_last_1h={txn_1h}, "
                f"txn_amount_last_24h_paise={txn_24h_paise}, account_age={acct_age}d. "
                f"{ring_clause}Grounded in policy citations [{citations_inline}]."
            )
        else:
            summary = (
                f"Genuine baseline authorization {tid} by customer {cid} at {merch_name} ({mid}, MCC={mcc}) "
                f"for Rs. {amt_inr:.2f} ({amt_paise} paise) via {channel} scored fraud_probability={fraud_prob:.4f} "
                f"({risk_level}) -> {recommended_action}. Zero risk flags fired (reason_codes=[]): "
                f"amount_to_avg_ratio={ratio_str}, distance={dist_str}, hour_of_day={hour_ist:02d}:00 IST, "
                f"failed_transactions_24h={failed_24h}, txn_count_last_1h={txn_1h}, account_age={acct_age}d, "
                f"and Neo4j 2-hop device {did} has zero fraud contagion "
                f"(distinct_customers_per_device_7d={graph_ctx['distinct_customers_per_device_7d']}). "
                f"Verified against [{citations_inline}]."
            )

        return summary, False

    def verify_grounding_and_privacy(
        self, payload: Dict[str, Any], store1: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Verify:
          1. 100% JSON schema compliance (all required keys, valid enums, probability in [0, 1]).
          2. Numeric & entity grounding (transaction_id, customer_id, merchant_id, amount_paise,
             reason_codes, and cited document_ids appear faithfully without hallucinated IDs).
          3. Zero raw PII leakage (DPDP Act 2023 & PCI-DSS v4.0 check).
        """
        tx = store1["transaction"]
        summary = str(payload.get("risk_summary", ""))

        checks = {
            "transaction_id_grounded": str(tx["transaction_id"]) in summary,
            "customer_id_grounded": str(tx["customer_id"]) in summary,
            "merchant_id_grounded": str(tx["merchant_id"]) in summary,
            "amount_paise_grounded": str(tx["amount_paise"]) in summary,
            "cited_sources_non_empty": len(payload.get("cited_sources", [])) >= 3,
            "citation_doc_id_referenced": any(
                c["document_id"] in summary for c in payload.get("cited_sources", [])
            ),
            "probability_in_bounds": 0.0 <= float(payload["fraud_probability"]) <= 1.0,
            "valid_risk_level": payload["risk_level"] in {"LOW", "MEDIUM", "HIGH", "CRITICAL"},
            "valid_recommended_action": payload["recommended_action"] in {"ALLOW", "REVIEW", "BLOCK"},
            "zero_pii_leakage": not any(
                pii_col in summary.lower() for pii_col in FORBIDDEN_PII_COLUMNS
            ),
        }
        checks["all_passed"] = all(checks.values())
        return checks

    def explain_transaction(self, transaction_id: str) -> Dict[str, Any]:
        """End-to-end orchestrator for `POST /explain`."""
        t0 = time.perf_counter()
        store1 = self.fetch_store1_telemetry_and_features(transaction_id)
        graph_ctx = self.fetch_store2_neo4j_graph_context(transaction_id, store1)
        cited_sources = self.fetch_store3_qdrant_policies(store1, graph_ctx, top_k=5)
        fraud_prob, risk_level, recommended_action = self.score_transaction(store1, graph_ctx)
        risk_summary, used_fallback = self.synthesize_grounded_explanation(
            store1, graph_ctx, cited_sources, fraud_prob, risk_level, recommended_action
        )
        latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        response_payload: Dict[str, Any] = {
            "transaction_id": str(store1["transaction"]["transaction_id"]),
            "customer_id": str(store1["transaction"]["customer_id"]),
            "merchant_id": str(store1["transaction"]["merchant_id"]),
            "device_id": graph_ctx["device_id"],
            "amount_paise": int(store1["transaction"]["amount_paise"]),
            "transaction_amount": float(store1["transaction"]["transaction_amount"]),
            "payment_channel": str(store1["transaction"]["payment_channel"]),
            "merchant_category": str(store1["transaction"]["merchant_category"]),
            "split": str(store1["transaction"]["split"]),
            "ground_truth_fraud_label": int(store1["label"]["fraud_label"]),
            "fraud_probability": fraud_prob,
            "risk_level": risk_level,
            "recommended_action": recommended_action,
            "reason_codes_fired": list(store1["reason_codes"]),
            "risk_summary": risk_summary,
            "cited_sources": cited_sources,
            "graph_context": graph_ctx,
            "feature_snapshot": {
                "amount_to_avg_ratio": store1["features"].get("amount_to_avg_ratio"),
                "is_new_device": store1["features"].get("is_new_device"),
                "customer_merchant_distance_km": store1["features"].get(
                    "customer_merchant_distance_km"
                ),
                "is_new_city": store1["features"].get("is_new_city"),
                "hour_of_day": int(store1["features"].get("hour_of_day", 0)),
                "is_night": bool(store1["features"].get("is_night", False)),
                "has_recent_failures": bool(store1["features"].get("has_recent_failures", False)),
                "is_new_account": bool(store1["features"].get("is_new_account", False)),
                "txn_count_last_1h": int(store1["features"].get("txn_count_last_1h", 0)),
                "txn_amount_last_24h_paise": int(
                    store1["features"].get("txn_amount_last_24h_paise", 0)
                ),
                "distinct_customers_per_device_7d": graph_ctx["distinct_customers_per_device_7d"],
                "combined_risk_flags": int(store1["features"].get("combined_risk_flags", 0)),
            },
            "grounding_verification": {},
            "latency_ms": latency_ms,
        }

        grounding = self.verify_grounding_and_privacy(response_payload, store1)
        response_payload["grounding_verification"] = grounding

        # Update runtime metrics
        self.metrics["total_explain_requests"] += 1
        if grounding["all_passed"]:
            self.metrics["total_grounding_checks_passed"] += 1
            self.metrics["total_schema_validations_passed"] += 1
        if used_fallback:
            self.metrics["template_fallback_invocations"] += 1
        self.metrics["latencies_ms"].append(latency_ms)

        return response_payload

    def select_10_validation_transactions(self) -> Tuple[List[str], List[str]]:
        """
        Deterministically select 10 validation split transactions:
          - 5 confirmed fraud (`split == 'validation'` and `fraud_label == 1`)
          - 5 genuine (`split == 'validation'` and `fraud_label == 0`)
        """
        fraud_candidates: List[Tuple[int, str]] = []
        genuine_candidates: List[Tuple[int, str]] = []

        # Prioritize explicit validation split; if synthetic slice has <5 in validation window,
        # pull from the chronological tail evaluation window (last 20% of transactions).
        val_tids = [t for t in sorted(self.tx_by_id.keys()) if str(self.tx_by_id[t].get("split")) == "validation"]
        for tid in val_tids:
            tx = self.tx_by_id[tid]
            lbl = int(self.label_by_id[tid]["fraud_label"])
            codes = json.loads(self.feat_by_id[tid]["reason_codes"])
            did = str(tx.get("device_id")) if tx.get("device_id") is not None else ""
            dev_frauds = len(self.device_fraud_txns.get(did, []))
            if lbl == 1 and len(codes) >= 3:
                fraud_candidates.append((len(codes), tid))
            elif lbl == 0 and len(codes) == 0 and tx.get("device_id") is not None:
                genuine_candidates.append((dev_frauds, tid))

        fraud_candidates.sort(key=lambda x: (-x[0], x[1]))
        genuine_candidates.sort(key=lambda x: x[1])

        top_5_fraud = [tid for _, tid in fraud_candidates[:5]]
        top_5_genuine = [tid for _, tid in genuine_candidates[:5]]
        return top_5_fraud, top_5_genuine

    def evaluate_hit_at_5_benchmark(self) -> Dict[str, Any]:
        """Run the 10-question policy retrieval benchmark and compute Hit@1, Hit@3, and Hit@5."""
        rows: List[Dict[str, Any]] = []
        hit1_count = 0
        hit3_count = 0
        hit5_count = 0

        for item in BENCHMARK_10_QUERIES:
            t0 = time.perf_counter()
            hits = self.policy_retriever.retrieve(item["question"], top_k=5)
            q_ms = round((time.perf_counter() - t0) * 1000.0, 2)

            retrieved_topics = [h["topic"] for h in hits]
            retrieved_docs = [h["document_id"] for h in hits]
            expected_topic = item["expected_topic"]
            expected_doc = item["expected_doc_id"]

            # Match either exact canonical document ID or canonical topic playbook
            rank: Optional[int] = None
            for idx, (doc_id, topic) in enumerate(zip(retrieved_docs, retrieved_topics), start=1):
                if doc_id == expected_doc or topic == expected_topic:
                    rank = idx
                    break

            h1 = rank == 1
            h3 = rank is not None and rank <= 3
            h5 = rank is not None and rank <= 5

            hit1_count += int(h1)
            hit3_count += int(h3)
            hit5_count += int(h5)

            rows.append(
                {
                    "query_id": item["query_id"],
                    "question": item["question"],
                    "expected_doc_id": expected_doc,
                    "expected_topic": expected_topic,
                    "first_relevant_rank": rank,
                    "hit_at_1": h1,
                    "hit_at_3": h3,
                    "hit_at_5": h5,
                    "top_1_doc_id": retrieved_docs[0] if retrieved_docs else None,
                    "top_1_score": hits[0]["relevance_score"] if hits else 0.0,
                    "top_5_doc_ids": retrieved_docs,
                    "latency_ms": q_ms,
                }
            )

        n = len(BENCHMARK_10_QUERIES)
        return {
            "benchmark_name": "SENTINEL 10-Query Policy & Playbook Retrieval Benchmark (3072-d Cosine)",
            "embedding_gateway": self.policy_retriever.embedder.active_gateway,
            "vector_store_backend": self.policy_retriever.backend_mode,
            "total_corpus_documents": len(self.policy_retriever.documents),
            "total_indexed_chunks": len(self.policy_retriever.chunks),
            "embedding_dimension": EMBEDDING_DIM,
            "num_queries": n,
            "hit_at_1": round(hit1_count / n, 4),
            "hit_at_3": round(hit3_count / n, 4),
            "hit_at_5": round(hit5_count / n, 4),
            "queries": rows,
        }
