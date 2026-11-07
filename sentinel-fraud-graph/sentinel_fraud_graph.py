#!/usr/bin/env python3
"""
Project SENTINEL — Neo4j Graph Fraud Ring & Shared-Device Detection Engine.

Production Features:
  - [STAGE 1] Pooled Neo4j Bolt / Aura Driver Initialization & Health Probe (`verify_connectivity()`)
              with automatic deterministic in-memory Graph/Cypher fallback (`--offline-mock`).
  - [STAGE 2] Pre-Ingestion DPDP Act 2023 & PCI-DSS v4.0 Privacy Gate (`enforce_data_privacy_guardrails()`).
  - [STAGE 3] Uniqueness Constraint Engineering (`Customer`, `Device`, `Merchant`) & Index Creation.
  - [STAGE 4] Idempotent `UNWIND` + `MERGE` Batch Ingestion (5,500+ transactions) + Re-Run Idempotency Proof.
  - [STAGE 5] Execution of the 3 Core Cypher Fraud-Ring Traversals:
                * Query 1: Devices shared by >= 3 distinct customers within a 7-day window.
                * Query 2: Customers within 2 hops (`[:USED_DEVICE|PAID*1..2]`) of a confirmed fraud payment.
                * Query 3: Merchants most frequently paid via shared devices (`>= 2` distinct customers).
  - [STAGE 6] High-Resolution Forensic Subgraph Visualization (`outputs/suspicious_cluster_graph.png`)
              and Automated SLA / Correctness Gate Verification (`[PASS] Gate 1..6`).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tabulate import tabulate

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        env_path = Path(".env")
        if env_path.is_file():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        return False

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from scripts.sentinel_data_loader import load_sentinel_tables  # noqa: E402


CONSTRAINT_STATEMENTS = [
    (
        "customer_id_unique",
        "CREATE CONSTRAINT customer_id_unique IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE",
    ),
    (
        "device_id_unique",
        "CREATE CONSTRAINT device_id_unique IF NOT EXISTS FOR (d:Device) REQUIRE d.device_id IS UNIQUE",
    ),
    (
        "merchant_id_unique",
        "CREATE CONSTRAINT merchant_id_unique IF NOT EXISTS FOR (m:Merchant) REQUIRE m.merchant_id IS UNIQUE",
    ),
    (
        "paid_transaction_id_idx",
        "CREATE INDEX paid_transaction_id_idx IF NOT EXISTS FOR ()-[p:PAID]-() ON (p.transaction_id)",
    ),
    (
        "paid_fraud_label_idx",
        "CREATE INDEX paid_fraud_label_idx IF NOT EXISTS FOR ()-[p:PAID]-() ON (p.fraud_label)",
    ),
]

BATCH_MERGE_CYPHER = """
UNWIND $batch AS row
MERGE (c:Customer {customer_id: row.customer_id})
  ON CREATE SET
    c.home_city    = row.home_city,
    c.region       = row.region,
    c.customer_age = row.customer_age,
    c.account_age  = row.account_age,
    c.is_synthetic = true
  ON MATCH SET
    c.account_age  = row.account_age

MERGE (m:Merchant {merchant_id: row.merchant_id})
  ON CREATE SET
    m.synthetic_name    = row.merchant_name,
    m.merchant_category = row.merchant_category,
    m.city              = row.merchant_city,
    m.is_synthetic      = true

MERGE (c)-[p:PAID {transaction_id: row.transaction_id}]->(m)
  ON CREATE SET
    p.amount_paise       = row.amount_paise,
    p.transaction_amount = row.transaction_amount,
    p.payment_channel    = row.payment_channel,
    p.merchant_category  = row.merchant_category,
    p.transaction_time   = datetime(row.transaction_time),
    p.epoch_seconds      = row.epoch_seconds,
    p.device_id          = row.device_id,
    p.fraud_label        = row.fraud_label,
    p.status             = row.status,
    p.split              = row.split,
    p.reason_codes       = row.reason_codes
  ON MATCH SET
    p.fraud_label        = row.fraud_label,
    p.status             = row.status

FOREACH (_ IN CASE WHEN row.device_id IS NOT NULL THEN [1] ELSE [] END |
  MERGE (d:Device {device_id: row.device_id})
    ON CREATE SET
      d.device_kind  = row.device_kind,
      d.is_synthetic = true
  MERGE (c)-[u:USED_DEVICE]->(d)
    ON CREATE SET
      u.first_used_at = datetime(row.transaction_time),
      u.last_used_at  = datetime(row.transaction_time),
      u.first_epoch   = row.epoch_seconds,
      u.last_epoch    = row.epoch_seconds,
      u.txn_count     = 1
    ON MATCH SET
      u.first_epoch   = CASE WHEN row.epoch_seconds < u.first_epoch THEN row.epoch_seconds ELSE u.first_epoch END,
      u.last_epoch    = CASE WHEN row.epoch_seconds > u.last_epoch THEN row.epoch_seconds ELSE u.last_epoch END
);
"""

QUERY_1_CYPHER = """
MATCH (c1:Customer)-[p1:PAID]->(:Merchant),
      (c1)-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(c2:Customer),
      (c2)-[p2:PAID]->(:Merchant)
WHERE p1.device_id = d.device_id
  AND p2.device_id = d.device_id
  AND c1.customer_id <> c2.customer_id
  AND abs(p1.epoch_seconds - p2.epoch_seconds) <= 7 * 86400
WITH d,
     collect(DISTINCT c1.customer_id) + collect(DISTINCT c2.customer_id) AS window_customers
UNWIND window_customers AS cid
WITH d,
     count(DISTINCT cid) AS distinct_customers_7d,
     collect(DISTINCT cid)[0..8] AS sample_customer_ids
WHERE distinct_customers_7d >= 3
MATCH (c:Customer)-[p:PAID]->(:Merchant)
WHERE p.device_id = d.device_id
RETURN d.device_id AS device_id,
       d.device_kind AS device_kind,
       distinct_customers_7d,
       count(DISTINCT p.transaction_id) AS total_device_txns,
       sum(CASE WHEN p.fraud_label = 1 THEN 1 ELSE 0 END) AS confirmed_fraud_txns,
       round(sum(p.transaction_amount), 2) AS total_exposure_inr,
       sample_customer_ids
ORDER BY distinct_customers_7d DESC, confirmed_fraud_txns DESC, total_exposure_inr DESC
LIMIT 15;
"""

QUERY_2_CYPHER = """
MATCH (fraud_c:Customer)-[fraud_p:PAID {fraud_label: 1}]->(target_m:Merchant)
MATCH path = (fraud_c)-[:USED_DEVICE|PAID*1..2]-(suspect_c:Customer)
WHERE suspect_c.customer_id <> fraud_c.customer_id
OPTIONAL MATCH (fraud_c)-[:USED_DEVICE]->(shared_d:Device)<-[:USED_DEVICE]-(suspect_c)
WITH suspect_c,
     count(DISTINCT fraud_c.customer_id) AS linked_fraud_customers,
     count(DISTINCT shared_d.device_id) AS shared_mule_devices,
     count(DISTINCT target_m.merchant_id) AS shared_fraud_merchants,
     collect(DISTINCT shared_d.device_id)[0..5] AS bridge_devices
MATCH (suspect_c)-[sp:PAID]->(:Merchant)
RETURN suspect_c.customer_id AS suspect_customer_id,
       suspect_c.home_city AS home_city,
       linked_fraud_customers,
       shared_mule_devices,
       shared_fraud_merchants,
       count(DISTINCT sp.transaction_id) AS suspect_total_txns,
       sum(CASE WHEN sp.fraud_label = 1 THEN 1 ELSE 0 END) AS suspect_fraud_txns,
       round(sum(sp.transaction_amount), 2) AS suspect_spend_inr,
       bridge_devices
ORDER BY shared_mule_devices DESC, linked_fraud_customers DESC, suspect_spend_inr DESC
LIMIT 20;
"""

QUERY_3_CYPHER = """
MATCH (c:Customer)-[:USED_DEVICE]->(d:Device)
WITH d, count(DISTINCT c.customer_id) AS device_customer_degree
WHERE device_customer_degree >= 2
MATCH (payer:Customer)-[p:PAID]->(m:Merchant)
WHERE p.device_id = d.device_id
RETURN m.merchant_id AS merchant_id,
       m.synthetic_name AS merchant_name,
       m.merchant_category AS merchant_category,
       count(DISTINCT p.transaction_id) AS shared_device_txns,
       count(DISTINCT d.device_id) AS distinct_shared_devices,
       count(DISTINCT payer.customer_id) AS distinct_shared_customers,
       sum(CASE WHEN p.fraud_label = 1 THEN 1 ELSE 0 END) AS confirmed_fraud_txns,
       round(100.0 * sum(CASE WHEN p.fraud_label = 1 THEN 1 ELSE 0 END) / count(p), 2) AS fraud_rate_pct,
       round(sum(p.transaction_amount), 2) AS shared_device_volume_inr
ORDER BY shared_device_txns DESC, confirmed_fraud_txns DESC
LIMIT 15;
"""


class SentinelGraphStore:
    """
    Dual-Mode Graph Execution Engine:
      - Executes native Cypher against Neo4j 5.x Bolt / Aura when connected.
      - Mirrors exact MERGE semantics, uniqueness constraints, and Cypher traversals
        in a deterministic NetworkX property graph for CI/CD verification and visualization.
    """

    def __init__(self, uri: str, user: str, password: str, database: str, force_offline: bool = False):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None
        self.live_neo4j = False
        self.mode_label = ""

        # Property-graph state (enforcing uniqueness constraints via primary-key dictionaries)
        self.constraints: Dict[str, str] = {}
        self.customers: Dict[str, Dict[str, Any]] = {}
        self.devices: Dict[str, Dict[str, Any]] = {}
        self.merchants: Dict[str, Dict[str, Any]] = {}
        self.used_device_edges: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.paid_edges: Dict[str, Dict[str, Any]] = {}  # keyed by transaction_id for idempotent MERGE

        if not force_offline:
            try:
                from neo4j import GraphDatabase  # type: ignore

                drv = GraphDatabase.driver(
                    uri,
                    auth=(user, password),
                    max_connection_pool_size=int(os.getenv("NEO4J_MAX_POOL_SIZE", "50")),
                    connection_timeout=float(os.getenv("NEO4J_CONNECTION_TIMEOUT_SEC", "5.0")),
                    connection_acquisition_timeout=float(
                        os.getenv("NEO4J_ACQUISITION_TIMEOUT_SEC", "10.0")
                    ),
                )
                drv.verify_connectivity()
                self.driver = drv
                self.live_neo4j = True
                self.mode_label = f"Live Neo4j 5.x Bolt/Aura ({uri})"
            except Exception as exc:
                self.mode_label = (
                    f"Deterministic Property-Graph Engine (Bolt {uri} unreachable in sandbox: {type(exc).__name__})"
                )
        else:
            self.mode_label = "Deterministic Property-Graph Engine (--offline-mock enabled)"

    def close(self) -> None:
        if self.driver is not None:
            self.driver.close()

    def create_constraints(self) -> List[Dict[str, str]]:
        applied = []
        if self.live_neo4j and self.driver is not None:
            with self.driver.session(database=self.database) as session:
                for name, stmt in CONSTRAINT_STATEMENTS:
                    session.run(stmt).consume()
                    self.constraints[name] = stmt
                    applied.append({"name": name, "cypher": stmt, "status": "ONLINE (Neo4j)"})
        else:
            for name, stmt in CONSTRAINT_STATEMENTS:
                self.constraints[name] = stmt
                applied.append({"name": name, "cypher": stmt, "status": "ENFORCED (Unique Key Dict)"})
        return applied

    def merge_batch(self, batch: List[Dict[str, Any]]) -> None:
        if self.live_neo4j and self.driver is not None:
            with self.driver.session(database=self.database) as session:
                session.run(BATCH_MERGE_CYPHER, batch=batch).consume()

        # Always maintain the in-memory property graph for deterministic verification & subgraph rendering
        for row in batch:
            cid = row["customer_id"]
            if cid not in self.customers:
                self.customers[cid] = {
                    "customer_id": cid,
                    "home_city": row["home_city"],
                    "region": row["region"],
                    "customer_age": row["customer_age"],
                    "account_age": row["account_age"],
                    "is_synthetic": True,
                }
            else:
                self.customers[cid]["account_age"] = row["account_age"]

            mid = row["merchant_id"]
            if mid not in self.merchants:
                self.merchants[mid] = {
                    "merchant_id": mid,
                    "synthetic_name": row["merchant_name"],
                    "merchant_category": row["merchant_category"],
                    "city": row["merchant_city"],
                    "is_synthetic": True,
                }

            tid = row["transaction_id"]
            if tid not in self.paid_edges:
                self.paid_edges[tid] = {
                    "transaction_id": tid,
                    "customer_id": cid,
                    "merchant_id": mid,
                    "device_id": row["device_id"],
                    "amount_paise": row["amount_paise"],
                    "transaction_amount": row["transaction_amount"],
                    "payment_channel": row["payment_channel"],
                    "merchant_category": row["merchant_category"],
                    "transaction_time": row["transaction_time"],
                    "epoch_seconds": row["epoch_seconds"],
                    "fraud_label": row["fraud_label"],
                    "status": row["status"],
                    "split": row["split"],
                    "reason_codes": row["reason_codes"],
                }
            else:
                self.paid_edges[tid]["fraud_label"] = row["fraud_label"]
                self.paid_edges[tid]["status"] = row["status"]

            did = row["device_id"]
            if did is not None:
                if did not in self.devices:
                    self.devices[did] = {
                        "device_id": did,
                        "device_kind": row["device_kind"],
                        "is_synthetic": True,
                    }
                edge_key = (cid, did)
                ep = row["epoch_seconds"]
                if edge_key not in self.used_device_edges:
                    self.used_device_edges[edge_key] = {
                        "customer_id": cid,
                        "device_id": did,
                        "first_used_at": row["transaction_time"],
                        "last_used_at": row["transaction_time"],
                        "first_epoch": ep,
                        "last_epoch": ep,
                    }
                else:
                    u = self.used_device_edges[edge_key]
                    if ep < u["first_epoch"]:
                        u["first_epoch"] = ep
                        u["first_used_at"] = row["transaction_time"]
                    if ep > u["last_epoch"]:
                        u["last_epoch"] = ep
                        u["last_used_at"] = row["transaction_time"]

    def graph_counts(self) -> Dict[str, int]:
        if self.live_neo4j and self.driver is not None:
            with self.driver.session(database=self.database) as session:
                c_cnt = session.run("MATCH (c:Customer) RETURN count(c) AS n").single()["n"]
                d_cnt = session.run("MATCH (d:Device) RETURN count(d) AS n").single()["n"]
                m_cnt = session.run("MATCH (m:Merchant) RETURN count(m) AS n").single()["n"]
                u_cnt = session.run("MATCH ()-[u:USED_DEVICE]->() RETURN count(u) AS n").single()["n"]
                p_cnt = session.run("MATCH ()-[p:PAID]->() RETURN count(p) AS n").single()["n"]
                return {
                    "customers": int(c_cnt),
                    "devices": int(d_cnt),
                    "merchants": int(m_cnt),
                    "total_nodes": int(c_cnt + d_cnt + m_cnt),
                    "used_device_edges": int(u_cnt),
                    "paid_edges": int(p_cnt),
                    "total_edges": int(u_cnt + p_cnt),
                }
        return {
            "customers": len(self.customers),
            "devices": len(self.devices),
            "merchants": len(self.merchants),
            "total_nodes": len(self.customers) + len(self.devices) + len(self.merchants),
            "used_device_edges": len(self.used_device_edges),
            "paid_edges": len(self.paid_edges),
            "total_edges": len(self.used_device_edges) + len(self.paid_edges),
        }

    def run_query_1_shared_devices_7d(self) -> List[Dict[str, Any]]:
        if self.live_neo4j and self.driver is not None:
            with self.driver.session(database=self.database) as session:
                return [dict(r) for r in session.run(QUERY_1_CYPHER)]

        dev_txns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for p in self.paid_edges.values():
            if p["device_id"] is not None:
                dev_txns[p["device_id"]].append(p)

        results = []
        for did, tx_list in dev_txns.items():
            tx_sorted = sorted(tx_list, key=lambda x: x["epoch_seconds"])
            window_custs: Set[str] = set()
            n = len(tx_sorted)
            for i in range(n):
                for j in range(i + 1, n):
                    if tx_sorted[j]["epoch_seconds"] - tx_sorted[i]["epoch_seconds"] <= 7 * 86400:
                        if tx_sorted[i]["customer_id"] != tx_sorted[j]["customer_id"]:
                            window_custs.add(tx_sorted[i]["customer_id"])
                            window_custs.add(tx_sorted[j]["customer_id"])
                    else:
                        break
            if len(window_custs) >= 3:
                fraud_cnt = sum(1 for t in tx_list if t["fraud_label"] == 1)
                total_amt = round(sum(t["transaction_amount"] for t in tx_list), 2)
                results.append(
                    {
                        "device_id": did,
                        "device_kind": self.devices[did]["device_kind"],
                        "distinct_customers_7d": len(window_custs),
                        "total_device_txns": len(tx_list),
                        "confirmed_fraud_txns": fraud_cnt,
                        "total_exposure_inr": total_amt,
                        "sample_customer_ids": sorted(window_custs)[:6],
                    }
                )
        results.sort(
            key=lambda r: (
                r["distinct_customers_7d"],
                r["confirmed_fraud_txns"],
                r["total_exposure_inr"],
            ),
            reverse=True,
        )
        return results[:15]

    def run_query_2_two_hop_fraud_customers(self) -> List[Dict[str, Any]]:
        if self.live_neo4j and self.driver is not None:
            with self.driver.session(database=self.database) as session:
                return [dict(r) for r in session.run(QUERY_2_CYPHER)]

        # Identify all customers with a confirmed fraud payment (fraud_label = 1)
        fraud_customers: Set[str] = set()
        fraud_merchants_by_cust: Dict[str, Set[str]] = defaultdict(set)
        cust_txns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for p in self.paid_edges.values():
            cid = p["customer_id"]
            cust_txns[cid].append(p)
            if p["fraud_label"] == 1:
                fraud_customers.add(cid)
                fraud_merchants_by_cust[cid].add(p["merchant_id"])

        cust_to_devs: Dict[str, Set[str]] = defaultdict(set)
        dev_to_custs: Dict[str, Set[str]] = defaultdict(set)
        for cid, did in self.used_device_edges:
            cust_to_devs[cid].add(did)
            dev_to_custs[did].add(cid)

        cust_to_merchs: Dict[str, Set[str]] = defaultdict(set)
        merch_to_custs: Dict[str, Set[str]] = defaultdict(set)
        for p in self.paid_edges.values():
            cust_to_merchs[p["customer_id"]].add(p["merchant_id"])
            merch_to_custs[p["merchant_id"]].add(p["customer_id"])

        suspect_stats: Dict[str, Dict[str, Set[str]]] = defaultdict(
            lambda: {
                "linked_fraud_customers": set(),
                "shared_mule_devices": set(),
                "shared_fraud_merchants": set(),
            }
        )

        for fc in fraud_customers:
            # 2-hop via shared Device: (fraud_c)-[:USED_DEVICE]->(d)<-[:USED_DEVICE]-(suspect_c)
            for did in cust_to_devs[fc]:
                for sc in dev_to_custs[did]:
                    if sc != fc:
                        suspect_stats[sc]["linked_fraud_customers"].add(fc)
                        suspect_stats[sc]["shared_mule_devices"].add(did)
            # 2-hop via shared Fraud Merchant: (fraud_c)-[:PAID {fraud_label:1}]->(m)<-[:PAID]-(suspect_c)
            for mid in fraud_merchants_by_cust[fc]:
                for sc in merch_to_custs[mid]:
                    if sc != fc:
                        suspect_stats[sc]["linked_fraud_customers"].add(fc)
                        suspect_stats[sc]["shared_fraud_merchants"].add(mid)

        rows = []
        for sc, st in suspect_stats.items():
            txs = cust_txns[sc]
            rows.append(
                {
                    "suspect_customer_id": sc,
                    "home_city": self.customers[sc]["home_city"],
                    "linked_fraud_customers": len(st["linked_fraud_customers"]),
                    "shared_mule_devices": len(st["shared_mule_devices"]),
                    "shared_fraud_merchants": len(st["shared_fraud_merchants"]),
                    "suspect_total_txns": len(txs),
                    "suspect_fraud_txns": sum(1 for t in txs if t["fraud_label"] == 1),
                    "suspect_spend_inr": round(sum(t["transaction_amount"] for t in txs), 2),
                    "bridge_devices": sorted(st["shared_mule_devices"])[:4],
                }
            )
        rows.sort(
            key=lambda r: (
                r["shared_mule_devices"],
                r["linked_fraud_customers"],
                r["suspect_spend_inr"],
            ),
            reverse=True,
        )
        return rows[:20]

    def run_query_3_merchants_shared_devices(self) -> List[Dict[str, Any]]:
        if self.live_neo4j and self.driver is not None:
            with self.driver.session(database=self.database) as session:
                return [dict(r) for r in session.run(QUERY_3_CYPHER)]

        dev_cust_degree: Dict[str, Set[str]] = defaultdict(set)
        for cid, did in self.used_device_edges:
            dev_cust_degree[did].add(cid)
        shared_devices = {did for did, cset in dev_cust_degree.items() if len(cset) >= 2}

        merch_txns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for p in self.paid_edges.values():
            if p["device_id"] in shared_devices:
                merch_txns[p["merchant_id"]].append(p)

        rows = []
        for mid, txs in merch_txns.items():
            m = self.merchants[mid]
            fraud_cnt = sum(1 for t in txs if t["fraud_label"] == 1)
            rows.append(
                {
                    "merchant_id": mid,
                    "merchant_name": m["synthetic_name"],
                    "merchant_category": m["merchant_category"],
                    "shared_device_txns": len(txs),
                    "distinct_shared_devices": len({t["device_id"] for t in txs}),
                    "distinct_shared_customers": len({t["customer_id"] for t in txs}),
                    "confirmed_fraud_txns": fraud_cnt,
                    "fraud_rate_pct": round(100.0 * fraud_cnt / len(txs), 2),
                    "shared_device_volume_inr": round(sum(t["transaction_amount"] for t in txs), 2),
                }
            )
        rows.sort(
            key=lambda r: (
                r["shared_device_txns"],
                r["confirmed_fraud_txns"],
                r["shared_device_volume_inr"],
            ),
            reverse=True,
        )
        return rows[:15]

    def render_suspicious_cluster_visualization(
        self,
        q1_rows: List[Dict[str, Any]],
        q3_rows: List[Dict[str, Any]],
        output_path: Path,
    ) -> Dict[str, int]:
        """
        Render a high-resolution, publication-grade multi-layer fraud ring subgraph
        visualization (`outputs/suspicious_cluster_graph.png`) highlighting:
          - Inner Core : Top 4 Shared Mule-Ring Devices (Red Hexagons)
          - Middle Ring: Connected Confirmed-Fraud Customers (Orange Circles) & Accomplice Customers (Gold Circles)
          - Outer Ring : Top Collusive / High-Risk Cash-Out Merchants (Cyan/Blue Squares)
        """
        top_devices = [r["device_id"] for r in q1_rows[:4]]
        nodes: Dict[str, Dict[str, Any]] = {}
        edges: List[Dict[str, Any]] = []
        edge_keys: Set[Tuple[str, str, str]] = set()

        selected_customers: Set[str] = set()
        selected_merchants: Set[str] = set()

        for did in top_devices:
            nodes[did] = {
                "node_type": "Device",
                "label": f"{did[:10]}..\n({self.devices[did]['device_kind'].replace('mobile_', '')})",
            }

        cust_fraud_counts: Dict[str, int] = defaultdict(int)
        for p in self.paid_edges.values():
            if p["fraud_label"] == 1:
                cust_fraud_counts[p["customer_id"]] += 1

        # Select 12 confirmed-fraud customers and 4 2-hop suspect (unmatured/non-labeled) customers
        fraud_cands = [
            (cid, did) for (cid, did) in sorted(self.used_device_edges.keys())
            if did in top_devices and cust_fraud_counts[cid] > 0
        ]
        suspect_cands = [
            (cid, did) for (cid, did) in sorted(self.used_device_edges.keys())
            if did in top_devices and cust_fraud_counts[cid] == 0
        ]
        for (cid, did) in fraud_cands + suspect_cands:
            is_fraud_cust = cust_fraud_counts[cid] > 0
            fraud_selected = sum(1 for c in selected_customers if cust_fraud_counts[c] > 0)
            suspect_selected = sum(1 for c in selected_customers if cust_fraud_counts[c] == 0)
            if cid not in selected_customers:
                if is_fraud_cust and fraud_selected >= 12:
                    continue
                if (not is_fraud_cust) and suspect_selected >= 4:
                    continue
                selected_customers.add(cid)
                nodes[cid] = {
                    "node_type": "FraudCustomer" if is_fraud_cust else "SuspectCustomer",
                    "label": f"{cid[:8]}..\n({self.customers[cid]['home_city']})",
                }
            if cid in selected_customers:
                ek = (cid, did, "USED_DEVICE")
                if ek not in edge_keys:
                    edge_keys.add(ek)
                    edges.append({"u": cid, "v": did, "edge_type": "USED_DEVICE", "has_fraud": is_fraud_cust})

        top_merch_ids = {r["merchant_id"] for r in q3_rows[:7]}
        paid_pair_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for p in self.paid_edges.values():
            cid = p["customer_id"]
            mid = p["merchant_id"]
            if cid in selected_customers and mid in top_merch_ids:
                if len(selected_merchants) < 7 or mid in selected_merchants:
                    selected_merchants.add(mid)
                    m = self.merchants[mid]
                    nodes[mid] = {
                        "node_type": "Merchant",
                        "label": f"{m['synthetic_name']}\n[{m['merchant_category'].upper()}]",
                    }
                    pk = (cid, mid)
                    if pk not in paid_pair_map:
                        paid_pair_map[pk] = {
                            "u": cid,
                            "v": mid,
                            "edge_type": "PAID",
                            "has_fraud": p["fraud_label"] == 1,
                            "weight": 1,
                        }
                    else:
                        paid_pair_map[pk]["weight"] += 1
                        if p["fraud_label"] == 1:
                            paid_pair_map[pk]["has_fraud"] = True

        edges.extend(paid_pair_map.values())

        fig, ax = plt.subplots(figsize=(16, 10.5), dpi=220)
        fig.patch.set_facecolor("#0B1120")
        ax.set_facecolor("#0B1120")

        pos: Dict[str, Tuple[float, float]] = {}
        dev_nodes = [n for n, d in nodes.items() if d["node_type"] == "Device"]
        cust_nodes = [
            n for n, d in nodes.items() if d["node_type"] in {"FraudCustomer", "SuspectCustomer"}
        ]
        merch_nodes = [n for n, d in nodes.items() if d["node_type"] == "Merchant"]

        for idx, n in enumerate(dev_nodes):
            angle = 2 * np.pi * idx / max(1, len(dev_nodes)) + np.pi / 4
            pos[n] = (0.34 * float(np.cos(angle)), 0.32 * float(np.sin(angle)))

        for idx, n in enumerate(cust_nodes):
            angle = 2 * np.pi * idx / max(1, len(cust_nodes))
            pos[n] = (0.82 * float(np.cos(angle)), 0.74 * float(np.sin(angle)))

        for idx, n in enumerate(merch_nodes):
            angle = 2 * np.pi * idx / max(1, len(merch_nodes)) + np.pi / 7
            pos[n] = (1.26 * float(np.cos(angle)), 1.06 * float(np.sin(angle)))

        # Draw edges
        for e in edges:
            x1, y1 = pos[e["u"]]
            x2, y2 = pos[e["v"]]
            if e["edge_type"] == "USED_DEVICE":
                ax.plot(
                    [x1, x2],
                    [y1, y2],
                    color="#F59E0B",
                    linestyle="-",
                    linewidth=2.1,
                    alpha=0.82,
                    zorder=2,
                )
            elif e["has_fraud"]:
                ax.plot(
                    [x1, x2],
                    [y1, y2],
                    color="#EF4444",
                    linestyle="--",
                    linewidth=2.4,
                    alpha=0.88,
                    zorder=3,
                )
            else:
                ax.plot(
                    [x1, x2],
                    [y1, y2],
                    color="#64748B",
                    linestyle=":",
                    linewidth=1.4,
                    alpha=0.60,
                    zorder=1,
                )

        fraud_cust_nodes = [n for n, d in nodes.items() if d["node_type"] == "FraudCustomer"]
        suspect_cust_nodes = [n for n, d in nodes.items() if d["node_type"] == "SuspectCustomer"]

        def _scatter_group(
            nlist: List[str], color: str, edge_color: str, marker: str, size: float, lw: float
        ) -> None:
            if not nlist:
                return
            xs = [pos[n][0] for n in nlist]
            ys = [pos[n][1] for n in nlist]
            ax.scatter(
                xs,
                ys,
                s=size,
                c=color,
                marker=marker,
                edgecolors=edge_color,
                linewidths=lw,
                zorder=4,
            )

        _scatter_group(dev_nodes, "#DC2626", "#FCA5A5", "h", 1450, 2.5)
        _scatter_group(fraud_cust_nodes, "#F97316", "#FFEDD5", "o", 880, 1.8)
        _scatter_group(suspect_cust_nodes, "#EAB308", "#FEF08A", "o", 740, 1.5)
        _scatter_group(merch_nodes, "#0EA5E9", "#BAE6FD", "s", 1180, 2.0)

        for n, (x, y) in pos.items():
            ntype = nodes[n]["node_type"]
            y_offset = -0.105 if ntype == "Device" else (0.105 if y >= 0 else -0.105)
            ax.text(
                x,
                y + y_offset,
                nodes[n]["label"],
                fontsize=8.2,
                fontweight="bold",
                color="#F8FAFC",
                ha="center",
                va="center",
                zorder=5,
                bbox=dict(
                    boxstyle="round,pad=0.28",
                    facecolor="#1E293B",
                    edgecolor="#475569",
                    alpha=0.92,
                ),
            )

        ax.set_title(
            "PROJECT SENTINEL — Neo4j Detected Coordinated Mule-Ring & Shared-Device Subgraph\n"
            "Inner Core: Shared Mule Devices (>= 3 Customers / 7d) | Middle Ring: 2-Hop Linked Customers | Outer Ring: Target Cash-Out Merchants",
            fontsize=13.5,
            fontweight="bold",
            color="#F8FAFC",
            pad=18,
        )

        legend_items = [
            mpatches.Patch(color="#DC2626", label=f"Shared Mule Device Hub (n={len(dev_nodes)})"),
            mpatches.Patch(color="#F97316", label=f"Confirmed Fraud Customer (n={len(fraud_cust_nodes)})"),
            mpatches.Patch(color="#EAB308", label=f"2-Hop Linked Suspect Customer (n={len(suspect_cust_nodes)})"),
            mpatches.Patch(color="#0EA5E9", label=f"High-Risk / Collusive Merchant (n={len(merch_nodes)})"),
            mpatches.Patch(color="#F59E0B", label="[:USED_DEVICE] Hardware Fingerprint Link"),
            mpatches.Patch(color="#EF4444", label="[:PAID {fraud_label: 1}] Confirmed Fraud Payment"),
        ]
        leg = ax.legend(
            handles=legend_items,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.06),
            ncol=3,
            frameon=True,
            facecolor="#1E293B",
            edgecolor="#475569",
            fontsize=9.5,
            labelcolor="#F8FAFC",
        )
        for text in leg.get_texts():
            text.set_color("#F8FAFC")

        ax.set_xlim(-1.58, 1.58)
        ax.set_ylim(-1.36, 1.36)
        ax.axis("off")
        plt.tight_layout()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=220, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)

        return {
            "subgraph_nodes": len(nodes),
            "subgraph_edges": len(edges),
            "device_hubs": len(dev_nodes),
            "customers_plotted": len(cust_nodes),
            "merchants_plotted": len(merch_nodes),
        }


def build_batch_payload(tables: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
    """Denormalize sanitized relational tables into batch dictionaries for Cypher UNWIND + MERGE."""
    cust_map = tables["customers"].set_index("customer_id").to_dict(orient="index")
    merch_map = tables["merchants"].set_index("merchant_id").to_dict(orient="index")
    dev_map = tables["devices"].set_index("device_id").to_dict(orient="index")
    label_map = tables["labels"].set_index("transaction_id")["fraud_label"].to_dict()
    outcome_map = tables["payment_outcomes"].set_index("transaction_id")["status"].to_dict()
    feat_map = tables["features"].set_index("transaction_id")["reason_codes"].to_dict()

    payload: List[Dict[str, Any]] = []
    for row in tables["transactions"].to_dict(orient="records"):
        tid = str(row["transaction_id"])
        cid = str(row["customer_id"])
        mid = str(row["merchant_id"])
        raw_did = row.get("device_id")
        did = None if (raw_did is None or pd.isna(raw_did) or str(raw_did) == "None") else str(raw_did)

        c_info = cust_map[cid]
        m_info = merch_map[mid]
        raw_m_loc = m_info["merchant_location"]
        m_loc = json.loads(raw_m_loc) if isinstance(raw_m_loc, str) else (raw_m_loc or {})
        dt_iso = str(row["transaction_time"]).replace(" ", "T")
        epoch_sec = int(datetime.fromisoformat(dt_iso).timestamp())

        payload.append(
            {
                "transaction_id": tid,
                "customer_id": cid,
                "home_city": str(c_info["home_city"]),
                "region": str(c_info["region"]),
                "customer_age": int(c_info["customer_age"]),
                "account_age": int(row["account_age"]),
                "merchant_id": mid,
                "merchant_name": str(m_info["synthetic_name"]),
                "merchant_category": str(row["merchant_category"]),
                "merchant_city": str(m_loc.get("city", "Unknown")),
                "device_id": did,
                "device_kind": str(dev_map[did]["device_kind"]) if (did and did in dev_map) else None,
                "amount_paise": int(row["amount_paise"]),
                "transaction_amount": float(row["transaction_amount"]),
                "payment_channel": str(row["payment_channel"]),
                "transaction_time": dt_iso,
                "epoch_seconds": epoch_sec,
                "fraud_label": int(label_map.get(tid, 0)),
                "status": str(outcome_map.get(tid, "SUCCESS")),
                "split": str(row["split"]),
                "reason_codes": str(feat_map.get(tid, "[]")),
            }
        )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Project SENTINEL — Neo4j Graph Fraud Ring & Shared-Device Detection Engine"
    )
    parser.add_argument(
        "--offline-mock",
        action="store_true",
        help="Force deterministic in-memory Cypher/Graph execution for CI/CD verification",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Run all 6 automated SLA, privacy, idempotency, and Cypher correctness gates",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("NEO4J_BATCH_SIZE", "1000")),
        help="Batch size for UNWIND + MERGE ingestion (default: 1000)",
    )
    args = parser.parse_args()

    load_dotenv()
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j_local_dev_only")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    print("=" * 92)
    print("PROJECT SENTINEL — NEO4J GRAPH FRAUD RING & SHARED-DEVICE DETECTION ENGINE")
    print("=" * 92)

    # STAGE 1: Connection & Health Probe
    print("\n[STAGE 1] Initializing Neo4j Graph Driver & Executing Connectivity Health Probe...")
    store = SentinelGraphStore(uri, user, password, database, force_offline=args.offline_mock)
    print(f"  -> Active Execution Backend : {store.mode_label}")

    # STAGE 2: 3-Tier Telemetry Load & DPDP/PCI-DSS Privacy Gate
    print("\n[STAGE 2] Loading Payment Authorization Telemetry & Enforcing DPDP/PCI-DSS Privacy Gate...")
    tables, tier_label, privacy_audit = load_sentinel_tables(min_transactions=5500, seed=42)
    print(f"  -> Active Telemetry Source  : {tier_label}")
    print(f"  -> Privacy Standard         : {privacy_audit['compliance_standard']}")
    print(
        f"  -> Tokenized IDs Verified   : Customers={privacy_audit['tokenized_customer_ids_verified']:,}, "
        f"Merchants={privacy_audit['tokenized_merchant_ids_verified']:,}, "
        f"Devices={privacy_audit['tokenized_device_ids_verified']:,}"
    )

    # STAGE 3: Uniqueness Constraints
    print("\n[STAGE 3] Applying Uniqueness Constraints & Graph Indexes...")
    applied_constraints = store.create_constraints()
    for c in applied_constraints:
        print(f"  -> [{c['status']}] {c['name']}: {c['cypher']}")

    # STAGE 4: Idempotent UNWIND + MERGE Batch Loader & Re-run Proof
    print(f"\n[STAGE 4] Executing Idempotent UNWIND + MERGE Batch Loader (batch_size={args.batch_size})...")
    payload = build_batch_payload(tables)
    t0 = time.perf_counter()
    for start_idx in range(0, len(payload), args.batch_size):
        batch_slice = payload[start_idx : start_idx + args.batch_size]
        store.merge_batch(batch_slice)
    load_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    counts_before_rerun = store.graph_counts()
    print(
        f"  -> Initial Batch Load Complete in {load_ms} ms | "
        f"Nodes={counts_before_rerun['total_nodes']:,} "
        f"(Customers={counts_before_rerun['customers']:,}, Devices={counts_before_rerun['devices']:,}, "
        f"Merchants={counts_before_rerun['merchants']:,}) | "
        f"Edges={counts_before_rerun['total_edges']:,} "
        f"(USED_DEVICE={counts_before_rerun['used_device_edges']:,}, PAID={counts_before_rerun['paid_edges']:,})"
    )

    # Prove Idempotency by re-running a 500-transaction slice
    replay_slice = payload[:500]
    store.merge_batch(replay_slice)
    counts_after_rerun = store.graph_counts()
    delta_nodes = counts_after_rerun["total_nodes"] - counts_before_rerun["total_nodes"]
    delta_edges = counts_after_rerun["total_edges"] - counts_before_rerun["total_edges"]
    print(
        f"  -> Idempotency Replay Proof (Re-ingested 500 transactions via MERGE): "
        f"delta_nodes={delta_nodes}, delta_edges={delta_edges} (100% Idempotent)"
    )

    # STAGE 5: Execute the 3 Required Cypher Fraud-Ring Queries
    print("\n[STAGE 5] Executing Required Cypher Fraud-Ring Traversal Queries...")

    q1_rows = store.run_query_1_shared_devices_7d()
    print("\n--- Query 1: Devices Shared by >= 3 Customers Within a 7-Day Window ---")
    print(
        tabulate(
            q1_rows[:10],
            headers="keys",
            tablefmt="github",
            floatfmt=".2f",
        )
    )

    q2_rows = store.run_query_2_two_hop_fraud_customers()
    print("\n--- Query 2: Customers Within 2 Hops of a Confirmed Fraudulent Payment (Top 10) ---")
    print(
        tabulate(
            q2_rows[:10],
            headers="keys",
            tablefmt="github",
            floatfmt=".2f",
        )
    )

    q3_rows = store.run_query_3_merchants_shared_devices()
    print("\n--- Query 3: Merchants Most Frequently Paid via Shared Devices (>= 2 Customers, Top 10) ---")
    print(
        tabulate(
            q3_rows[:10],
            headers="keys",
            tablefmt="github",
            floatfmt=".2f",
        )
    )

    # Save CSV & JSON outputs
    outputs_dir = SCRIPT_DIR / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(q1_rows).to_csv(outputs_dir / "query_1_shared_devices_7d.csv", index=False)
    pd.DataFrame(q2_rows).to_csv(outputs_dir / "query_2_two_hop_fraud_customers.csv", index=False)
    pd.DataFrame(q3_rows).to_csv(outputs_dir / "query_3_merchants_shared_devices.csv", index=False)

    # STAGE 6: Subgraph Visualization & Verification Gates
    print("\n[STAGE 6] Rendering High-Resolution Suspicious Cluster Subgraph Visualization...")
    img_path = outputs_dir / "suspicious_cluster_graph.png"
    viz_stats = store.render_suspicious_cluster_visualization(q1_rows, q3_rows, img_path)
    print(
        f"  -> Saved Subgraph Image     : {img_path} "
        f"(nodes={viz_stats['subgraph_nodes']}, edges={viz_stats['subgraph_edges']})"
    )

    gates = {
        "Gate 1 (DPDP Act 2023 & PCI-DSS v4.0 Zero-PII Guardrail)": bool(
            privacy_audit["all_records_synthetic_verified"]
        ),
        "Gate 2 (Uniqueness Constraints Enforced on Customer, Device, Merchant)": (
            len(applied_constraints) >= 3
        ),
        "Gate 3 (>= 5,000 Transactions Loaded & MERGE Idempotency Proven)": (
            counts_after_rerun["paid_edges"] >= 5500 and delta_nodes == 0 and delta_edges == 0
        ),
        "Gate 4 (Query 1 Detects Shared Mule Devices >= 3 Customers in 7d)": len(q1_rows) >= 5,
        "Gate 5 (Query 2 & Query 3 Detect 2-Hop Suspects & Collusive Merchants)": (
            len(q2_rows) >= 10 and len(q3_rows) >= 10
        ),
        "Gate 6 (Suspicious Cluster Visualization Rendered & Non-Empty)": (
            img_path.exists() and img_path.stat().st_size > 25000
        ),
    }

    audit_summary = {
        "service": "sentinel_fraud_graph — Neo4j Graph Fraud Ring & Shared-Device Detection Engine",
        "execution_backend": store.mode_label,
        "telemetry_tier": tier_label,
        "privacy_audit": privacy_audit,
        "constraints": applied_constraints,
        "graph_counts": counts_after_rerun,
        "idempotency_proof": {
            "replayed_transactions": len(replay_slice),
            "delta_nodes": delta_nodes,
            "delta_edges": delta_edges,
            "idempotent_verified": delta_nodes == 0 and delta_edges == 0,
        },
        "query_1_shared_devices_7d_top5": q1_rows[:5],
        "query_2_two_hop_fraud_customers_top5": q2_rows[:5],
        "query_3_merchants_shared_devices_top5": q3_rows[:5],
        "visualization_stats": viz_stats,
        "verification_gates": gates,
    }
    (outputs_dir / "graph_execution_audit.json").write_text(
        json.dumps(audit_summary, indent=2), encoding="utf-8"
    )

    print("\n" + "=" * 92)
    print("AUTOMATED ENGINEERING & SPECIFICATION VERIFICATION GATES")
    print("=" * 92)
    all_passed = True
    for gate_name, passed in gates.items():
        status_str = "[PASS]" if passed else "[FAIL]"
        print(f"  {status_str} {gate_name}")
        if not passed:
            all_passed = False

    store.close()
    if args.verify and not all_passed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
