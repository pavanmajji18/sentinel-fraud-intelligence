#!/usr/bin/env python3
"""
FastAPI Service for Project SENTINEL: Polyglot RAG Investigator Assistant (sentinel_rag_investigator).

Endpoints:
  - `POST /explain`: Orchestrates PostgreSQL/Supabase/MongoDB telemetry + features, Neo4j 2-hop
    graph neighborhood, and Qdrant 3072-d policy retrieval into a strict schema-validated JSON
    investigator explanation.
  - `GET /health`: Reports polyglot store readiness, DPDP Act 2023 & PCI-DSS v4.0 privacy gate
    status, and corpus indexing metrics.
  - `GET /metrics`: Reports p50/p95/p99 latency SLAs (`p99 <= 250 ms`), grounding pass rate,
    and schema compliance rate.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.orchestrator import SentinelInvestigatorOrchestrator


class ExplainRequest(BaseModel):
    transaction_id: str = Field(
        ...,
        description="Tokenized transaction identifier (e.g. TX_0000004935)",
        examples=["TX_0000004935"],
    )


class CitedSourceModel(BaseModel):
    document_id: str
    document_type: str
    title: str
    topic: str
    chunk_number: int
    relevance_score: float
    excerpt: str


class GraphContextModel(BaseModel):
    customer_id: str
    device_id: Optional[str] = None
    merchant_id: str
    distinct_customers_per_device_7d: int
    shared_device_ring_detected: bool
    peer_customer_ids_on_device: List[str]
    two_hop_fraud_exposure_count: int
    two_hop_fraud_transaction_ids: List[str]
    merchants_paid_by_device_count: int
    cypher_query_template: str


class ExplainResponse(BaseModel):
    transaction_id: str
    customer_id: str
    merchant_id: str
    device_id: Optional[str] = None
    amount_paise: int
    transaction_amount: float
    payment_channel: str
    merchant_category: str
    split: str
    ground_truth_fraud_label: int
    fraud_probability: float
    risk_level: str
    recommended_action: str
    reason_codes_fired: List[str]
    risk_summary: str
    cited_sources: List[CitedSourceModel]
    graph_context: GraphContextModel
    feature_snapshot: Dict[str, Any]
    grounding_verification: Dict[str, Any]
    latency_ms: float


app = FastAPI(
    title="SENTINEL Polyglot RAG Investigator Assistant API",
    description=(
        "Real-time payment fraud investigator assistant orchestrating relational/NoSQL point-in-time "
        "features (FE-01..FE-10), Neo4j 2-hop shared-device graph context, and Qdrant 3072-d policy "
        "retrieval under DPDP Act 2023 & PCI-DSS v4.0 guardrails."
    ),
    version="1.0.0",
)

_ORCHESTRATOR: Optional[SentinelInvestigatorOrchestrator] = None


def get_orchestrator() -> SentinelInvestigatorOrchestrator:
    global _ORCHESTRATOR
    if _ORCHESTRATOR is None:
        force_offline = os.getenv("SENTINEL_OFFLINE_MOCK", "true").lower() in {"1", "true", "yes"}
        _ORCHESTRATOR = SentinelInvestigatorOrchestrator(force_offline=force_offline)
    return _ORCHESTRATOR


@app.get("/health")
def health_check() -> Dict[str, Any]:
    orch = get_orchestrator()
    return {
        "status": "healthy",
        "service": "sentinel-investigator-assistant-rag",
        "telemetry_tier": orch.telemetry_tier,
        "vector_store_backend": orch.policy_retriever.backend_mode,
        "embedding_gateway": orch.policy_retriever.embedder.active_gateway,
        "indexed_policy_documents": len(orch.policy_retriever.documents),
        "indexed_policy_chunks": len(orch.policy_retriever.chunks),
        "transactions_loaded": len(orch.tx_by_id),
        "privacy_governance": orch.privacy_audit,
        "startup_ms": orch.startup_ms,
    }


@app.get("/metrics")
def metrics_endpoint() -> Dict[str, Any]:
    orch = get_orchestrator()
    lats = orch.metrics["latencies_ms"]
    total = max(1, orch.metrics["total_explain_requests"])
    return {
        "total_explain_requests": orch.metrics["total_explain_requests"],
        "grounding_pass_rate": round(orch.metrics["total_grounding_checks_passed"] / total, 4),
        "schema_compliance_rate": round(orch.metrics["total_schema_validations_passed"] / total, 4),
        "template_fallback_invocations": orch.metrics["template_fallback_invocations"],
        "latency_p50_ms": round(float(np.percentile(lats, 50)), 2) if lats else 0.0,
        "latency_p95_ms": round(float(np.percentile(lats, 95)), 2) if lats else 0.0,
        "latency_p99_ms": round(float(np.percentile(lats, 99)), 2) if lats else 0.0,
        "sla_p99_target_ms": 250.0,
        "sla_compliant": (float(np.percentile(lats, 99)) <= 250.0) if lats else True,
    }


@app.post("/explain", response_model=ExplainResponse)
def explain_transaction_endpoint(req: ExplainRequest) -> Dict[str, Any]:
    orch = get_orchestrator()
    try:
        return orch.explain_transaction(req.transaction_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
