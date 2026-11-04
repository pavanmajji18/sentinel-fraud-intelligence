#!/usr/bin/env python3
"""Automated test suite for SENTINEL Polyglot FastAPI RAG Investigator Assistant (sentinel_rag_investigator)."""

import sys
from pathlib import Path

# Ensure application root is in python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fastapi.testclient import TestClient
from app.main import app


def test_health_and_explain_lifecycle() -> None:
    client = TestClient(app)
    h = client.get("/health")
    assert h.status_code == 200
    assert h.json()["status"] == "healthy"
    assert h.json()["indexed_policy_documents"] >= 200

    # Test 404 on unknown transaction
    r404 = client.post("/explain", json={"transaction_id": "TX_NONEXISTENT"})
    assert r404.status_code == 404

    # Test valid transaction explanation
    r200 = client.post("/explain", json={"transaction_id": "TX_0000004935"})
    assert r200.status_code == 200
    body = r200.json()
    assert body["transaction_id"] == "TX_0000004935"
    assert 0.0 <= body["fraud_probability"] <= 1.0
    assert body["risk_level"] in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
    assert body["recommended_action"] in {"ALLOW", "REVIEW", "BLOCK"}
    assert len(body["cited_sources"]) == 5
    assert body["grounding_verification"]["all_passed"] is True
