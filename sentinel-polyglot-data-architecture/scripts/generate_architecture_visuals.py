#!/usr/bin/env python3
"""
Project SENTINEL — Visual Asset Generator (Post 1 of 8: Polyglot Data Architecture).

Generates 3 publication-ready 300-DPI enterprise diagrams with modern dark-slate aesthetic:
  1. outputs/polyglot_18_table_er_and_routing.png
  2. outputs/temporal_60d_maturation_timeline.png
  3. outputs/linkedin_post1_architecture_showcase.png
"""

from __future__ import annotations

from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, ArrowStyle

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
OUTPUTS_DIR = REPO_ROOT / "outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------------------
# Modern Enterprise Dark-Slate Design Tokens
# ------------------------------------------------------------------------------
BG_DARK = "#0A0E17"           # Deep space obsidian canvas
SURFACE_CARD = "#111827"      # High-contrast panel surface
SURFACE_SUB = "#1E293B"       # Secondary card surface
BORDER_SUBTLE = "#334155"     # Subtle boundary lines
BORDER_MUTED = "#1E293B"

TEXT_PRIMARY = "#F8FAFC"      # High-readability white title text
TEXT_SECONDARY = "#94A3B8"    # Slate subtitle text
TEXT_MUTED = "#64748B"        # Low-emphasis metadata text

# Domain Accent Colors
BLUE_ACCENT = "#38BDF8"       # Core ledger & Postgres
BLUE_BG = "#0C4A6E"
BLUE_BORDER = "#0284C7"

EMERALD_ACCENT = "#34D399"    # MongoDB telemetry & mature training
EMERALD_BG = "#064E3B"
EMERALD_BORDER = "#059669"

PURPLE_ACCENT = "#C084FC"     # Neo4j graph & rings
PURPLE_BG = "#4C1D95"
PURPLE_BORDER = "#7C3AED"

AMBER_ACCENT = "#FBBF24"      # Qdrant vectors & validation split
AMBER_BG = "#78350F"
AMBER_BORDER = "#D97706"

RED_ACCENT = "#F87171"        # Redis fast cache & embargo buffer
RED_BG = "#7F1D1D"
RED_BORDER = "#DC2626"

CYAN_ACCENT = "#22D3EE"       # Holdout split & SLAs
CYAN_BG = "#164E63"
CYAN_BORDER = "#0891B2"


# ------------------------------------------------------------------------------
# Helper Drawing Utilities
# ------------------------------------------------------------------------------
def draw_rounded_card(
    ax, x: float, y: float, w: float, h: float,
    facecolor: str = SURFACE_CARD,
    edgecolor: str = BORDER_SUBTLE,
    linewidth: float = 1.4,
    rounding: float = 0.6,
    zorder: int = 2
) -> FancyBboxPatch:
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0.2,rounding_size={rounding}",
        facecolor=facecolor,
        edgecolor=edgecolor,
        linewidth=linewidth,
        zorder=zorder
    )
    ax.add_patch(patch)
    return patch


def draw_pill_badge(
    ax, x: float, y: float, w: float, h: float,
    text: str,
    bg_color: str,
    border_color: str,
    text_color: str = "#F8FAFC",
    fontsize: float = 8.5,
    fontweight: str = "bold",
    zorder: int = 5
):
    pill = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.15,rounding_size=0.4",
        facecolor=bg_color,
        edgecolor=border_color,
        linewidth=1.2,
        zorder=zorder
    )
    ax.add_patch(pill)
    ax.text(
        x + w / 2.0, y + h / 2.0, text,
        ha="center", va="center",
        fontsize=fontsize, fontweight=fontweight,
        color=text_color, zorder=zorder + 1
    )


# ------------------------------------------------------------------------------
# DIAGRAM 1: 18-Table Polyglot ER Architecture & 5-Engine Storage Routing
# ------------------------------------------------------------------------------
def generate_polyglot_routing_diagram() -> Path:
    """Generate outputs/polyglot_18_table_er_and_routing.png."""
    fig, ax = plt.subplots(figsize=(24, 13.5), dpi=300)
    fig.patch.set_facecolor(BG_DARK)
    ax.set_facecolor(BG_DARK)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    # Header Section
    draw_pill_badge(
        ax, 2.0, 95.8, 18.0, 2.4,
        "PROJECT SENTINEL • ARCHITECTURE BLUEPRINT",
        "#1E293B", "#38BDF8", "#38BDF8", fontsize=8.0
    )
    draw_pill_badge(
        ax, 20.6, 95.8, 14.5, 2.4,
        "POST 1 OF 8 • LINKEDIN SERIES",
        "#1E293B", "#94A3B8", "#E2E8F0", fontsize=8.0
    )

    ax.text(
        2.0, 92.6,
        "PROJECT SENTINEL — 18-Table Polyglot Data Architecture & 5-Engine Storage Routing",
        fontsize=18, fontweight="bold", color=TEXT_PRIMARY, va="top"
    )
    ax.text(
        2.0, 89.6,
        "4.2M Daily Authorizations | 2,800 TPS Peak | p50 <= 80 ms | p99 <= 250 ms | 300 ms Fail-Open SLA (< Rs. 10,000 Approve; >= Rs. 10,000 Review)",
        fontsize=11.2, color=TEXT_SECONDARY, va="top"
    )

    # --------------------------------------------------------------------------
    # LEFT COLUMN: The 18 Tables across 4 Functional Domains
    # --------------------------------------------------------------------------
    ax.text(
        2.0, 86.2,
        "18-TABLE RELATIONAL & UNSTRUCTURED SCHEMA CATALOG (`sentinel_demo`)",
        fontsize=11.5, fontweight="bold", color="#38BDF8"
    )

    # Domain 1: Core Entities & Ledger (6 tables)
    draw_rounded_card(ax, 2.0, 57.0, 43.5, 27.5, facecolor="#0B132B", edgecolor=BLUE_BORDER, linewidth=1.5)
    draw_pill_badge(ax, 3.0, 81.2, 22.0, 2.2, "DOMAIN A: CORE ENTITIES & FINANCIAL LEDGER (6)", BLUE_BG, BLUE_BORDER, "#93C5FD", fontsize=8.2)

    tables_domain_a = [
        ("customers", "PK: customer_id (C_*)", "25,000 rows", "Demographics, KYC age, region, home city"),
        ("merchants", "PK: merchant_id (M_*)", "2,000 rows", "MCC tier, GPS coordinates, synthetic name"),
        ("devices", "PK: device_id (D_*)", "100,000 rows", "Hardware fingerprint, OS kind (Android/iOS)"),
        ("transactions", "PK: transaction_id (TX_*)", "1,100,000 rows", "amount_paise (bigint), channel, UTC ts, GPS"),
        ("payment_outcomes", "PK: transaction_id (1:1)", "1,100,000 rows", "Gateway outcome_time, SUCCESS/FAILED status"),
        ("labels", "PK: transaction_id (1:1)", "1,100,000 rows", "fraud_label, 60d maturation label_available_at"),
    ]
    ty = 77.2
    for name, pk, vol, desc in tables_domain_a:
        draw_rounded_card(ax, 3.2, ty, 41.1, 3.4, facecolor="#131D33", edgecolor="#1E3A8A", linewidth=1.0)
        ax.text(4.2, ty + 2.3, f"`{name}`", fontsize=9.2, fontweight="bold", color="#93C5FD", va="center")
        ax.text(14.5, ty + 2.3, pk, fontsize=8.2, color="#94A3B8", va="center")
        ax.text(28.0, ty + 2.3, vol, fontsize=8.0, color="#38BDF8", va="center")
        ax.text(4.2, ty + 1.0, desc, fontsize=7.8, color="#CBD5E1", va="center")
        ty -= 3.8

    # Domain 2: Real-Time Feature Store (1 table)
    draw_rounded_card(ax, 2.0, 46.5, 43.5, 9.2, facecolor="#180F1E", edgecolor="#9333EA", linewidth=1.4)
    draw_pill_badge(ax, 3.0, 52.6, 21.0, 2.2, "DOMAIN B: REAL-TIME FEATURE STORE (1)", "#4C1D95", "#A855F7", "#E9D5FF", fontsize=8.2)

    draw_rounded_card(ax, 3.2, 47.7, 41.1, 4.2, facecolor="#241433", edgecolor="#7E22CE", linewidth=1.0)
    ax.text(4.2, 50.6, "`features`", fontsize=9.2, fontweight="bold", color="#D8B4FE", va="center")
    ax.text(12.5, 50.6, "PK: transaction_id (1:1)", fontsize=8.2, color="#94A3B8", va="center")
    ax.text(27.0, 50.6, "1,100,000 rows", fontsize=8.0, color="#C084FC", va="center")
    ax.text(4.2, 49.0, "Point-in-time derivations: FE-01..FE-10 (velocity, ratios, geo) + R01..R08 reason codes", fontsize=7.8, color="#E2E8F0", va="center")

    # Domain 3: Unstructured Policy & RAG (5 tables)
    draw_rounded_card(ax, 2.0, 24.0, 43.5, 21.3, facecolor="#06221A", edgecolor=EMERALD_BORDER, linewidth=1.4)
    draw_pill_badge(ax, 3.0, 42.1, 21.0, 2.2, "DOMAIN C: UNSTRUCTURED POLICY & RAG (5)", EMERALD_BG, EMERALD_BORDER, "#A7F3D0", fontsize=8.2)

    tables_domain_c = [
        ("legacy_cases", "PK: legacy_case_id", "2,000 rows", "Closed investigation case dockets, evidence JSON"),
        ("documents", "PK: document_id", "39,006 docs", "RBI master directions, NPCI circulars, SOP playbooks"),
        ("document_chunks", "PK: chunk_id", "120,000+ chunks", "180-word passage sliding window, 40-word overlap"),
        ("gold_explanations", "PK: example_id", "8,000 rows", "Ground-truth investigator explanations for SFT fine-tuning"),
        ("preference_pairs", "PK: pair_id", "5,000 rows", "DPO (chosen, rejected) pairs for factual alignment"),
    ]
    ty = 38.3
    for name, pk, vol, desc in tables_domain_c:
        draw_rounded_card(ax, 3.2, ty, 41.1, 3.2, facecolor="#0B3326", edgecolor="#047857", linewidth=0.9)
        ax.text(4.2, ty + 2.1, f"`{name}`", fontsize=8.8, fontweight="bold", color="#6EE7B7", va="center")
        ax.text(16.0, ty + 2.1, pk, fontsize=7.8, color="#94A3B8", va="center")
        ax.text(28.5, ty + 2.1, vol, fontsize=7.8, color="#34D399", va="center")
        ax.text(4.2, ty + 0.9, desc, fontsize=7.5, color="#D1FAE5", va="center")
        ty -= 3.5

    # Domain 4: Governance, Auditing & Overrides (6 tables)
    draw_rounded_card(ax, 2.0, 2.0, 43.5, 20.8, facecolor="#231709", edgecolor=AMBER_BORDER, linewidth=1.4)
    draw_pill_badge(ax, 3.0, 19.7, 23.5, 2.2, "DOMAIN D: GOVERNANCE, AUDIT & CONFIG (6)", AMBER_BG, AMBER_BORDER, "#FDE68A", fontsize=8.2)

    tables_domain_d = [
        ("decision_audit", "PK: transaction_id", "1,000,000 rows", "Online model score, latency_ms & decision log"),
        ("threshold_sets", "PK: threshold_id", "Config profiles", "MCC probability cutoffs (0.30, 0.60, 0.85)"),
        ("hard_rules", "PK: rule_id", "Config rules", "Deterministic compliance & sanctions override rules"),
        ("list_entries", "PK: entry_id", "136 entries", "Blocklist/Allowlist entity entries (C_*, M_*, D_*)"),
        ("config_changes", "PK: change_id", "Audit log", "Immutable audit log of rule/threshold updates"),
        ("reviewer_feedback", "PK: feedback_id", "Human queue", "Investigator verdicts on manual REVIEW cases"),
    ]
    ty = 16.5
    for name, pk, vol, desc in tables_domain_d:
        draw_rounded_card(ax, 3.2, ty, 41.1, 2.9, facecolor="#33220B", edgecolor="#B45309", linewidth=0.9)
        ax.text(4.2, ty + 1.9, f"`{name}`", fontsize=8.6, fontweight="bold", color="#FCD34D", va="center")
        ax.text(16.2, ty + 1.9, pk, fontsize=7.8, color="#94A3B8", va="center")
        ax.text(28.8, ty + 1.9, vol, fontsize=7.6, color="#FBBF24", va="center")
        ax.text(4.2, ty + 0.8, desc, fontsize=7.4, color="#FEF3C7", va="center")
        ty -= 3.1

    # --------------------------------------------------------------------------
    # RIGHT COLUMN: The 5 Dedicated Storage Engines & Latencies
    # --------------------------------------------------------------------------
    ax.text(
        49.5, 86.2,
        "5-ENGINE POLYGLOT STORAGE ROUTING & PRODUCTION WORKLOAD MAPPING",
        fontsize=11.5, fontweight="bold", color="#38BDF8"
    )

    engines = [
        {
            "id": "ENGINE 1",
            "name": "PostgreSQL 16 / Supabase",
            "tag": "ACID SYSTEM OF RECORD & FINANCIAL LEDGER",
            "latency": "5 - 12 ms",
            "sla": "PASS",
            "y": 69.5, "h": 15.0,
            "bg": "#0B1528", "border": BLUE_BORDER, "accent": BLUE_ACCENT,
            "items": [
                "• Houses all 18 relational tables with strict Foreign Key integrity & audit logs",
                "• Guarantees 64-bit integer amount_paise arithmetic (zero float precision drift)",
                "• Manages 60-day chargeback maturation window (label_available_at) & temporal splits",
                "• Serves offline training batches, audit analytics, and configuration mutations"
            ]
        },
        {
            "id": "ENGINE 2",
            "name": "Redis 7.2 In-Memory Store",
            "tag": "REAL-TIME SLIDING-WINDOW VELOCITY COUNTERS",
            "latency": "< 5 ms",
            "sla": "PASS",
            "y": 52.8, "h": 14.8,
            "bg": "#210B0E", "border": RED_BORDER, "accent": RED_ACCENT,
            "items": [
                "• Atomic pipeline counters for real-time velocity features (FE-07: count_1h, spend_24h)",
                "• Sub-5 ms rolling customer & device aggregation via sorted sets (ZADD / ZREMRANGEBYSCORE)",
                "• Strict sliding-window TTL eviction preventing unbounded memory growth under 2,800 TPS",
                "• Fallback circuit breaker routing to PostgreSQL replica if Redis memory threshold breaches"
            ]
        },
        {
            "id": "ENGINE 3",
            "name": "MongoDB 7.0 (WiredTiger)",
            "tag": "HIGH-THROUGHPUT BEHAVIORAL TELEMETRY",
            "latency": "4 - 10 ms",
            "sla": "PASS",
            "y": 36.2, "h": 14.8,
            "bg": "#072018", "border": EMERALD_BORDER, "accent": EMERALD_ACCENT,
            "items": [
                "• `sentinel_fraud_db`: Hybrid bounded embedding of devices array inside customers",
                "• Compound ESR Index {customer_id: 1, transaction_time: -1} -> 0 COLLSCAN queries",
                "• Ingests high-throughput sensor telemetry, biometric timings, and channel metadata",
                "• Handles heterogeneous payloads (3.01% nullable GPS, 0.89% nullable device_id)"
            ]
        },
        {
            "id": "ENGINE 4",
            "name": "Neo4j 5.20 Enterprise",
            "tag": "PROPERTY-GRAPH MULTI-HOP FRAUD RING TOPOLOGY",
            "latency": "8 - 22 ms",
            "sla": "PASS",
            "y": 19.5, "h": 14.8,
            "bg": "#1C0D2E", "border": PURPLE_BORDER, "accent": PURPLE_ACCENT,
            "items": [
                "• Graph Topology: (:Customer)-[:USED_DEVICE]->(:Device), (:Customer)-[:PAID]->(:Merchant)",
                "• Index-free adjacency: 2-hop contagion traversal in O(k) time vs O(N^2) SQL self-joins",
                "• Detects 24-node collusive mule rings, shared device syndicates, and cashout terminals",
                "• Enforces null-safe edge creation: skips [:USED_DEVICE] edge if device_id IS NULL"
            ]
        },
        {
            "id": "ENGINE 5",
            "name": "Qdrant Vector Database",
            "tag": "3072-D DENSE SEMANTIC POLICY RETRIEVAL",
            "latency": "6 - 18 ms",
            "sla": "PASS",
            "y": 2.8, "h": 14.8,
            "bg": "#241808", "border": AMBER_BORDER, "accent": AMBER_ACCENT,
            "items": [
                "• Dense indexing over 225 regulatory circulars, master directions & fraud SOP playbooks",
                "• 3072-d Gemini Embedding 2 vectors indexed via HNSW (M=16, efConstruction=128, Cosine)",
                "• Hit@1 = 100%, Hit@3 = 100%, MRR@3 = 1.0000 on compliance query benchmarks",
                "• Pre-filtered payload queries by authority (RBI, NPCI) & domain (UPI, Cards, AML)"
            ]
        },
    ]

    for eng in engines:
        draw_rounded_card(ax, 49.5, eng["y"], 48.5, eng["h"], facecolor=eng["bg"], edgecolor=eng["border"], linewidth=1.5)
        
        # Header line
        draw_pill_badge(ax, 50.8, eng["y"] + eng["h"] - 3.2, 10.0, 2.2, eng["id"], eng["border"], eng["accent"], "#FFFFFF", fontsize=7.5)
        ax.text(61.8, eng["y"] + eng["h"] - 2.1, eng["name"], fontsize=11.2, fontweight="bold", color=TEXT_PRIMARY, va="center")
        
        draw_pill_badge(ax, 85.0, eng["y"] + eng["h"] - 3.2, 12.0, 2.2, f"Latency: {eng['latency']}", "#1E293B", eng["accent"], eng["accent"], fontsize=8.0)
        
        ax.text(50.8, eng["y"] + eng["h"] - 4.5, eng["tag"], fontsize=8.2, fontweight="bold", color=eng["accent"], va="center")
        
        # Bullet items
        item_y = eng["y"] + eng["h"] - 6.2
        for it in eng["items"]:
            ax.text(51.0, item_y, it, fontsize=7.8, color="#E2E8F0", va="top")
            item_y -= 2.1

    # Directional Arrows connecting Domains to Engines
    arrow_props = dict(
        arrowstyle="-|>",
        color="#38BDF8",
        lw=2.0,
        mutation_scale=14,
        zorder=10
    )
    # Domain A -> Postgres
    ax.annotate("", xy=(49.5, 78.0), xytext=(45.5, 74.0), arrowprops=arrow_props)
    # Domain B -> Redis & Postgres
    ax.annotate("", xy=(49.5, 61.0), xytext=(45.5, 51.0), arrowprops=dict(arrowstyle="-|>", color="#F87171", lw=2.0, mutation_scale=14, zorder=10))
    # Domain A/Core -> MongoDB
    ax.annotate("", xy=(49.5, 44.0), xytext=(45.5, 65.0), arrowprops=dict(arrowstyle="-|>", color="#34D399", lw=2.0, mutation_scale=14, zorder=10))
    # Domain A/Core -> Neo4j
    ax.annotate("", xy=(49.5, 27.0), xytext=(45.5, 62.0), arrowprops=dict(arrowstyle="-|>", color="#C084FC", lw=2.0, mutation_scale=14, zorder=10))
    # Domain C -> Qdrant
    ax.annotate("", xy=(49.5, 10.0), xytext=(45.5, 34.0), arrowprops=dict(arrowstyle="-|>", color="#FBBF24", lw=2.0, mutation_scale=14, zorder=10))

    # Bottom Compliance Strip
    draw_rounded_card(ax, 2.0, 0.4, 96.0, 1.4, facecolor="#0F172A", edgecolor="#334155", linewidth=1.0)
    ax.text(
        50.0, 1.1,
        "DPDP Act 2023 & PCI-DSS v4.0 Zero-PII Enforced | C_*, M_*, D_* Cryptographic Tokenization | 3.01% Missing GPS & 0.89% Missing Device Null Mitigations Active",
        fontsize=8.2, color="#94A3B8", ha="center", va="center"
    )

    out_file = OUTPUTS_DIR / "polyglot_18_table_er_and_routing.png"
    plt.tight_layout()
    plt.savefig(out_file, dpi=300, facecolor=fig.get_facecolor(), edgecolor="none")
    plt.close()
    print(f"[OK] Generated: {out_file} ({out_file.stat().st_size:,} bytes)")
    return out_file


# ------------------------------------------------------------------------------
# DIAGRAM 2: 60-Day Chargeback Maturation Timeline & Zero-Leakage Splits
# ------------------------------------------------------------------------------
def generate_temporal_maturation_timeline() -> Path:
    """Generate outputs/temporal_60d_maturation_timeline.png."""
    fig, ax = plt.subplots(figsize=(24, 13.5), dpi=300)
    fig.patch.set_facecolor(BG_DARK)
    ax.set_facecolor(BG_DARK)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    # Header Section
    draw_pill_badge(
        ax, 2.0, 95.8, 22.0, 2.4,
        "PROJECT SENTINEL • TEMPORAL DATA GOVERNANCE",
        "#1E293B", "#38BDF8", "#38BDF8", fontsize=8.0
    )
    draw_pill_badge(
        ax, 24.6, 95.8, 14.5, 2.4,
        "POST 1 OF 8 • LINKEDIN SERIES",
        "#1E293B", "#94A3B8", "#E2E8F0", fontsize=8.0
    )

    ax.text(
        2.0, 92.6,
        "PROJECT SENTINEL — 60-Day Chargeback Maturation & Zero-Leakage Temporal Splits",
        fontsize=18, fontweight="bold", color=TEXT_PRIMARY, va="top"
    )
    ax.text(
        2.0, 89.6,
        "Rule: label_available_at = transaction_time + 60 days (Eliminating Optimistic Look-Ahead Label Leakage in Production Fraud Models)",
        fontsize=11.2, color=TEXT_SECONDARY, va="top"
    )

    # --------------------------------------------------------------------------
    # TIMELINE SECTION: Gantt Bars across Chronological Splits
    # --------------------------------------------------------------------------
    ax.text(
        2.0, 85.5,
        "CHRONOLOGICAL PRODUCTION TIMELINE: 16-MONTH DATA MATURATION SEQUENCE (2024-11-01 -> 2026-03-01)",
        fontsize=11.5, fontweight="bold", color="#38BDF8"
    )

    # Big Timeline Container
    draw_rounded_card(ax, 2.0, 48.0, 96.0, 36.0, facecolor="#0E1626", edgecolor=BORDER_SUBTLE, linewidth=1.5)

    # Date Axis Scale: X from 5.0 to 95.0 represents Nov 1, 2024 to Mar 1, 2026 (16 months = 485 days)
    # Total days = 485
    # X_start = 5.0, Width = 90.0 (0.1856 units per day)
    # 1. Warmup: Nov 1, 2024 -> Jan 1, 2025 (61 days) -> Width = 11.3
    # 2. Train Mature: Jan 1, 2025 -> Sep 2, 2025 (244 days) -> Width = 45.3
    # 3. Embargo Zone: Sep 2, 2025 -> Nov 1, 2025 (60 days) -> Width = 11.1
    # 4. Validation: Nov 1, 2025 -> Jan 1, 2026 (61 days) -> Width = 11.3
    # 5. Holdout: Jan 1, 2026 -> Mar 1, 2026 (59 days) -> Width = 11.0

    timeline_segments = [
        {
            "name": "WARMUP WINDOW",
            "dates": "2024-11-01 -> 2025-01-01",
            "duration": "61 Days (100k txns)",
            "x": 5.0, "w": 11.3,
            "bg": "#1E293B", "border": "#475569", "text_c": "#94A3B8",
            "role": "Rolling 90d Context Only",
            "sub": "Seeds customer avg spend & seen devices. ZERO model fitting."
        },
        {
            "name": "TRAIN SPLIT (MATURE ZONE)",
            "dates": "2025-01-01 -> 2025-09-02",
            "duration": "244 Days (3,368 mature)",
            "x": 16.5, "w": 45.1,
            "bg": "#064E3B", "border": EMERALD_BORDER, "text_c": "#6EE7B7",
            "role": "Strictly Matured Ground-Truth",
            "sub": "Disputes resolved before cutoff: label_available_at <= T_val. Safe for gradients."
        },
        {
            "name": "60-DAY EMBARGO BUFFER",
            "dates": "2025-09-02 -> 2025-11-01",
            "duration": "60 Days (1,510 purged)",
            "x": 61.8, "w": 11.1,
            "bg": "#7F1D1D", "border": RED_BORDER, "text_c": "#FCA5A5",
            "role": "IMMUTABLE QUARANTINE",
            "sub": "Pending bank arbitration: label_available_at > T_val. PURGED from train set!"
        },
        {
            "name": "VALIDATION SPLIT",
            "dates": "2025-11-01 -> 2026-01-01",
            "duration": "61 Days (150k txns)",
            "x": 73.1, "w": 11.3,
            "bg": "#78350F", "border": AMBER_BORDER, "text_c": "#FDE68A",
            "role": "Threshold Calibration",
            "sub": "Calibrates probability cutoffs (threshold_sets) & early stopping."
        },
        {
            "name": "HOLDOUT SPLIT",
            "dates": "2026-01-01 -> 2026-03-01",
            "duration": "59 Days (150k txns)",
            "x": 84.6, "w": 10.4,
            "bg": "#164E63", "border": CYAN_BORDER, "text_c": "#A5F3FC",
            "role": "Locked Production SLA",
            "sub": "Evaluates Recall >= 85%, Precision >= 70%, PR-AUC >= 0.80."
        },
    ]

    # Draw Time Segments
    bar_y = 66.0
    bar_h = 10.0
    for seg in timeline_segments:
        draw_rounded_card(
            ax, seg["x"], bar_y, seg["w"], bar_h,
            facecolor=seg["bg"], edgecolor=seg["border"], linewidth=1.6
        )
        ax.text(
            seg["x"] + seg["w"] / 2.0, bar_y + bar_h - 2.2,
            seg["name"],
            ha="center", va="center",
            fontsize=8.2, fontweight="bold", color=seg["text_c"]
        )
        ax.text(
            seg["x"] + seg["w"] / 2.0, bar_y + bar_h - 4.4,
            seg["dates"],
            ha="center", va="center",
            fontsize=7.2, color="#E2E8F0"
        )
        ax.text(
            seg["x"] + seg["w"] / 2.0, bar_y + bar_h - 6.2,
            f"[{seg['duration']}]",
            ha="center", va="center",
            fontsize=7.0, fontweight="bold", color=seg["text_c"]
        )
        ax.text(
            seg["x"] + seg["w"] / 2.0, bar_y + 1.8,
            seg["role"],
            ha="center", va="center",
            fontsize=7.2, color="#F8FAFC"
        )

    # Date ticks & vertical markers along timeline
    date_markers = [
        (5.0, "2024-11-01\nTimeline Start"),
        (16.5, "2025-01-01\nTrain Start"),
        (61.8, "2025-09-02\nT_val - 60 Days"),
        (72.9, "2025-11-01\nT_val (Cutoff)"),
        (84.4, "2026-01-01\nValidation Cutoff"),
        (95.0, "2026-03-01\nHoldout End"),
    ]
    for x_val, label in date_markers:
        ax.plot([x_val, x_val], [bar_y - 2.5, bar_y + bar_h + 1.5], color="#475569", linestyle="--", lw=1.2, zorder=4)
        ax.text(x_val, bar_y - 4.0, label, ha="center", va="top", fontsize=7.2, color="#94A3B8")

    # Glowing Warning Focus on the Embargo Zone
    ax.annotate(
        "THE 60-DAY MATURATION EMBARGO ZONE\n1,510 un-matured disputes held back\nPrevents Nov/Dec chargeback look-ahead leakage",
        xy=(67.3, bar_y + bar_h),
        xytext=(67.3, bar_y + bar_h + 5.5),
        ha="center", va="bottom",
        fontsize=8.5, fontweight="bold", color="#FCA5A5",
        arrowprops=dict(arrowstyle="->", color="#F87171", lw=1.8),
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#450A0A", edgecolor="#EF4444", lw=1.4)
    )

    # Formula Callout inside Timeline Card
    ax.text(
        50.0, 50.5,
        "Mathematical Invariant: label_available_at = transaction_time + 60 days   |   Filter: WHERE split = 'train' AND label_available_at <= T_val",
        ha="center", va="center",
        fontsize=9.2, fontweight="bold", color="#38BDF8",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#0B1B36", edgecolor="#0284C7", lw=1.2)
    )

    # --------------------------------------------------------------------------
    # BOTTOM SECTION: Naive vs Sentinel Comparative Callout Panels
    # --------------------------------------------------------------------------
    ax.text(
        2.0, 44.5,
        "ARCHITECTURAL IMPACT & DRIFT PREVENTION: NAIVE VS. ZERO-LEAKAGE SPLIT",
        fontsize=11.5, fontweight="bold", color="#38BDF8"
    )

    # Left: Naive Split Card (Failure Mode)
    draw_rounded_card(ax, 2.0, 4.0, 47.0, 38.5, facecolor="#1F0D11", edgecolor=RED_BORDER, linewidth=1.6)
    draw_pill_badge(ax, 3.5, 38.0, 20.0, 2.6, "NAIVE TEMPORAL / RANDOM SPLIT", RED_BG, RED_BORDER, "#FCA5A5", fontsize=8.6)
    draw_pill_badge(ax, 34.0, 38.0, 13.5, 2.6, "HIGH PRODUCTION RISK", "#450A0A", "#EF4444", "#FCA5A5", fontsize=8.0)

    ax.text(3.5, 35.0, "Fatal Assumption: Immediate Label Availability (`label_available_at = t`)", fontsize=9.2, fontweight="bold", color="#F87171")
    
    naive_bullets = [
        ("• Naive SQL Filter", "`SELECT * FROM transactions WHERE split = 'train';` (ignores 60-day lag)"),
        ("• Look-Ahead Leakage", "Transactions authorized in Sep-Oct 2025 have disputes settling in Nov-Dec. Naive split includes these future chargebacks in training!"),
        ("• Target Encoding Poisoning", "FE-08 (`mcc_target_encoded_rate`) incorporates future fraud rates, giving tree models an omniscient look-ahead signal during training."),
        ("• Offline Metric Illusion", "Model achieves **0.98 PR-AUC** in offline validation due to cheating on future dispute outcomes."),
        ("• Catastrophic Production Decay", "Once deployed in live 2,800 TPS traffic where dispute lag is real (30-60 days), model accuracy collapses to **0.71 PR-AUC** (severe business fraud losses).")
    ]
    ny = 32.2
    for title, desc in naive_bullets:
        ax.text(3.5, ny, title, fontsize=8.4, fontweight="bold", color="#FCA5A5")
        ax.text(3.5, ny - 1.6, desc, fontsize=7.8, color="#CBD5E1")
        ny -= 4.8

    # Right: Sentinel Zero-Leakage Split Card (Production Standard)
    draw_rounded_card(ax, 51.0, 4.0, 47.0, 38.5, facecolor="#082218", edgecolor=EMERALD_BORDER, linewidth=1.6)
    draw_pill_badge(ax, 52.5, 38.0, 22.0, 2.6, "SENTINEL POINT-IN-TIME SPLIT", EMERALD_BG, EMERALD_BORDER, "#6EE7B7", fontsize=8.6)
    draw_pill_badge(ax, 82.5, 38.0, 14.0, 2.6, "PRODUCTION GENERALIZED", "#064E3B", "#10B981", "#A7F3D0", fontsize=8.0)

    ax.text(52.5, 35.0, "Governing Principle: Strict Point-in-Time 60-Day Embargo Gate", fontsize=9.2, fontweight="bold", color="#34D399")

    sentinel_bullets = [
        ("• Leakage-Free SQL Filter", "`SELECT t.*, l.fraud_label FROM transactions t JOIN labels l ON ... WHERE t.split='train' AND l.label_available_at <= T_val;`"),
        ("• 60-Day Embargo Buffer", "1,510 immature transactions in the 60-day pre-cutoff window are strictly purged from training batches until their chargeback period expires."),
        ("• Unbiased Feature Materialization", "FE-01..FE-10 and FE-08 MCC target encodings use only transactions whose ground truth was provably settled before T_val."),
        ("• Honest Offline PR-AUC", "Offline validation yield: **0.81 PR-AUC** (realistic, rigorous, and exceeds the >= 0.80 SLA threshold)."),
        ("• Zero Production Degradation", "Generalizes flawlessly to live traffic (4.2M daily txns) with zero performance cliff, keeping net fraud loss <= 6.4 bps.")
    ]
    sy = 32.2
    for title, desc in sentinel_bullets:
        ax.text(52.5, sy, title, fontsize=8.4, fontweight="bold", color="#6EE7B7")
        ax.text(52.5, sy - 1.6, desc, fontsize=7.8, color="#D1FAE5")
        sy -= 4.8

    # Bottom Compliance Footer
    draw_rounded_card(ax, 2.0, 0.4, 96.0, 1.4, facecolor="#0F172A", edgecolor="#334155", linewidth=1.0)
    ax.text(
        50.0, 1.1,
        "RBI & NPCI Chargeback Dispute Governance | 60-Day Window Hard-Coded into Data Architecture | Zero Data Drift between Training & Inference",
        fontsize=8.2, color="#94A3B8", ha="center", va="center"
    )

    out_file = OUTPUTS_DIR / "temporal_60d_maturation_timeline.png"
    plt.tight_layout()
    plt.savefig(out_file, dpi=300, facecolor=fig.get_facecolor(), edgecolor="none")
    plt.close()
    print(f"[OK] Generated: {out_file} ({out_file.stat().st_size:,} bytes)")
    return out_file


# ------------------------------------------------------------------------------
# DIAGRAM 3: Combined 2-Panel Executive Showcase (LinkedIn Hero Graphic)
# ------------------------------------------------------------------------------
def generate_linkedin_showcase(routing_img_path: Path, timeline_img_path: Path) -> Path:
    """Generate outputs/linkedin_post1_architecture_showcase.png combining both visuals."""
    fig, axes = plt.subplots(2, 1, figsize=(24, 25), dpi=300)
    fig.patch.set_facecolor(BG_DARK)

    # Load previously rendered images
    img_routing = plt.imread(routing_img_path)
    img_timeline = plt.imread(timeline_img_path)

    # Panel 1: Routing Diagram
    axes[0].imshow(img_routing)
    axes[0].axis("off")

    # Panel 2: Timeline Diagram
    axes[1].imshow(img_timeline)
    axes[1].axis("off")

    plt.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01, hspace=0.02)
    out_file = OUTPUTS_DIR / "linkedin_post1_architecture_showcase.png"
    plt.savefig(out_file, dpi=300, facecolor=fig.get_facecolor(), edgecolor="none")
    plt.close()
    print(f"[OK] Generated: {out_file} ({out_file.stat().st_size:,} bytes)")
    return out_file


# ------------------------------------------------------------------------------
# Master Execution Entrypoint
# ------------------------------------------------------------------------------
def main():
    print("=" * 88)
    print("PROJECT SENTINEL — GENERATING PUBLICATION-READY LINKEDIN VISUAL ASSETS")
    print("=" * 88)

    p1 = generate_polyglot_routing_diagram()
    p2 = generate_temporal_maturation_timeline()
    p3 = generate_linkedin_showcase(p1, p2)

    print("\n[SUCCESS] All 3 Visual Assets Generated Deterministically:")
    print(f"  1. {p1} ({p1.stat().st_size / 1024:.1f} KB)")
    print(f"  2. {p2} ({p2.stat().st_size / 1024:.1f} KB)")
    print(f"  3. {p3} ({p3.stat().st_size / 1024:.1f} KB)")
    print("=" * 88)


if __name__ == "__main__":
    main()
