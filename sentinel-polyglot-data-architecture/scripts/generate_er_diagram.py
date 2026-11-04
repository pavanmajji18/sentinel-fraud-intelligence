#!/usr/bin/env python3
"""
Generate the 4K UHD (3840x2160) Entity-Relationship (ER) architecture diagram
for the 18-table `sentinel_demo` schema in Project SENTINEL.
"""

from __future__ import annotations

from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_er_diagram() -> Path:
    """Render the 18-table Entity-Relationship diagram across all 4 architectural domains."""
    fig, ax = plt.subplots(figsize=(24, 13.5), dpi=160)
    fig.patch.set_facecolor("#0B1120")
    ax.set_facecolor("#0B1120")
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(
        2.0, 96.2,
        "PROJECT SENTINEL — 18-TABLE ER ARCHITECTURE (sentinel_demo)",
        fontsize=17.5, fontweight="bold", color="#F8FAFC", family="DejaVu Sans"
    )
    ax.text(
        2.0, 93.3,
        "Tier-1 RBI-Regulated Digital Payment Aggregator | 1.1M Authorizations | DPDP Act 2023 & PCI-DSS v4.0 Tokenized (C_*, M_*, D_*, is_synthetic=True)",
        fontsize=10.2, color="#94A3B8", family="DejaVu Sans"
    )

    legend_items = [
        ("Domain A: Core Entities & Telemetry (7)", "#1E3A8A", "#60A5FA"),
        ("Domain B: Unstructured & RAG (3)", "#064E3B", "#34D399"),
        ("Domain C: Alignment & Scoring Audit (3)", "#4C1D95", "#C084FC"),
        ("Domain D: Policy & Feedback Config (5)", "#78350F", "#FBBF24"),
    ]
    lx = 52.0
    for label, bg_c, border_c in legend_items:
        patch = FancyBboxPatch(
            (lx, 95.2), 11.2, 2.8,
            boxstyle="round,pad=0.25,rounding_size=0.5",
            facecolor=bg_c, edgecolor=border_c, linewidth=1.6
        )
        ax.add_patch(patch)
        ax.text(lx + 5.6, 96.6, label, fontsize=8.3, fontweight="bold", color="#F8FAFC", ha="center", va="center")
        lx += 11.7

    def draw_table_card(
        x: float, y: float, w: float, h: float,
        num: str, name: str, rows_str: str, grain: str,
        pk: str, fk: str, cols: str,
        header_bg: str, border_col: str
    ) -> None:
        body = FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.15,rounding_size=0.6",
            facecolor="#111827", edgecolor=border_col, linewidth=1.8, zorder=3
        )
        ax.add_patch(body)
        hh = 2.7
        hdr = FancyBboxPatch(
            (x, y + h - hh), w, hh,
            boxstyle="round,pad=0.15,rounding_size=0.5",
            facecolor=header_bg, edgecolor=border_col, linewidth=1.2, zorder=4
        )
        ax.add_patch(hdr)
        ax.text(x + 0.7, y + h - hh / 2, f"[{num}] {name}", fontsize=10.2, fontweight="bold", color="#FFFFFF", va="center", zorder=5)
        ax.text(x + w - 0.6, y + h - hh / 2, rows_str, fontsize=8.4, fontweight="bold", color="#E2E8F0", ha="right", va="center", zorder=5)

        ty = y + h - hh - 1.35
        ax.text(x + 0.7, ty, f"Grain: {grain}", fontsize=8.2, color="#CBD5E1", va="center", zorder=5)
        ty -= 1.55
        ax.text(x + 0.7, ty, f"PK: {pk}", fontsize=8.3, fontweight="bold", color="#38BDF8", va="center", zorder=5)
        ty -= 1.55
        ax.text(x + 0.7, ty, f"FK: {fk}", fontsize=8.0, color="#FBBF24" if fk != "None" else "#64748B", va="center", zorder=5)
        ty -= 1.65
        ax.text(x + 0.7, ty, f"Cols: {cols}", fontsize=7.6, color="#94A3B8", va="center", zorder=5)

    # Column 1: Core Master Entities & Unstructured RAG Lineage
    c1_x, c1_w = 2.0, 21.0
    draw_table_card(
        c1_x, 76.5, c1_w, 11.0, "01", "customers", "25k full | 2.5k rep",
        "1 registered payer account", "customer_id (C_<16-hex>)", "None",
        "created_at, customer_age, home_city, region",
        "#1E3A8A", "#60A5FA"
    )
    draw_table_card(
        c1_x, 63.0, c1_w, 11.0, "02", "merchants", "2k full | 200 rep",
        "1 onboarded merchant terminal", "merchant_id (M_<16-hex>)", "None",
        "synthetic_name, merchant_category, merchant_location",
        "#1E3A8A", "#60A5FA"
    )
    draw_table_card(
        c1_x, 49.5, c1_w, 11.0, "03", "devices", "100k full | 10.1k rep",
        "1 hardware device fingerprint", "device_id (D_<16-hex>)", "None",
        "device_kind (android, ios, desktop_web)",
        "#1E3A8A", "#60A5FA"
    )
    draw_table_card(
        c1_x, 34.0, c1_w, 11.5, "08", "legacy_cases", "2k full | 2 rep",
        "1 closed fraud investigation docket", "legacy_case_id", "customer_id -> customers",
        "transaction_time, closed_at, amount_paise, evidence",
        "#064E3B", "#34D399"
    )
    draw_table_card(
        c1_x, 18.5, c1_w, 12.0, "09", "documents", "39,006 full | 5,256 rep",
        "1 policy, playbook, RBI/NPCI or case doc", "document_id", "transaction_id?, legacy_case_id?",
        "kind, title, split, content, metadata (JSONB)",
        "#064E3B", "#34D399"
    )
    draw_table_card(
        c1_x, 4.0, c1_w, 11.5, "10", "document_chunks", "120k+ full | 16k rep",
        "1 text chunk (180w, 40w overlap)", "chunk_id", "document_id -> documents",
        "chunk_index, content, word_count, split (3072-d Qdrant)",
        "#064E3B", "#34D399"
    )

    # Column 2: Central Authorization Hub & 1:1 Satellites
    c2_x, c2_w = 27.5, 24.5
    draw_table_card(
        c2_x, 66.0, c2_w, 16.5, "04", "transactions (CENTRAL FACT HUB)", "1.1M full | 100k rep",
        "1 payment authorization attempt (incl. declines)", "transaction_id (TX_<10d>)",
        "customer_id, merchant_id, device_id (nullable)",
        "amount_paise (bigint), transaction_amount, payment_channel,\n      merchant_category, transaction_time (UTC), split",
        "#1D4ED8", "#38BDF8"
    )
    draw_table_card(
        c2_x, 51.0, c2_w, 11.5, "05", "payment_outcomes (1:1 Satellite)", "1.1M full | 100k rep",
        "1 terminal gateway settlement status", "transaction_id", "transaction_id -> transactions",
        "outcome_time (t + 2s), status (SUCCESS/FAILED), failure_code",
        "#1E3A8A", "#60A5FA"
    )
    draw_table_card(
        c2_x, 35.5, c2_w, 12.5, "06", "labels (1:1 Ground Truth + 60d)", "1.1M (0.6% pos)",
        "1 ground-truth verdict after 60d maturation", "transaction_id", "transaction_id -> transactions",
        "fraud_label (0/1), label_source, label_available_at (t + 60d),\n      split, eligible_at_next_split_start",
        "#1E3A8A", "#60A5FA"
    )
    draw_table_card(
        c2_x, 18.5, c2_w, 14.0, "07", "features (1:1 Point-in-Time Vector)", "1.1M full | 100k rep",
        "1 derived feature vector at feature_as_of = t", "transaction_id", "transaction_id -> transactions",
        "FE-01..FE-10 (amount_to_avg_ratio, is_new_device, distance_km,\n      is_night, txn_count_last_1h, reason_codes R01..R08)",
        "#1E3A8A", "#60A5FA"
    )
    draw_table_card(
        c2_x, 4.0, c2_w, 11.5, "18", "reviewer_feedback", "Fixtures | Analyst Queue",
        "1 human investigator verdict on REVIEW txn", "feedback_id", "transaction_id -> transactions",
        "reviewed_at, verdict (CONFIRMED_FRAUD / FALSE_POS), corrected_explanation",
        "#78350F", "#FBBF24"
    )

    # Column 3: LLM Alignment & Scoring Audit
    c3_x, c3_w = 56.5, 20.5
    draw_table_card(
        c3_x, 75.5, c3_w, 12.0, "11", "gold_explanations", "8,000 full | 8,500 rep",
        "1 verified JSON investigator explanation (SFT)", "example_id", "transaction_id -> transactions",
        "split, prompt, target (strict JSON schema), fraud_label",
        "#4C1D95", "#C084FC"
    )
    draw_table_card(
        c3_x, 60.5, c3_w, 12.0, "12", "preference_pairs", "5,000 full | 5,000 rep",
        "1 (chosen, rejected) explanation pair (DPO)", "pair_id", "transaction_id -> transactions",
        "split, prompt, chosen, rejected, rejection_reason",
        "#4C1D95", "#C084FC"
    )
    draw_table_card(
        c3_x, 44.5, c3_w, 13.0, "13", "decision_audit", "1.0M full | Subset rep",
        "1 online scoring event & latency audit log", "transaction_id", "transaction_id -> transactions",
        "scored_at, fraud_probability, decision (APPROVE/REVIEW/BLOCK),\n      risk_level, reason_codes, model_version, latency_ms",
        "#4C1D95", "#C084FC"
    )

    # Column 4: Policy Governance & Config Tables
    c4_x, c4_w = 78.5, 19.5
    draw_table_card(
        c4_x, 75.5, c4_w, 12.0, "14", "threshold_sets", "Config | MCC Cutoffs",
        "1 probability cutoff profile per MCC group", "threshold_id", "None (governs decision_audit)",
        "mcc_group, medium_from (0.30), high_from (0.60), critical (0.85)",
        "#78350F", "#FBBF24"
    )
    draw_table_card(
        c4_x, 60.5, c4_w, 12.0, "15", "hard_rules", "Config | Override Rules",
        "1 deterministic compliance override rule", "rule_id", "None (overrides decision_audit)",
        "code, expression, action (BLOCK/REVIEW), priority, enabled",
        "#78350F", "#FBBF24"
    )
    draw_table_card(
        c4_x, 45.5, c4_w, 12.0, "16", "list_entries", "136 full | Allow/Block",
        "1 entity blocklist or allowlist record", "entry_id", "None (polymorphic entity_id)",
        "list_type (BLOCK/ALLOW), entity_id (C_*/M_*/D_*), effective_from",
        "#78350F", "#FBBF24"
    )
    draw_table_card(
        c4_x, 30.5, c4_w, 12.0, "17", "config_changes", "Config | Mutation Log",
        "1 immutable audit record of rule/cutoff change", "change_id", "None (polymorphic entity_id)",
        "entity, entity_id, before_value, after_value, changed_at",
        "#78350F", "#FBBF24"
    )

    poly_box = FancyBboxPatch(
        (56.5, 4.0), 41.5, 23.5,
        boxstyle="round,pad=0.25,rounding_size=0.7",
        facecolor="#0F172A", edgecolor="#38BDF8", linewidth=2.0, zorder=3
    )
    ax.add_patch(poly_box)
    ax.text(
        58.0, 25.2,
        "POLYGLOT 4-DATABASE + REDIS STORAGE MAPPING & TEMPORAL INVARIANTS",
        fontsize=11.0, fontweight="bold", color="#38BDF8", zorder=5
    )
    bullets_poly = [
        "1. PostgreSQL / Supabase + Redis (5-12ms): System of record for all 18 tables; ACID joins on transaction_id; Redis caches 1h/24h/90d rolling counters.",
        "2. MongoDB (4-10ms): customers (embedded bounded devices array) + transactions indexed via ESR {customer_id: 1, transaction_time: -1}.",
        "3. Neo4j (8-22ms): (:Customer)-[:USED_DEVICE]->(:Device) & [:PAID_MERCHANT]->(:Merchant) for 2-hop fraud rings & shared devices (>=3 customers in 7d).",
        "4. Qdrant (6-18ms): sentinel_policy_corpus storing 3072-d Gemini Embedding 2 vectors from document_chunks (Distance.COSINE, payload filter document_type).",
        "5. Zero-Leakage Rule: Feature windows require t_prior < t_current; supervised training & FE-08 require label_available_at <= split_boundary (60-day maturity)."
    ]
    py = 21.8
    for line in bullets_poly:
        ax.text(58.0, py, line, fontsize=7.5, color="#E2E8F0", va="center", zorder=5)
        py -= 3.8

    def connect(p1: tuple[float, float], p2: tuple[float, float], col: str, label: str = "", rad: float = 0.0) -> None:
        ax.annotate(
            "", xy=p2, xytext=p1,
            arrowprops=dict(
                arrowstyle="-|>,head_length=0.6,head_width=0.4",
                color=col, lw=1.8,
                connectionstyle=f"arc3,rad={rad}"
            ),
            zorder=2
        )
        if label:
            mx, my = (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2
            ax.text(
                mx, my + 0.9, label, fontsize=7.5, fontweight="bold", color=col,
                ha="center", va="center",
                bbox=dict(boxstyle="round,pad=0.15", facecolor="#0B1120", edgecolor=col, lw=0.8),
                zorder=6
            )

    connect((23.0, 82.0), (27.5, 78.5), "#60A5FA", "1:N customer_id")
    connect((23.0, 68.5), (27.5, 73.5), "#60A5FA", "1:N merchant_id")
    connect((23.0, 55.0), (27.5, 69.0), "#60A5FA", "0..1:N device_id")

    connect((12.5, 76.5), (12.5, 45.5), "#34D399", "1:N customer_id", rad=0.15)
    connect((12.5, 34.0), (12.5, 30.5), "#34D399", "0..1:N legacy_case_id")
    connect((12.5, 18.5), (12.5, 15.5), "#34D399", "1:N document_id")
    connect((27.5, 67.5), (23.0, 25.5), "#34D399", "0..1:N transaction_id", rad=-0.12)

    connect((39.5, 66.0), (39.5, 62.5), "#38BDF8", "1:1 tx_id")
    connect((39.5, 51.0), (39.5, 48.0), "#38BDF8", "1:1 tx_id")
    connect((39.5, 35.5), (39.5, 32.5), "#38BDF8", "1:1 tx_id")
    connect((39.5, 18.5), (39.5, 15.5), "#FBBF24", "1:N tx_id")

    connect((52.0, 78.5), (56.5, 81.5), "#C084FC", "1:N tx_id")
    connect((52.0, 74.0), (56.5, 66.5), "#C084FC", "1:N tx_id")
    connect((52.0, 70.0), (56.5, 51.0), "#C084FC", "1:1 tx_id")

    connect((78.5, 81.0), (77.0, 54.0), "#FBBF24", "mcc_group", rad=0.12)
    connect((78.5, 66.0), (77.0, 51.5), "#FBBF24", "rule_id", rad=0.06)
    connect((78.5, 51.0), (77.0, 49.0), "#FBBF24", "entity_id")
    connect((88.2, 42.5), (88.2, 45.5), "#FBBF24", "audits")

    er_out_path = OUT_DIR / "sentinel_er_diagram_18_tables.png"
    plt.tight_layout(pad=0.5)
    fig.savefig(er_out_path, dpi=160, facecolor=fig.get_facecolor())
    plt.close(fig)
    return er_out_path


if __name__ == "__main__":
    out = generate_er_diagram()
    print(f"Generated 18-table ER architecture diagram -> {out}")
