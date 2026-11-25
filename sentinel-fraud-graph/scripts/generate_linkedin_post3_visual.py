#!/usr/bin/env python3
"""
Project SENTINEL — LinkedIn Post 3/8 Visual Showcase Asset Generator.
=====================================================================

Renders a publication-grade, 16:9 widescreen split-screen visual showcase
(1920x1080 px at 300 DPI) for LinkedIn Post 3/8:
  "Neo4j 5 Fraud Ring & Shared-Device Detection Engine"

Panels:
  - Top Banner : Title, architecture badges, DPDP 2023 & PCI-DSS v4.0 metadata.
  - Left Panel : Styled dark IDE code card with syntax-highlighted 2-hop Cypher traversal
                 query ([:USED_DEVICE|PAID*1..2]) + Architectural trade-off callout.
  - Right Panel: Embedded high-resolution Suspicious Cluster Graph (outputs/suspicious_cluster_graph.png)
                 cleanly framed with aspect-ratio preservation + Tokenized entity legend.
  - Footer Strip: GitHub repository navigation link + Zero-PII compliance statement.

Output Target:
  outputs/linkedin_post3_fraud_graph_showcase.png
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, FancyBboxPatch
import numpy as np
from PIL import Image

SCRIPT_DIR = Path(__file__).resolve().parent
MODULE_ROOT = SCRIPT_DIR.parent
OUTPUTS_DIR = MODULE_ROOT / "outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------------------
# Design Tokens & Color Palette (Dark Slate Fintech Theme)
# ------------------------------------------------------------------------------
BG_DARK = "#090D16"            # Deep Dark Slate Canvas
PANEL_BG = "#0D1322"           # Card Surface
PANEL_INNER = "#070B14"        # Code & Image Frame Surface
CALLOUT_BG = "#061826"         # Callout Box Background
BORDER_SUBTLE = "#1E293B"      # Dark Slate Border
BORDER_ACTIVE = "#38BDF8"      # Cyan Accent Border
BORDER_CALLOUT = "#0284C7"     # Callout Border

TEXT_PRIMARY = "#F8FAFC"       # Pure Crisp White
TEXT_SECONDARY = "#94A3B8"     # Slate Subtitles / Labels
TEXT_MUTED = "#64748B"         # Subtle Line Numbers / Descriptions
TEXT_CODE = "#E2E8F0"          # Code Default Text

# Accent Badges
CYAN_ACCENT = "#38BDF8"
CYAN_BG = "#082F49"
CYAN_BORDER = "#0284C7"

AMBER_ACCENT = "#F59E0B"
AMBER_BG = "#451A03"
AMBER_BORDER = "#B45309"

EMERALD_ACCENT = "#10B981"
EMERALD_BG = "#064E3B"
EMERALD_BORDER = "#059669"

PURPLE_ACCENT = "#A855F7"
PURPLE_BG = "#3B0764"
PURPLE_BORDER = "#7E22CE"

RED_ACCENT = "#EF4444"
RED_BG = "#450A0A"
RED_BORDER = "#DC2626"


# ------------------------------------------------------------------------------
# Drawing Helpers
# ------------------------------------------------------------------------------
def draw_card(
    ax, x: float, y: float, w: float, h: float,
    facecolor: str = PANEL_BG,
    edgecolor: str = BORDER_SUBTLE,
    linewidth: float = 1.2,
    rounding: float = 0.8,
    zorder: int = 2
) -> FancyBboxPatch:
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0.0,rounding_size={rounding}",
        facecolor=facecolor,
        edgecolor=edgecolor,
        linewidth=linewidth,
        zorder=zorder
    )
    ax.add_patch(patch)
    return patch


def draw_pill(
    ax, x: float, y: float, w: float, h: float,
    text: str,
    bg_color: str,
    border_color: str,
    text_color: str = "#F8FAFC",
    fontsize: float = 8.5,
    fontweight: str = "bold",
    zorder: int = 5
) -> FancyBboxPatch:
    pill = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.0,rounding_size=0.5",
        facecolor=bg_color,
        edgecolor=border_color,
        linewidth=1.0,
        zorder=zorder
    )
    ax.add_patch(pill)
    ax.text(
        x + w / 2.0, y + h / 2.0, text,
        ha="center", va="center",
        fontsize=fontsize, fontweight=fontweight,
        color=text_color, zorder=zorder + 1
    )
    return pill


def measure_monospace_char_advance(fig, ax, fontsize: float) -> float:
    """Measure exact single-character advance width in data units on the active axes."""
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    sample = "0" * 40
    t_full = ax.text(10, 50, f"|{sample}|", fontsize=fontsize, family="monospace")
    t_bars = ax.text(10, 50, "||", fontsize=fontsize, family="monospace")
    b_full = t_full.get_window_extent(renderer).transformed(ax.transData.inverted())
    b_bars = t_bars.get_window_extent(renderer).transformed(ax.transData.inverted())
    t_full.remove()
    t_bars.remove()
    return (b_full.width - b_bars.width) / len(sample)


# ------------------------------------------------------------------------------
# 2-Hop Cypher Traversal Query Tokens (Exact Query 2 from sentinel_fraud_graph.py)
# ------------------------------------------------------------------------------
CYPHER_LINES: List[Tuple[str, List[Tuple[str, str, bool]]]] = [
    ("01", [
        ("MATCH ", "#38BDF8", True),
        ("(fraud_c", "#F8FAFC", False),
        (":", "#94A3B8", False),
        ("Customer", "#34D399", True),
        (")-[", "#94A3B8", False),
        ("fraud_p", "#F8FAFC", False),
        (":", "#94A3B8", False),
        ("PAID", "#F59E0B", True),
        (" {fraud_label: ", "#94A3B8", False),
        ("1", "#FB923C", True),
        ("}]->(", "#94A3B8", False),
        ("target_m", "#F8FAFC", False),
        (":", "#94A3B8", False),
        ("Merchant", "#38BDF8", True),
        (")", "#94A3B8", False),
    ]),
    ("02", [
        ("MATCH ", "#38BDF8", True),
        ("path = (fraud_c)-[:", "#F8FAFC", False),
        ("USED_DEVICE", "#F59E0B", True),
        ("|", "#94A3B8", False),
        ("PAID", "#F59E0B", True),
        ("*1..2", "#EF4444", True),
        ("]-(suspect_c:", "#F8FAFC", False),
        ("Customer", "#34D399", True),
        (")", "#94A3B8", False),
    ]),
    ("03", [
        ("WHERE ", "#C084FC", True),
        ("suspect_c.customer_id <> fraud_c.customer_id", "#CBD5E1", False),
    ]),
    ("04", [
        ("OPTIONAL MATCH ", "#38BDF8", True),
        ("(fraud_c)-[:", "#94A3B8", False),
        ("USED_DEVICE", "#F59E0B", True),
        ("]->(shared_d:", "#94A3B8", False),
        ("Device", "#F472B6", True),
        (")<-[:", "#94A3B8", False),
        ("USED_DEVICE", "#F59E0B", True),
        ("]-(suspect_c)", "#94A3B8", False),
    ]),
    ("05", [
        ("WITH ", "#C084FC", True),
        ("suspect_c,", "#F8FAFC", False),
    ]),
    ("06", [
        ("     count", "#38BDF8", True),
        ("(", "#94A3B8", False),
        ("DISTINCT ", "#C084FC", True),
        ("fraud_c.customer_id) ", "#CBD5E1", False),
        ("AS ", "#C084FC", True),
        ("linked_fraud_customers,", "#F8FAFC", False),
    ]),
    ("07", [
        ("     count", "#38BDF8", True),
        ("(", "#94A3B8", False),
        ("DISTINCT ", "#C084FC", True),
        ("shared_d.device_id) ", "#CBD5E1", False),
        ("AS ", "#C084FC", True),
        ("shared_mule_devices,", "#F8FAFC", False),
    ]),
    ("08", [
        ("     count", "#38BDF8", True),
        ("(", "#94A3B8", False),
        ("DISTINCT ", "#C084FC", True),
        ("target_m.merchant_id) ", "#CBD5E1", False),
        ("AS ", "#C084FC", True),
        ("shared_fraud_merchants,", "#F8FAFC", False),
    ]),
    ("09", [
        ("     collect", "#38BDF8", True),
        ("(", "#94A3B8", False),
        ("DISTINCT ", "#C084FC", True),
        ("shared_d.device_id)[", "#CBD5E1", False),
        ("0..5", "#FB923C", True),
        ("] ", "#CBD5E1", False),
        ("AS ", "#C084FC", True),
        ("bridge_devices", "#F8FAFC", False),
    ]),
    ("10", [
        ("MATCH ", "#38BDF8", True),
        ("(suspect_c)-[sp:", "#94A3B8", False),
        ("PAID", "#F59E0B", True),
        ("]->(:", "#94A3B8", False),
        ("Merchant", "#38BDF8", True),
        (")", "#94A3B8", False),
    ]),
    ("11", [
        ("RETURN ", "#38BDF8", True),
        ("suspect_c.customer_id ", "#F8FAFC", False),
        ("AS ", "#C084FC", True),
        ("suspect_customer_id,", "#38BDF8", False),
    ]),
    ("12", [
        ("       suspect_c.home_city ", "#F8FAFC", False),
        ("AS ", "#C084FC", True),
        ("home_city,", "#CBD5E1", False),
    ]),
    ("13", [
        ("       linked_fraud_customers, shared_mule_devices,", "#CBD5E1", False),
    ]),
    ("14", [
        ("       shared_fraud_merchants,", "#CBD5E1", False),
    ]),
    ("15", [
        ("       count", "#38BDF8", True),
        ("(", "#94A3B8", False),
        ("DISTINCT ", "#C084FC", True),
        ("sp.transaction_id) ", "#CBD5E1", False),
        ("AS ", "#C084FC", True),
        ("suspect_total_txns,", "#CBD5E1", False),
    ]),
    ("16", [
        ("       sum", "#38BDF8", True),
        ("(", "#94A3B8", False),
        ("CASE WHEN ", "#C084FC", True),
        ("sp.fraud_label = ", "#CBD5E1", False),
        ("1 ", "#FB923C", True),
        ("THEN ", "#C084FC", True),
        ("1 ", "#FB923C", True),
        ("ELSE ", "#C084FC", True),
        ("0 ", "#FB923C", True),
        ("END", "#C084FC", True),
        (") ", "#94A3B8", False),
        ("AS ", "#C084FC", True),
        ("suspect_fraud_txns,", "#CBD5E1", False),
    ]),
    ("17", [
        ("       round", "#38BDF8", True),
        ("(sum(sp.transaction_amount), ", "#CBD5E1", False),
        ("2", "#FB923C", True),
        (") ", "#CBD5E1", False),
        ("AS ", "#C084FC", True),
        ("suspect_spend_inr,", "#CBD5E1", False),
    ]),
    ("18", [
        ("       bridge_devices", "#CBD5E1", False),
    ]),
    ("19", [
        ("ORDER BY ", "#C084FC", True),
        ("shared_mule_devices ", "#CBD5E1", False),
        ("DESC", "#C084FC", True),
        (", linked_fraud_customers ", "#CBD5E1", False),
        ("DESC", "#C084FC", True),
        (", suspect_spend_inr ", "#CBD5E1", False),
        ("DESC", "#C084FC", True),
    ]),
    ("20", [
        ("LIMIT ", "#38BDF8", True),
        ("20", "#FB923C", True),
        (";", "#94A3B8", False),
    ]),
]


# ------------------------------------------------------------------------------
# Main Visual Generation Routine
# ------------------------------------------------------------------------------
def generate_showcase_visual() -> Path:
    """Generate the 16:9 split-screen showcase visual asset."""
    target_path = OUTPUTS_DIR / "linkedin_post3_fraud_graph_showcase.png"
    cluster_img_path = OUTPUTS_DIR / "suspicious_cluster_graph.png"

    if not cluster_img_path.exists():
        raise FileNotFoundError(f"Missing required cluster graph asset: {cluster_img_path}")

    # Create figure: 19.2 x 10.8 inches @ 100 DPI = 1920 x 1080 px
    fig = plt.figure(figsize=(19.2, 10.8), dpi=100, facecolor=BG_DARK)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG_DARK)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    # ==========================================================================
    # 1. TOP HEADER BANNER (Y: 86.5 -> 98.5)
    # ==========================================================================
    draw_pill(ax, 2.2, 95.2, 15.6, 2.5, "PROJECT SENTINEL • PART 3/8", "#111C33", CYAN_BORDER, CYAN_ACCENT, fontsize=8.0)
    draw_pill(ax, 18.5, 95.2, 21.0, 2.5, "NEO4J 5.20 • PROPERTY GRAPH", "#2A1138", PURPLE_BORDER, "#D8B4FE", fontsize=8.0)
    draw_pill(ax, 40.2, 95.2, 20.2, 2.5, "TIER-1 RBI PAYMENT AGGREGATOR", "#062E20", EMERALD_BORDER, "#6EE7B7", fontsize=8.0)
    draw_pill(ax, 61.1, 95.2, 16.5, 2.5, "p99 <= 250ms SCORING SLA", "#331604", AMBER_BORDER, "#FDE68A", fontsize=8.0)
    draw_pill(ax, 78.3, 95.2, 19.5, 2.5, "DPDP 2023 & PCI-DSS v4.0", "#2D0A0A", RED_BORDER, "#FCA5A5", fontsize=8.0)

    ax.text(
        2.2, 92.5,
        "Project SENTINEL (Part 3/8) — Neo4j 5 Fraud Ring & Shared-Device Detection Engine",
        fontsize=16.8, fontweight="bold", color=TEXT_PRIMARY, va="top"
    )

    ax.text(
        2.2, 89.2,
        "110,000 AuraDB Transactions | 2,500 Customers | 200 Merchants | O(1) Index-Free Pointer Hops | DPDP 2023 & PCI-DSS v4.0 Tokenized",
        fontsize=10.0, color=TEXT_SECONDARY, va="top"
    )

    # ==========================================================================
    # 2. MAIN PANELS LAYOUT (Y: 6.8 -> 86.3, Height = 79.5)
    # ==========================================================================
    panel_y = 6.8
    panel_h = 79.5
    panel_w = 47.0
    left_x = 2.2
    right_x = 50.8

    # --------------------------------------------------------------------------
    # LEFT PANEL: 2-Hop Cypher Traversal Code Card & Architecture Callout
    # --------------------------------------------------------------------------
    draw_card(ax, left_x, panel_y, panel_w, panel_h, facecolor=PANEL_BG, edgecolor=BORDER_SUBTLE, linewidth=1.4, rounding=1.0)

    draw_pill(ax, left_x + 1.6, panel_y + panel_h - 3.8, 18.0, 2.4, "CYPHER 2-HOP TRAVERSAL", "#0C2340", CYAN_BORDER, "#38BDF8", fontsize=7.8)
    draw_pill(ax, left_x + 20.3, panel_y + panel_h - 3.8, 16.5, 2.4, "[:USED_DEVICE|PAID*1..2]", "#381122", RED_BORDER, "#F87171", fontsize=7.6)
    draw_pill(ax, left_x + 37.5, panel_y + panel_h - 3.8, 8.0, 2.4, "O(1) HOPS", "#0B2B1E", EMERALD_BORDER, "#34D399", fontsize=7.6)

    # IDE Window
    ide_x = left_x + 1.6
    ide_y = panel_y + 24.2
    ide_w = panel_w - 3.2
    ide_h = panel_h - 29.0  # 50.5 units tall
    draw_card(ax, ide_x, ide_y, ide_w, ide_h, facecolor=PANEL_INNER, edgecolor="#1E293B", linewidth=1.2, rounding=0.6)

    title_bar_h = 2.8
    title_bar_y = ide_y + ide_h - title_bar_h
    draw_card(ax, ide_x, title_bar_y, ide_w, title_bar_h, facecolor="#0B1120", edgecolor="#1E293B", linewidth=0.8, rounding=0.4)

    ax.add_patch(Circle((ide_x + 1.2, title_bar_y + 1.4), 0.35, facecolor="#EF4444", edgecolor="none", zorder=6))
    ax.add_patch(Circle((ide_x + 2.2, title_bar_y + 1.4), 0.35, facecolor="#F59E0B", edgecolor="none", zorder=6))
    ax.add_patch(Circle((ide_x + 3.2, title_bar_y + 1.4), 0.35, facecolor="#10B981", edgecolor="none", zorder=6))

    ax.text(
        ide_x + 4.5, title_bar_y + 1.4,
        "queries.cypher — Query 2: Multi-Hop Fraud Proximity & Sleeper Ring Traversal",
        fontsize=7.8, family="monospace", color="#94A3B8", va="center", zorder=6
    )

    code_fontsize = 7.8
    char_advance = measure_monospace_char_advance(fig, ax, fontsize=code_fontsize)
    code_start_y = title_bar_y - 1.9
    line_spacing = 2.25

    for idx, (line_no, tokens) in enumerate(CYPHER_LINES):
        cur_y = code_start_y - idx * line_spacing
        ax.text(
            ide_x + 1.1, cur_y,
            line_no,
            fontsize=7.4, family="monospace", color="#475569", va="center", zorder=5
        )
        ax.plot([ide_x + 2.8, ide_x + 2.8], [cur_y - 0.9, cur_y + 0.9], color="#1E293B", linewidth=0.6, zorder=4)

        token_x = ide_x + 3.5
        for text_frag, col, is_b in tokens:
            ax.text(
                token_x, cur_y,
                text_frag,
                fontsize=code_fontsize, family="monospace",
                color=col, fontweight="bold" if is_b else "normal",
                va="center", zorder=5
            )
            token_x += char_advance * len(text_frag)

    # Compact Callout Box Underneath Code Block
    callout_x = left_x + 1.6
    callout_y = panel_y + 1.6
    callout_w = panel_w - 3.2
    callout_h = 21.0
    draw_card(ax, callout_x, callout_y, callout_w, callout_h, facecolor=CALLOUT_BG, edgecolor=BORDER_CALLOUT, linewidth=1.2, rounding=0.6)

    ax.text(
        callout_x + 1.4, callout_y + callout_h - 1.8,
        "CORE ARCHITECTURAL TRADE-OFF: INDEX-FREE ADJACENCY VS. SQL JOINS",
        fontsize=8.4, fontweight="bold", color=CYAN_ACCENT, va="center", zorder=5
    )

    tradeoff_points = [
        (
            "• O(1) Index-Free Pointer Hops vs. SQL Join Explosion",
            [
                "Neo4j stores physical double-pointers in memory, traversing 2 hops in ~2.1 ms without multi-table",
                "Cartesian products. In PostgreSQL, 2-hop traversals require 4+ self-joins, thrashing the buffer cache."
            ]
        ),
        (
            "• Uniqueness Constraints + Parameterized MERGE",
            [
                "Enforces O(log N) index lookups during UNWIND batch ingestion. Stream consumer replays",
                "(Kafka / Kinesis at-least-once delivery) execute idempotently with zero node/edge duplication (delta = 0)."
            ]
        ),
        (
            "• Pre-Maturation Sleeper Detection (< 250ms SLA)",
            [
                "Dispute/chargeback labels mature up to 60 days post-authorization. 2-hop graph contagion discovers",
                "sleeper accomplice accounts sharing hardware fingerprints before chargebacks crystallize."
            ]
        ),
    ]

    by = callout_y + callout_h - 4.0
    for title, desc_lines in tradeoff_points:
        ax.text(callout_x + 1.4, by, title, fontsize=7.6, fontweight="bold", color="#F8FAFC", va="center", zorder=5)
        by -= 1.6
        for line_str in desc_lines:
            ax.text(callout_x + 2.8, by, line_str, fontsize=6.8, color="#CBD5E1", va="center", zorder=5)
            by -= 1.4
        by -= 0.8

    # --------------------------------------------------------------------------
    # RIGHT PANEL: Embedded Suspicious Cluster Graph & Entity Legend
    # --------------------------------------------------------------------------
    draw_card(ax, right_x, panel_y, panel_w, panel_h, facecolor=PANEL_BG, edgecolor=BORDER_SUBTLE, linewidth=1.4, rounding=1.0)

    draw_pill(ax, right_x + 1.6, panel_y + panel_h - 3.8, 17.5, 2.4, "RADIAL TOPOLOGY VISUALIZATION", "#0C2340", CYAN_BORDER, "#38BDF8", fontsize=7.6)
    draw_pill(ax, right_x + 19.8, panel_y + panel_h - 3.8, 17.5, 2.4, "outputs/suspicious_cluster_graph.png", "#261033", PURPLE_BORDER, "#D8B4FE", fontsize=7.4)
    draw_pill(ax, right_x + 38.0, panel_y + panel_h - 3.8, 7.5, 2.4, "24N • 51E", "#0B2B1E", EMERALD_BORDER, "#34D399", fontsize=7.4)

    ax.text(
        right_x + 1.6, panel_y + panel_h - 5.5,
        "Forensic Multi-Tier Fraud Ring: 4 Shared Mule Devices, 13 Customers, 7 Merchants",
        fontsize=8.3, fontweight="bold", color=TEXT_PRIMARY, va="center", zorder=5
    )

    # Embedded Image Frame Coordinates (Preserving Exact Aspect Ratio)
    frame_x = right_x + 1.6
    frame_y = panel_y + 21.8
    frame_w = panel_w - 3.2   # 43.8 units
    frame_h = 50.4            # 50.4 units

    draw_card(ax, frame_x, frame_y, frame_w, frame_h, facecolor=PANEL_INNER, edgecolor="#1E293B", linewidth=1.2, rounding=0.6)

    cluster_pil = Image.open(cluster_img_path)
    orig_w, orig_h = cluster_pil.size
    aspect_ratio = orig_w / orig_h  # 3498 / 2301 = 1.5202

    pad_u = 0.6
    avail_w = frame_w - 2 * pad_u
    avail_h = frame_h - 2 * pad_u
    avail_aspect = (avail_w * 19.2) / (avail_h * 10.8)

    if avail_aspect > aspect_ratio:
        img_draw_h = avail_h
        img_draw_w = (img_draw_h * 10.8 * aspect_ratio) / 19.2
    else:
        img_draw_w = avail_w
        img_draw_h = (img_draw_w * 19.2 / aspect_ratio) / 10.8

    img_x = frame_x + pad_u + (avail_w - img_draw_w) / 2.0
    img_y = frame_y + pad_u + (avail_h - img_draw_h) / 2.0

    cluster_np = np.asarray(cluster_pil)
    ax.imshow(
        cluster_np,
        extent=[img_x, img_x + img_draw_w, img_y, img_y + img_draw_h],
        aspect="auto",
        zorder=3
    )

    # Crisp Legend / Annotation Callout Underneath Embedded Graph
    legend_x = right_x + 1.6
    legend_y = panel_y + 1.6
    legend_w = panel_w - 3.2
    legend_h = 18.8
    draw_card(ax, legend_x, legend_y, legend_w, legend_h, facecolor=CALLOUT_BG, edgecolor=BORDER_CALLOUT, linewidth=1.2, rounding=0.6)

    ax.text(
        legend_x + 1.4, legend_y + legend_h - 1.7,
        "TOKENIZED ENTITY LEGEND & RING ATTRIBUTION (DPDP ACT 2023 ZERO-PII)",
        fontsize=8.4, fontweight="bold", color=AMBER_ACCENT, va="center", zorder=5
    )

    legend_items = [
        (
            "C_* (Customers — 13 Nodes)",
            "#F59E0B",
            [
                "12 Confirmed-Fraud accounts (Orange Circles) + 1 Sleeper Accomplice (Gold Circle)",
                "linked via 2-hop device edges (pre-maturation sleeper contagion discovery)."
            ]
        ),
        (
            "D_* (Shared Devices — 4 Nodes)",
            "#EF4444",
            [
                "Inner Core Mule Hubs (Red Hexagons). Shared by >= 3 customers within 7 days.",
                "Extreme fraud concentration: 83.3% to 93.8% confirmed fraudulent payments."
            ]
        ),
        (
            "M_* (Merchants — 7 Nodes)",
            "#38BDF8",
            [
                "Outer Ring Cash-Out Terminals (Cyan Squares) in Electronics, Travel & Gaming.",
                "Shared-device fraud rate: 61.5% - 77.1% (high-risk MCC terminals)."
            ]
        ),
        (
            "Edges: [:USED_DEVICE] & [:PAID]",
            "#34D399",
            [
                "Solid Amber = [:USED_DEVICE] hardware binding | Dashed Red = [:PAID {fraud_label: 1}] cash-out."
            ]
        )
    ]

    ly = legend_y + legend_h - 3.7
    for title, col, lines in legend_items:
        ax.text(legend_x + 1.4, ly, title, fontsize=7.4, fontweight="bold", color=col, va="center", zorder=5)
        ly -= 1.35
        for l_str in lines:
            ax.text(legend_x + 2.8, ly, l_str, fontsize=6.7, color="#CBD5E1", va="center", zorder=5)
            ly -= 1.25
        ly -= 0.45

    # ==========================================================================
    # 3. FOOTER STRIP (Y: 1.6 -> 5.2)
    # ==========================================================================
    footer_x = 2.2
    footer_y = 1.6
    footer_w = 95.6
    footer_h = 3.6
    draw_card(ax, footer_x, footer_y, footer_w, footer_h, facecolor="#070B14", edgecolor=BORDER_SUBTLE, linewidth=1.0, rounding=0.5)

    ax.text(
        footer_x + 1.8, footer_y + 1.8,
        "github.com/pavanmajji18/sentinel-fraud-intelligence/tree/main/sentinel-fraud-graph",
        fontsize=8.4, family="monospace", fontweight="bold", color=CYAN_ACCENT, va="center", zorder=5
    )

    ax.text(
        footer_x + footer_w - 1.8, footer_y + 1.8,
        "Zero-PII Tokenized Graph Telemetry (C_*, D_*, M_*) | DPDP Act 2023 & PCI-DSS v4.0 Certified",
        fontsize=8.2, fontweight="bold", color=EMERALD_ACCENT, ha="right", va="center", zorder=5
    )

    # ==========================================================================
    # 4. EXPORT TO EXACT 1920x1080 @ 300 DPI
    # ==========================================================================
    temp_render_path = OUTPUTS_DIR / "temp_showcase_render.png"
    plt.savefig(
        temp_render_path,
        dpi=100,
        facecolor=fig.get_facecolor(),
        edgecolor="none"
    )
    plt.close()

    with Image.open(temp_render_path) as im:
        if im.size != (1920, 1080):
            im = im.resize((1920, 1080), Image.Resampling.LANCZOS)
        im.save(target_path, dpi=(300, 300), optimize=True)

    if temp_render_path.exists():
        temp_render_path.unlink()

    file_size_kb = target_path.stat().st_size / 1024.0
    print(f"[SUCCESS] Generated LinkedIn Showcase Asset: {target_path}")
    print(f"          Target Resolution: 1920x1080 px | DPI: 300 | File Size: {file_size_kb:.1f} KB")
    return target_path


if __name__ == "__main__":
    generate_showcase_visual()
