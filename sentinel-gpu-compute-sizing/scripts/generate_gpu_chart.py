#!/usr/bin/env python3
"""
Generates publication-quality GPU utilization, cluster sizing & cost economics
diagnostic telemetry chart: outputs/gpu_utilization_chart.png
"""

import json
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = SCRIPT_DIR / "outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


def generate_chart():
    # Set dark high-contrast publication style
    plt.style.use("dark_background")
    fig, axes = plt.subplots(2, 2, figsize=(16, 11), dpi=200)
    fig.patch.set_facecolor("#0b0f19")
    for ax in axes.flat:
        ax.set_facecolor("#111827")
        ax.grid(True, linestyle="--", alpha=0.25, color="#9ca3af")
        ax.tick_params(colors="#e5e7eb", labelsize=10)
        for spine in ax.spines.values():
            spine.set_color("#374151")

    # Panel 1: Peak Compute vs Memory Bandwidth
    gpus = ["NVIDIA L4\n(24GB)", "RTX A6000\n(48GB)", "NVIDIA L40\n(48GB)", "A100-80GB\n(80GB)", "RTX 3050\n(Laptop 4GB)"]
    tflops = [121.0, 154.8, 181.0, 312.0, 9.0]
    bandwidth = [300.0, 768.0, 864.0, 2039.0, 192.0]

    x = np.arange(len(gpus))
    width = 0.35

    ax1 = axes[0, 0]
    rects1 = ax1.bar(x - width/2, tflops, width, label="FP16/BF16 Peak TFLOPS", color="#3b82f6", edgecolor="#60a5fa", alpha=0.9)
    ax1_twin = ax1.twinx()
    ax1_twin.set_facecolor("#111827")
    rects2 = ax1_twin.bar(x + width/2, bandwidth, width, label="Memory Bandwidth (GB/s)", color="#10b981", edgecolor="#34d399", alpha=0.9)
    ax1_twin.tick_params(colors="#e5e7eb", labelsize=10)
    ax1_twin.grid(False)

    ax1.set_ylabel("Peak Compute (TFLOPS)", color="#60a5fa", fontsize=11, fontweight="bold")
    ax1_twin.set_ylabel("Memory Bandwidth (GB/s)", color="#34d399", fontsize=11, fontweight="bold")
    ax1.set_title("NVIDIA GPU Sizing Spectrum: Compute vs. Bandwidth", fontsize=12, fontweight="bold", color="#f3f4f6", pad=12)
    ax1.set_xticks(x)
    ax1.set_xticklabels(gpus, fontsize=9.5)

    # Panel 2: VRAM Allocation Breakdown (Qwen-2.5 7B BF16 vs QLoRA NF4)
    ax2 = axes[0, 1]
    configs = ["Full BF16\n(AdamW 32-bit)", "Full BF16\n(AdamW 8-bit)", "LoRA BF16\n(Rank=16, Frozen)", "QLoRA NF4\n(Rank=16, 4-bit)"]
    weights = [14.0, 14.0, 14.0, 4.0]
    grads = [14.0, 14.0, 0.11, 0.11]
    optimizer = [28.0, 14.0, 0.45, 0.45]
    activations = [8.2, 8.2, 5.8, 5.8]

    p1 = ax2.bar(configs, weights, label="Model Weights", color="#6366f1", width=0.45)
    p2 = ax2.bar(configs, grads, bottom=weights, label="Gradients", color="#ec4899", width=0.45)
    p3 = ax2.bar(configs, optimizer, bottom=np.array(weights) + np.array(grads), label="Optimizer States", color="#f59e0b", width=0.45)
    p4 = ax2.bar(configs, activations, bottom=np.array(weights) + np.array(grads) + np.array(optimizer), label="Activations & KV", color="#06b6d4", width=0.45)

    # Threshold lines for 24GB L4 and 48GB A6000
    ax2.axhline(24, color="#e11d48", linestyle="--", linewidth=1.5, label="24 GB Limit (NVIDIA L4)")
    ax2.axhline(48, color="#10b981", linestyle="--", linewidth=1.5, label="48 GB Limit (RTX A6000 / L40)")
    ax2.set_ylabel("VRAM Footprint (GB)", color="#e5e7eb", fontsize=11, fontweight="bold")
    ax2.set_title("Qwen-2.5 7B VRAM Budget: 48GB OOM vs. 24GB L4 QLoRA Fit", fontsize=12, fontweight="bold", color="#f3f4f6", pad=12)
    ax2.legend(loc="upper right", fontsize=8.5, framealpha=0.4)

    # Panel 3: 2,800 TPS Peak Cluster Provisioning & Sizing
    ax3 = axes[1, 0]
    components = ["Inline Scoring\n(8x L4 @ 350 TPS)", "Async RAG\n(2x L4 @ 1400 TPS)", "Total Cluster\n(10x L4 Fleet)"]
    tps_capacity = [2800, 2800, 2800]
    utilization_pct = [81.5, 68.2, 78.8]

    bars = ax3.bar(components, utilization_pct, color=["#3b82f6", "#8b5cf6", "#10b981"], width=0.42, edgecolor="#9ca3af")
    for bar in bars:
        h = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., h + 1.5, f"{h:.1f}% util\n(2,800 TPS)", ha="center", va="bottom", color="#f3f4f6", fontsize=9.5, fontweight="bold")
    ax3.set_ylim(0, 105)
    ax3.axhline(85, color="#f59e0b", linestyle=":", linewidth=1.5, label="Target Load Ceiling (85%)")
    ax3.set_ylabel("Peak Cluster Utilization (%)", color="#e5e7eb", fontsize=11, fontweight="bold")
    ax3.set_title("2,800 TPS Festive Peak Provisioning (10x NVIDIA L4 Instances)", fontsize=12, fontweight="bold", color="#f3f4f6", pad=12)
    ax3.legend(loc="upper left", fontsize=9, framealpha=0.4)

    # Panel 4: Unit Economics (Inference Cost per 1,000 Transactions in INR)
    ax4 = axes[1, 1]
    solutions = ["Pre-Sentinel\nBaseline", "A100-80GB\nFleet (8x A100)", "RTX A6000\nFleet (8x A6000)", "SENTINEL Production\n(10x NVIDIA L4)"]
    costs_inr = [8.50, 6.28, 3.42, 2.14]
    colors = ["#ef4444", "#f97316", "#3b82f6", "#10b981"]

    b4 = ax4.bar(solutions, costs_inr, color=colors, width=0.45, edgecolor="#d1d5db")
    for bar in b4:
        h = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., h + 0.15, f"Rs. {h:.2f}", ha="center", va="bottom", color="#f3f4f6", fontsize=10, fontweight="bold")
    ax4.axhline(4.00, color="#ef4444", linestyle="--", linewidth=1.8, label="OBJ-12 SLA Ceiling (Rs. 4.00 / 1k txns)")
    ax4.set_ylim(0, 10.5)
    ax4.set_ylabel("Cost per 1,000 Transactions (INR)", color="#e5e7eb", fontsize=11, fontweight="bold")
    ax4.set_title("Inference Economics at 2,800 TPS Peak (OBJ-12 Compliance)", fontsize=12, fontweight="bold", color="#f3f4f6", pad=12)
    ax4.legend(loc="upper right", fontsize=9, framealpha=0.4)

    fig.suptitle("Project SENTINEL — GPU Compute Sizing, VRAM Footprint & Inference Economics", fontsize=16, fontweight="bold", color="#f9fafb", y=0.98)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    chart_path = OUTPUTS_DIR / "gpu_utilization_chart.png"
    plt.savefig(chart_path, dpi=200, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"[OK] Generated: {chart_path}")


if __name__ == "__main__":
    generate_chart()
