#!/usr/bin/env python3
"""
Project SENTINEL — GPU Benchmark & PyTorch GEMM / Mini-LoRA Telemetry Entrypoint
================================================================================
Canonical entrypoint for GPU compute profiling, Tensor Core GEMM FLOPs verification,
mini-LoRA backward step validation, and embedded telemetry report generation.

Exports:
  - outputs/gpu_benchmark_report.json
  - outputs/gpu_telemetry_report.json

Usage:
  python gpu_benchmark.py --verify
  python gpu_benchmark.py --matrix-dim 2048 --iterations 8
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

# Ensure local directory is importable
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import gpu_demo_and_server


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Project SENTINEL — GPU Benchmark & PyTorch Telemetry Entrypoint"
    )
    parser.add_argument("--matrix-dim", type=int, default=2048, help="Matrix dimension M=N=K for GEMM benchmark (default: 2048)")
    parser.add_argument("--iterations", type=int, default=8, help="Number of benchmark iterations (default: 8)")
    parser.add_argument("--port", type=int, default=8080, help="HTTP telemetry server port (default: 8080)")
    parser.add_argument("--serve-seconds", type=int, default=0, help="Seconds to keep HTTP server alive (default: 0)")
    parser.add_argument("--verify", action="store_true", default=True, help="Assert all SLA and governance gates")
    parser.add_argument("--output-json", type=str, default=str(SCRIPT_DIR / "outputs" / "gpu_benchmark_report.json"), help="Output JSON path")
    args = parser.parse_args()

    # Delegate to canonical gpu_demo_and_server execution
    gpu_demo_and_server.main_with_args(
        matrix_dim=args.matrix_dim,
        iterations=args.iterations,
        port=args.port,
        serve_seconds=args.serve_seconds,
        verify=args.verify,
        output_json=args.output_json,
    )

    # Ensure outputs/gpu_telemetry_report.json is synchronized
    bench_path = Path(args.output_json)
    telemetry_path = SCRIPT_DIR / "outputs" / "gpu_telemetry_report.json"
    if bench_path.exists() and bench_path != telemetry_path:
        shutil.copy2(bench_path, telemetry_path)
        print(f"  -> Synchronized benchmark report to {telemetry_path}")


if __name__ == "__main__":
    main()
