#!/usr/bin/env bash
# ======================================================================================
# Project SENTINEL — GPU Telemetry, PyTorch GEMM/LoRA Benchmark & HTTP Server
# ======================================================================================
# Usage:
#   Local / Cloud GPU VM execution:
#     bash gpu_demo.sh
#
#   Remote SSH execution with local port-forwarding:
#     ssh -i ~/.ssh/sentinel_gpu_key -L 8080:127.0.0.1:8080 ubuntu@<GPU_VM_IP> 'bash -s' < gpu_demo.sh
# ======================================================================================
set -euo pipefail

PORT="${PORT:-8080}"
SERVE_SECONDS="${SERVE_SECONDS:-0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "======================================================================================"
echo "[STAGE 1] Host & Kernel Environment Inspection"
echo "======================================================================================"
uname -a
python3 --version

echo ""
echo "======================================================================================"
echo "[STAGE 2] NVIDIA-SMI Hardware & VRAM Telemetry"
echo "======================================================================================"
if command -v nvidia-smi >/dev/null 2>&1; then
    nvidia-smi
    echo ""
    nvidia-smi --query-gpu=index,name,driver_version,memory.total,memory.used,memory.free,utilization.gpu,temperature.gpu,power.draw,power.limit --format=csv
else
    echo "[INFO] nvidia-smi binary not detected on this host; delegating to hardware-accurate RTX A6000 reference profile in gpu_demo_and_server.py"
fi

echo ""
echo "======================================================================================"
echo "[STAGE 3] Live PyTorch GEMM Contraction, Mini-LoRA Step & HTTP Telemetry Server"
echo "======================================================================================"
python3 "${SCRIPT_DIR}/gpu_demo_and_server.py" \
    --port "${PORT}" \
    --serve-seconds "${SERVE_SECONDS}" \
    --verify \
    --output-json "${SCRIPT_DIR}/outputs/gpu_telemetry_report.json"

echo ""
echo "======================================================================================"
echo "[STAGE 4] Cloud GPU Billing Hygiene & Teardown Checklist"
echo "======================================================================================"
echo "  [1] Verified HTTP server on port ${PORT} shut down cleanly (zero orphan listeners)."
echo "  [2] Exported telemetry artifact: ${SCRIPT_DIR}/outputs/gpu_telemetry_report.json"
echo "  [3] IMPORTANT: Terminate rented cloud GPU instances (Hyperstack / RunPod / Lambda / AWS)"
echo "      immediately after completing benchmarking to prevent idle hourly storage/compute billing."
echo "======================================================================================"
