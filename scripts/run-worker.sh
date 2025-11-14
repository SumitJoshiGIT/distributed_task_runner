#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: run-worker.sh <worker_id> [api_base]

Runs the Node-based worker runner with the provided worker ID.
- <worker_id>: Required unique identifier for the worker.
- [api_base]: Optional backend URL (default: http://localhost:4000).

Examples:
  ./scripts/run-worker.sh worker-1
  ./scripts/run-worker.sh worker-2 http://localhost:5000
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 ]]; then
  echo "Error: worker_id is required" >&2
  usage >&2
  exit 1
fi

WORKER_ID="$1"
API_BASE="${2:-${API_BASE:-http://localhost:4000}}"

export WORKER_ID
export API_BASE

cd "$PROJECT_ROOT"

echo "Starting worker '${WORKER_ID}' against ${API_BASE}..."
exec node scripts/worker-runner.mjs
