#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --run-dir <path>" >&2
  exit 1
}

RUN_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir) RUN_DIR="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n "$RUN_DIR" ]] || usage
RUN_ID="$(basename "$RUN_DIR")"
SCREEN_DIR="$RUN_DIR/screenshots"
mkdir -p "$SCREEN_DIR"

cat <<EOFMSG
Capture the following screenshots and save them with these exact names:
  $SCREEN_DIR/${RUN_ID}-cluster-overview.png
  $SCREEN_DIR/${RUN_ID}-node2-detail.png
  $SCREEN_DIR/${RUN_ID}-node3-detail.png
  $SCREEN_DIR/${RUN_ID}-worker-execution.png

Recommended dashboard captures:
- Cluster Overview
- Node Detail filtered to node2-worker
- Node Detail filtered to node3-worker
- Worker Execution (if relevant to this run)
EOFMSG
