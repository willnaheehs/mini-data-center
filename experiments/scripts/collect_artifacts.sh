#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --run-dir <path> [--results-source <path>] [--logs-source <path>]" >&2
  exit 1
}

RUN_DIR=""
RESULTS_SOURCE="/tmp/mini-dc-logs"
LOGS_SOURCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir) RUN_DIR="$2"; shift 2 ;;
    --results-source) RESULTS_SOURCE="$2"; shift 2 ;;
    --logs-source) LOGS_SOURCE="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n "$RUN_DIR" ]] || usage
mkdir -p "$RUN_DIR/results" "$RUN_DIR/logs"

if [[ -d "$RESULTS_SOURCE" ]]; then
  cp -a "$RESULTS_SOURCE"/. "$RUN_DIR/results/"
fi

if [[ -n "$LOGS_SOURCE" && -d "$LOGS_SOURCE" ]]; then
  cp -a "$LOGS_SOURCE"/. "$RUN_DIR/logs/"
fi

echo "Artifacts collected into $RUN_DIR"
