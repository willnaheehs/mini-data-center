#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --policy <policy> --workload <profile> --run <number> [--workload-file <path>] [--job-count <n>] [--submission-pattern <pattern>]" >&2
  exit 1
}

POLICY=""
WORKLOAD=""
RUN_NUM=""
WORKLOAD_FILE=""
JOB_COUNT="0"
SUBMISSION_PATTERN="manual"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --policy) POLICY="$2"; shift 2 ;;
    --workload) WORKLOAD="$2"; shift 2 ;;
    --run) RUN_NUM="$2"; shift 2 ;;
    --workload-file) WORKLOAD_FILE="$2"; shift 2 ;;
    --job-count) JOB_COUNT="$2"; shift 2 ;;
    --submission-pattern) SUBMISSION_PATTERN="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n "$POLICY" && -n "$WORKLOAD" && -n "$RUN_NUM" ]] || usage

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATE_STR="$(date +%F)"
RUN_ID="${DATE_STR}-${WORKLOAD}-${POLICY}-run$(printf '%02d' "$RUN_NUM")"
RUN_DIR="$ROOT_DIR/experiments/runs/$RUN_ID"
TEMPLATE_DIR="$ROOT_DIR/experiments/templates"

mkdir -p "$RUN_DIR"/{config,metrics,logs,results,screenshots}

cp "$TEMPLATE_DIR/manifest.template.json" "$RUN_DIR/manifest.json"
cp "$TEMPLATE_DIR/notes.template.md" "$RUN_DIR/notes.md"

if [[ -z "$WORKLOAD_FILE" ]]; then
  WORKLOAD_FILE="dispatcher/jobs-${WORKLOAD}.json"
fi

python3 - <<PY
from pathlib import Path
replacements = {
    "{{RUN_ID}}": "$RUN_ID",
    "{{POLICY}}": "$POLICY",
    "{{WORKLOAD_PROFILE}}": "$WORKLOAD",
    "{{WORKLOAD_FILE}}": "$WORKLOAD_FILE",
    "{{JOB_COUNT}}": "$JOB_COUNT",
    "{{SUBMISSION_PATTERN}}": "$SUBMISSION_PATTERN",
}
for rel in ["manifest.json", "notes.md"]:
    path = Path("$RUN_DIR") / rel
    text = path.read_text()
    for old, new in replacements.items():
        text = text.replace(old, new)
    path.write_text(text)
PY

for path in \
  "$ROOT_DIR/observability/control-node/prometheus/prometheus.yml" \
  "$ROOT_DIR/observability/node-collectors/node-exporter-compose.yml"; do
  if [[ -f "$path" ]]; then
    cp "$path" "$RUN_DIR/config/"
  fi
done

if [[ -f "$ROOT_DIR/$WORKLOAD_FILE" ]]; then
  mkdir -p "$RUN_DIR/config/workloads"
  cp "$ROOT_DIR/$WORKLOAD_FILE" "$RUN_DIR/config/workloads/"
fi

printf '%s\n' "$RUN_DIR"
