#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --policy <policy> --workload <profile> --run <number> [extra new_run args]" >&2
  exit 1
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

[[ $# -ge 6 ]] || usage
RUN_DIR="$($SCRIPT_DIR/new_run.sh "$@")"
RUN_ID="$(basename "$RUN_DIR")"
START_TS="$(date --iso-8601=seconds)"

python3 - <<PY
import json
from pathlib import Path
path = Path("$RUN_DIR/manifest.json")
data = json.loads(path.read_text())
data["timestamp_start"] = "$START_TS"
data["run_status"] = "started"
path.write_text(json.dumps(data, indent=2) + "\n")
PY

echo "Run created: $RUN_ID"
echo "Manifest updated with start timestamp."
echo "Next steps:"
echo "  1. Launch the workload using your current submission path."
echo "  2. When the run completes, execute:"
echo "     $SCRIPT_DIR/collect_artifacts.sh --run-dir $RUN_DIR"
echo "     $SCRIPT_DIR/summarize_results.py --run-dir $RUN_DIR"
echo "     $SCRIPT_DIR/capture_screenshots.sh --run-dir $RUN_DIR"
