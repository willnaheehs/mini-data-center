#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-/var/lib/node_exporter/textfile_collector}"
OUTPUT_FILE="${OUTPUT_DIR}/mini_dc_cpu_temp.prom"
TMP_FILE="${OUTPUT_FILE}.tmp"
METRIC_NAME="mini_dc_cpu_temperature_celsius"

mkdir -p "$OUTPUT_DIR"

package_line="$(sensors 2>/dev/null | awk '/^Package id 0:/ { print; exit }')"
if [[ -z "$package_line" ]]; then
  echo "Package id 0 not found in sensors output" >&2
  exit 1
fi

package_temp="$(awk '{print $4}' <<<"$package_line" | tr -d '+°C')"
if [[ -z "$package_temp" ]]; then
  echo "Failed to parse Package id 0 temperature" >&2
  exit 1
fi

cat > "$TMP_FILE" <<EOF
# HELP ${METRIC_NAME} CPU package temperature reported by lm-sensors.
# TYPE ${METRIC_NAME} gauge
${METRIC_NAME} ${package_temp}
EOF

mv "$TMP_FILE" "$OUTPUT_FILE"
