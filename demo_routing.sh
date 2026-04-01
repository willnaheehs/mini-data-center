#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-openclaw-dev}"
KUBECONFIG_PATH="${KUBECONFIG:-/home/openclaw/.kube/config}"
POLICY="round_robin"
JOB_NAME="dispatcher-demo"
CLEAR_RESULTS=0
CAPTURE_OUTPUT=0
OUTPUT_DIR="$(dirname "$0")/demo-output"

usage() {
  cat <<EOF
Usage: $0 [round_robin|state_aware] [--clear-results] [--capture-output]

Options:
  --clear-results   remove /data/job_results.csv on both workers before the run
  --capture-output  save full script output under demo-output/
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    round_robin|state_aware)
      POLICY="$1"
      shift
      ;;
    --clear-results)
      CLEAR_RESULTS=1
      shift
      ;;
    --capture-output)
      CAPTURE_OUTPUT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

export KUBECONFIG="$KUBECONFIG_PATH"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

need kubectl
need python3

if [[ "$CAPTURE_OUTPUT" -eq 1 ]]; then
  mkdir -p "$OUTPUT_DIR"
  RUN_STAMP="$(date +%Y%m%d-%H%M%S)"
  LOG_PATH="$OUTPUT_DIR/${RUN_STAMP}-${POLICY}.log"
  exec > >(tee "$LOG_PATH") 2>&1
fi

section() {
  echo
  echo "=================================================================="
  echo "$1"
  echo "=================================================================="
}

section "Mini data center routing demo"
echo "Namespace : $NAMESPACE"
echo "KUBECONFIG: $KUBECONFIG"
echo "Policy    : $POLICY"
if [[ "$CAPTURE_OUTPUT" -eq 1 ]]; then
  echo "Output log: $LOG_PATH"
fi

section "Current pods"
kubectl get pods -n "$NAMESPACE" -o wide

if [[ "$CLEAR_RESULTS" -eq 1 ]]; then
  section "Clear persisted CSV files on both workers"
  kubectl exec -n "$NAMESPACE" deploy/worker-node2 -- sh -c 'rm -f /data/job_results.csv && echo cleared node2'
  kubectl exec -n "$NAMESPACE" deploy/worker-node3 -- sh -c 'rm -f /data/job_results.csv && echo cleared node3'
fi

section "Delete any previous demo job"
kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found=true

section "Create fresh dispatcher job with selected routing policy"
python3 - <<'PY' "$POLICY" "$NAMESPACE" "$JOB_NAME" "$(dirname "$0")/k8s/dispatcher.yaml" | kubectl apply -f -
import sys, yaml
policy, namespace, job_name, manifest_path = sys.argv[1:5]
with open(manifest_path, 'r', encoding='utf-8') as f:
    src = yaml.safe_load(f)
container = src['spec']['template']['spec']['containers'][0]
for env in container.get('env', []):
    if env.get('name') == 'ROUTING_POLICY':
        env['value'] = policy
job = {
    'apiVersion': 'batch/v1',
    'kind': 'Job',
    'metadata': {
        'name': job_name,
        'namespace': namespace,
    },
    'spec': {
        'template': src['spec']['template']
    }
}
print(yaml.safe_dump(job, sort_keys=False))
PY

section "Wait for dispatcher job to complete"
kubectl wait --for=condition=complete job/"$JOB_NAME" -n "$NAMESPACE" --timeout=180s

section "Dispatcher logs"
kubectl logs -n "$NAMESPACE" job/"$JOB_NAME"

section "Worker node2 recent logs"
kubectl logs -n "$NAMESPACE" deploy/worker-node2 --tail=80

section "Worker node3 recent logs"
kubectl logs -n "$NAMESPACE" deploy/worker-node3 --tail=80

section "Worker node2 CSV"
kubectl exec -n "$NAMESPACE" deploy/worker-node2 -- cat /data/job_results.csv

section "Worker node3 CSV"
kubectl exec -n "$NAMESPACE" deploy/worker-node3 -- cat /data/job_results.csv

section "Summary"
echo "Demo finished successfully."
if [[ "$CAPTURE_OUTPUT" -eq 1 ]]; then
  echo "Saved full output to: $LOG_PATH"
fi
