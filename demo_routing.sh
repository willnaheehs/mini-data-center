#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-openclaw-dev}"
KUBECONFIG_PATH="${KUBECONFIG:-/home/openclaw/.kube/config}"
POLICY="${1:-round_robin}"
JOB_NAME="dispatcher-demo"

export KUBECONFIG="$KUBECONFIG_PATH"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

need kubectl
need python3

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

section "Current pods"
kubectl get pods -n "$NAMESPACE" -o wide

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
echo "If you want a cleaner before/after dataset, clear /data/job_results.csv on both workers before rerunning."
