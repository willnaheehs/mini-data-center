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
kubectl create job "$JOB_NAME" --from=job/dispatcher -n "$NAMESPACE" --dry-run=client -o yaml \
  | python3 -c '
import sys, yaml
obj = yaml.safe_load(sys.stdin.read())
container = obj["spec"]["template"]["spec"]["containers"][0]
for env in container.get("env", []):
    if env.get("name") == "ROUTING_POLICY":
        env["value"] = sys.argv[1]
print(yaml.safe_dump(obj, sort_keys=False))
' "$POLICY" \
  | kubectl apply -f -

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
