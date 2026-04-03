# Observability Implementation Plan

## First pass

Implement a host-based observability stack with:
- Grafana
- Prometheus
- Loki
- Node Exporter on each node
- Promtail on each node

## Why this approach

- Keeps observability work isolated from the Kubernetes demo manifests
- Easier to deploy incrementally on a fixed 3-node lab cluster
- Merge-safe because changes live mostly under `observability/`
- Provides immediate value before adding custom application metrics

## Data sources covered in first pass

### Metrics
- Host metrics from Node Exporter
- Prometheus self-metrics

### Logs
- System logs from `/var/log/*.log`
- Docker container logs from `/var/lib/docker/containers/*/*-json.log`
- Worker CSV output from `/tmp/mini-dc-logs/*.csv`

## Known future improvements

- Add Grafana datasource provisioning
- Add Grafana dashboard provisioning
- Add Redis exporter
- Add kube-state-metrics and/or cAdvisor
- Add app-native metrics endpoint for dispatcher and worker
- Parse CSV/job output into real Prometheus metrics if needed
- Secure default credentials and exposed ports before long-term use
