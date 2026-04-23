# Observability Stack

This directory contains a first-pass host-based observability layer for the 3-node mini data center.

## Goal

Provide visibility into:
- host health on all three nodes
- centralized logs from the cluster nodes
- a Grafana UI for dashboards
- a Prometheus metrics backend
- a Loki log backend

## Topology

### Central stack (run on `node1-control`)
- Grafana
- Prometheus
- Loki

### Node collectors (run on each node)
- Node Exporter
- Promtail
- optional custom host metrics via Node Exporter textfile collector

Run the node collectors on:
- `node1-control`
- `node2-worker`
- `node3-worker`

## Directory layout

- `control-node/`
  - `docker-compose.yml`
  - `prometheus/prometheus.yml`
  - `loki/config.yml`
- `node-collectors/`
  - `node-exporter-compose.yml`
  - `export-cpu-temp.sh`
  - `mini-dc-cpu-temp-export.service`
  - `mini-dc-cpu-temp-export.timer`
  - `textfile-collector-setup.md`
  - `promtail/config.yml`

## Notes

- This is intentionally separate from the Kubernetes app manifests so it can merge cleanly later.
- This first version is host-based via Docker Compose rather than deployed inside Kubernetes.
- Node Exporter exposes host metrics to Prometheus.
- Node Exporter textfile collector can expose custom host metrics such as CPU temperature from `lm-sensors`.
- Promtail ships logs to Loki.
- Grafana reads from Prometheus and Loki.

## Ports

Default exposed ports on `node1-control`:
- Grafana: `3000`
- Prometheus: `9090`
- Loki: `3100`

Node Exporter on each node:
- `9100`

## Suggested next steps after config creation

1. Bring up control-node stack on `node1-control`
2. Bring up node collectors on each node
3. Add Grafana datasources
4. Add dashboards for:
   - cluster overview
   - node detail
   - workload health
5. Optionally add app-level metrics from dispatcher/worker output
