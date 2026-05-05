# Observability Stack

- host health on all three nodes
- centralized logs from the cluster nodes
- a Grafana UI for dashboards
- a Prometheus metrics backend
- a Loki log backend

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
