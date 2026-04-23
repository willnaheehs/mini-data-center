# Node Exporter textfile collector setup

This adds custom host metrics to Node Exporter using the textfile collector.

## What this enables

- custom Prometheus metrics written as `.prom` files on each node
- first metric: `mini_dc_cpu_temperature_celsius`
- data source: `lm-sensors` (`sensors` command)

## Files

- `node-exporter-compose.yml` - enables the textfile collector and mounts the collector directory
- `export-cpu-temp.sh` - writes CPU package temperature to a `.prom` file
- `mini-dc-cpu-temp-export.service` - oneshot systemd unit that runs the exporter script
- `mini-dc-cpu-temp-export.timer` - systemd timer that refreshes the metric automatically after boot and every 15 seconds

## One-time setup on each node

1. Create the collector directory:

   ```bash
   sudo mkdir -p /var/lib/node_exporter/textfile_collector
   ```

2. Copy the script onto the node:

   ```bash
   sudo install -m 0755 export-cpu-temp.sh /usr/local/bin/export-cpu-temp.sh
   ```

3. Install the systemd units:

   ```bash
   sudo install -m 0644 mini-dc-cpu-temp-export.service /etc/systemd/system/mini-dc-cpu-temp-export.service
   sudo install -m 0644 mini-dc-cpu-temp-export.timer /etc/systemd/system/mini-dc-cpu-temp-export.timer
   sudo systemctl daemon-reload
   ```

4. Test the script manually once:

   ```bash
   sudo /usr/local/bin/export-cpu-temp.sh
   cat /var/lib/node_exporter/textfile_collector/mini_dc_cpu_temp.prom
   ```

5. Enable and start the timer so it survives reboots and refreshes every 15 seconds:

   ```bash
   sudo systemctl enable --now mini-dc-cpu-temp-export.timer
   ```

6. Restart node-exporter from the `observability/node-collectors` directory:

   ```bash
   docker compose -f node-exporter-compose.yml up -d
   ```

## Verification

Check that the timer is active:

```bash
systemctl status mini-dc-cpu-temp-export.timer
systemctl list-timers mini-dc-cpu-temp-export.timer
```

On the node:

```bash
curl -s http://localhost:9100/metrics | grep mini_dc_cpu_temperature_celsius
```

In Prometheus:

```promql
mini_dc_cpu_temperature_celsius
```

## Notes

- This script intentionally exports `Package id 0`, which is the cleanest CPU temperature for your report/dashboard story.
- If a node exposes a different sensor label later, adjust the awk match in the script.
- The script writes atomically via a temporary file so Prometheus never reads a partial metric file.
- With the provided systemd timer, the metric refreshes automatically after boot and every 15 seconds without manual restarts.
