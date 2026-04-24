#!/usr/bin/env python3
import argparse
import csv
import json
from pathlib import Path
from statistics import mean

parser = argparse.ArgumentParser()
parser.add_argument("--run-dir", required=True)
args = parser.parse_args()

run_dir = Path(args.run_dir)
results_dir = run_dir / "results"
metrics_dir = run_dir / "metrics"
metrics_dir.mkdir(parents=True, exist_ok=True)

rows = []
for csv_path in results_dir.rglob("*.csv"):
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows.extend(list(reader))

summary = {
    "row_count": len(rows),
}

for key in ["wait_seconds", "service_seconds", "total_seconds"]:
    values = []
    for row in rows:
        try:
            values.append(float(row[key]))
        except Exception:
            pass
    if values:
        values_sorted = sorted(values)
        p95_index = max(0, min(len(values_sorted) - 1, round(0.95 * (len(values_sorted) - 1))))
        summary[key] = {
            "avg": mean(values),
            "max": max(values),
            "p95": values_sorted[p95_index],
        }

workers = {}
for row in rows:
    worker = row.get("worker_name") or row.get("worker") or "unknown"
    workers[worker] = workers.get(worker, 0) + 1
summary["jobs_per_worker"] = workers

(metrics_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

with (metrics_dir / "summary.csv").open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["metric", "value"])
    writer.writerow(["row_count", summary["row_count"]])
    for metric in ["wait_seconds", "service_seconds", "total_seconds"]:
        if metric in summary:
            writer.writerow([f"{metric}_avg", summary[metric]["avg"]])
            writer.writerow([f"{metric}_p95", summary[metric]["p95"]])
            writer.writerow([f"{metric}_max", summary[metric]["max"]])

print(f"Wrote summaries to {metrics_dir}")
