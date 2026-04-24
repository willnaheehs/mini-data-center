#!/usr/bin/env python3
import argparse
import csv
from statistics import mean

from common import manifest_path, read_json, write_json


def percentile(values: list[float], p: float) -> float:
    values = sorted(values)
    if not values:
        raise ValueError("empty values")
    idx = round((len(values) - 1) * p)
    return values[idx]


def metric_value(metrics: dict, *names: str):
    for name in names:
        if name in metrics and metrics[name] is not None:
            return float(metrics[name])
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    manifest_file = manifest_path(args.run_id)
    run_root = manifest_file.parent
    results_dir = run_root / "results" / "job-results"
    metrics_dir = run_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    totals, waits, services = [], [], []
    jobs_per_worker = {}
    completed = failed = 0
    negative_metric_warnings = []

    for path in sorted(results_dir.glob("*.json")):
        payload = read_json(path)
        result = payload.get("result", {})
        status = result.get("status") or payload.get("status", {}).get("status")
        if status == "completed":
            completed += 1
        elif status in {"failed", "deleted"}:
            failed += 1

        metrics = result.get("metrics", {})
        extracted = {
            "total_time": metric_value(metrics, "total_time", "total_seconds"),
            "wait_time": metric_value(metrics, "wait_time", "wait_seconds"),
            "service_time": metric_value(metrics, "service_time", "service_seconds"),
        }

        for key, bucket in [("total_time", totals), ("wait_time", waits), ("service_time", services)]:
            value = extracted[key]
            if value is not None:
                bucket.append(value)
                if value < 0:
                    negative_metric_warnings.append({
                        "job_file": path.name,
                        "metric": key,
                        "value": value,
                    })

        worker = result.get("worker_name") or payload.get("status", {}).get("worker_name") or "unknown"
        jobs_per_worker[worker] = jobs_per_worker.get(worker, 0) + 1

    summary = {
        "job_count": completed + failed,
        "completed_count": completed,
        "failed_count": failed,
        "jobs_per_worker": jobs_per_worker,
    }
    for name, values in [("total_time", totals), ("wait_time", waits), ("service_time", services)]:
        if values:
            summary[name] = {
                "avg": mean(values),
                "p95": percentile(values, 0.95),
                "max": max(values),
                "min": min(values),
            }

    if negative_metric_warnings:
        summary["warnings"] = {
            "negative_timing_metrics_detected": True,
            "details": negative_metric_warnings,
        }

    write_json(metrics_dir / "summary.json", summary)
    with (metrics_dir / "summary.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        writer.writerow(["job_count", summary["job_count"]])
        writer.writerow(["completed_count", summary["completed_count"]])
        writer.writerow(["failed_count", summary["failed_count"]])
        for metric in ["total_time", "wait_time", "service_time"]:
            if metric in summary:
                writer.writerow([f"{metric}_avg", summary[metric]["avg"]])
                writer.writerow([f"{metric}_p95", summary[metric]["p95"]])
                writer.writerow([f"{metric}_max", summary[metric]["max"]])
                writer.writerow([f"{metric}_min", summary[metric]["min"]])

    manifest = read_json(manifest_file)
    manifest["summary_status"] = "completed"
    write_json(manifest_file, manifest)
    import json
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
