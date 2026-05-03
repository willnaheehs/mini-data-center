#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
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


def summarize_series(values: list[float]) -> dict:
    return {
        "avg": mean(values),
        "p95": percentile(values, 0.95),
        "max": max(values),
        "min": min(values),
    }


def flatten_result_rows(manifest: dict, results_dir: Path) -> list[dict]:
    rows = []
    submission_records = manifest.get("submission_records", {}) or {}
    for path in sorted(results_dir.glob("*.json")):
        payload = read_json(path)
        status = payload.get("status", {})
        result = payload.get("result", {})
        metrics = result.get("metrics", {})
        runner_observation = payload.get("runner_observation", {})
        result_body = result.get("result", {})
        metadata = result.get("metadata", {})
        job_id = result.get("job_id") or status.get("job_id")
        job_request = (submission_records.get(job_id, {}) or {}).get("job_request", {})
        params = job_request.get("params", {})
        task_name = params.get("task") or result_body.get("task") or result.get("worker_job_type")

        rows.append(
            {
                "run_id": manifest.get("run_id"),
                "policy": manifest.get("policy_applied") or manifest.get("policy"),
                "workload_file": manifest.get("workload_file"),
                "job_type": result.get("job_type") or status.get("job_type"),
                "worker_job_type": result.get("worker_job_type"),
                "task": task_name,
                "submission_interval_ms": manifest.get("submit_interval_ms_effective", manifest.get("submit_interval_ms")),
                "job_id": job_id,
                "status": result.get("status") or status.get("status"),
                "target_worker": result.get("target_worker") or status.get("target_worker"),
                "actual_worker": result.get("worker_name") or status.get("worker_name"),
                "submitted_at": result.get("submitted_at") or status.get("submitted_at"),
                "started_at": result.get("started_at") or status.get("started_at"),
                "finished_at": result.get("finished_at") or status.get("finished_at"),
                "runner_observed_total_time": runner_observation.get("runner_observed_total_time"),
                "wait_time": metrics.get("wait_time"),
                "service_time": metrics.get("service_time"),
                "total_time": metrics.get("total_time"),
                "cpu_temperature_celsius": metrics.get("cpu_temperature_celsius"),
                "experiment_run": metadata.get("experiment_run"),
                "result_summary": result.get("result_summary"),
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    manifest_file = manifest_path(args.run_id)
    manifest = read_json(manifest_file)
    run_root = manifest_file.parent
    results_dir = run_root / "results" / "job-results"
    metrics_dir = run_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    runner_totals = []
    worker_totals, waits, services, cpu_temps = [], [], [], []
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

        runner_observation = payload.get("runner_observation", {})
        observed_total = runner_observation.get("runner_observed_total_time")
        if observed_total is not None:
            runner_totals.append(float(observed_total))

        metrics = result.get("metrics", {})
        extracted = {
            "worker_total_time": metric_value(metrics, "total_time", "total_seconds"),
            "wait_time": metric_value(metrics, "wait_time", "wait_seconds"),
            "service_time": metric_value(metrics, "service_time", "service_seconds"),
            "cpu_temperature_celsius": metric_value(metrics, "cpu_temperature_celsius"),
        }

        if extracted["service_time"] is not None:
            services.append(extracted["service_time"])
        if extracted["cpu_temperature_celsius"] is not None:
            cpu_temps.append(extracted["cpu_temperature_celsius"])
        for key, bucket in [("worker_total_time", worker_totals), ("wait_time", waits)]:
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
        "run_id": manifest.get("run_id", args.run_id),
        "policy": manifest.get("policy"),
        "policy_applied": manifest.get("policy_applied") or manifest.get("policy"),
        "workload_file": manifest.get("workload_file"),
        "job_type": "compute",
        "requested_job_count": manifest.get("job_count"),
        "expanded_job_count": manifest.get("expanded_job_count"),
        "submit_interval_ms_requested": manifest.get("submit_interval_ms"),
        "submit_interval_ms_effective": manifest.get("submit_interval_ms_effective", manifest.get("submit_interval_ms")),
        "submission_pattern": manifest.get("submission_pattern"),
        "timestamp_start": manifest.get("timestamp_start"),
        "timestamp_end": manifest.get("timestamp_end"),
        "job_count": completed + failed,
        "completed_count": completed,
        "failed_count": failed,
        "jobs_per_worker": jobs_per_worker,
    }
    if runner_totals:
        summary["runner_observed_total_time"] = summarize_series(runner_totals)
    if services:
        summary["service_time"] = summarize_series(services)
    positive_worker_totals = [v for v in worker_totals if v >= 0]
    positive_waits = [v for v in waits if v >= 0]
    if positive_worker_totals:
        summary["worker_total_time"] = summarize_series(positive_worker_totals)
    if positive_waits:
        summary["wait_time"] = summarize_series(positive_waits)
    if cpu_temps:
        summary["cpu_temperature_celsius"] = summarize_series(cpu_temps)

    if negative_metric_warnings:
        summary["warnings"] = {
            "negative_cross_host_timing_metrics_detected": True,
            "details": negative_metric_warnings,
        }

    write_json(metrics_dir / "summary.json", summary)
    detailed_rows = flatten_result_rows(manifest, results_dir)
    if detailed_rows:
        with (metrics_dir / "job_results_summary.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(detailed_rows[0].keys()))
            writer.writeheader()
            for row in detailed_rows:
                writer.writerow(row)

    run_summary_row = {
        "run_id": summary["run_id"],
        "policy": summary["policy"],
        "policy_applied": summary["policy_applied"],
        "workload_file": summary["workload_file"],
        "job_type": summary["job_type"],
        "requested_job_count": summary["requested_job_count"],
        "expanded_job_count": summary["expanded_job_count"],
        "submit_interval_ms_requested": summary["submit_interval_ms_requested"],
        "submit_interval_ms_effective": summary["submit_interval_ms_effective"],
        "submission_pattern": summary["submission_pattern"],
        "timestamp_start": summary["timestamp_start"],
        "timestamp_end": summary["timestamp_end"],
        "job_count": summary["job_count"],
        "completed_count": summary["completed_count"],
        "failed_count": summary["failed_count"],
        "runner_observed_total_time_avg": (summary.get("runner_observed_total_time") or {}).get("avg"),
        "runner_observed_total_time_p95": (summary.get("runner_observed_total_time") or {}).get("p95"),
        "service_time_avg": (summary.get("service_time") or {}).get("avg"),
        "service_time_p95": (summary.get("service_time") or {}).get("p95"),
        "worker_total_time_avg": (summary.get("worker_total_time") or {}).get("avg"),
        "worker_total_time_p95": (summary.get("worker_total_time") or {}).get("p95"),
        "wait_time_avg": (summary.get("wait_time") or {}).get("avg"),
        "wait_time_p95": (summary.get("wait_time") or {}).get("p95"),
        "cpu_temperature_avg": (summary.get("cpu_temperature_celsius") or {}).get("avg"),
        "cpu_temperature_p95": (summary.get("cpu_temperature_celsius") or {}).get("p95"),
    }
    for name in ["run_summary.csv", "summary.csv"]:
        with (metrics_dir / name).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(run_summary_row.keys()))
            writer.writeheader()
            writer.writerow(run_summary_row)

    manifest["summary_status"] = "completed"
    write_json(manifest_file, manifest)
    import json
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
