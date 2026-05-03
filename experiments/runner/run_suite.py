#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

from common import RUNS_ROOT, utc_now_iso, write_json
from presets import build_class_project_runs, class_project_suite_definition

SUITES_ROOT = RUNS_ROOT / "_suites"


def suite_dir(suite_id: str) -> Path:
    return SUITES_ROOT / suite_id


def suite_manifest_path(suite_id: str) -> Path:
    return suite_dir(suite_id) / "suite_manifest.json"


def suite_summary_csv_path(suite_id: str) -> Path:
    return suite_dir(suite_id) / "suite_summary.csv"


def run_script(script_name: str, *args: str, api_base: str | None = None) -> str:
    script_path = Path(__file__).resolve().parent / script_name
    cmd = [sys.executable, str(script_path), *args]
    if api_base:
        cmd.extend(["--api-base", api_base])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"{script_name} failed")
    return proc.stdout.strip()


def build_run_id(suite_id: str, workload_label: str, policy: str) -> str:
    return f"{suite_id}-{workload_label}-{policy.replace('_', '-')}"


def write_suite_summary_csv(suite_id: str, manifest: dict) -> None:
    rows = []
    for run in manifest.get("runs", []):
        summary = run.get("summary") or {}
        rows.append(
            {
                "suite_id": suite_id,
                "run_id": run.get("run_id"),
                "policy": run.get("policy"),
                "workload_label": run.get("workload_label"),
                "workload_file": run.get("workload_file"),
                "job_type": summary.get("job_type", "compute"),
                "submit_interval_ms": summary.get("submit_interval_ms_effective"),
                "job_count": summary.get("job_count"),
                "completed_count": summary.get("completed_count"),
                "failed_count": summary.get("failed_count"),
                "avg_wait_time": (summary.get("wait_time") or {}).get("avg"),
                "p95_wait_time": (summary.get("wait_time") or {}).get("p95"),
                "avg_service_time": (summary.get("service_time") or {}).get("avg"),
                "p95_service_time": (summary.get("service_time") or {}).get("p95"),
                "avg_total_time": (summary.get("worker_total_time") or {}).get("avg"),
                "p95_total_time": (summary.get("worker_total_time") or {}).get("p95"),
                "run_status": run.get("run_status"),
            }
        )

    path = suite_summary_csv_path(suite_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else [
        "suite_id", "run_id", "policy", "workload_label", "workload_file", "job_type",
        "submit_interval_ms", "job_count", "completed_count", "failed_count",
        "avg_wait_time", "p95_wait_time", "avg_service_time", "p95_service_time",
        "avg_total_time", "p95_total_time", "run_status",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite-id", required=True)
    parser.add_argument("--submit-interval-ms", type=int, default=250)
    parser.add_argument("--job-count", type=int, default=0)
    parser.add_argument("--api-base", default=None)
    args = parser.parse_args()

    definition = class_project_suite_definition()
    suite_root = suite_dir(args.suite_id)
    if suite_root.exists():
        raise SystemExit(f"Suite already exists: {suite_root}")
    suite_root.mkdir(parents=True, exist_ok=True)

    manifest = {
        "suite_id": args.suite_id,
        "preset_id": definition["preset_id"],
        "title": definition["title"],
        "description": definition["description"],
        "created_at": utc_now_iso(),
        "started_at": utc_now_iso(),
        "finished_at": None,
        "status": "running",
        "submit_interval_ms": args.submit_interval_ms,
        "job_count_override": args.job_count,
        "run_count": definition["run_count"],
        "runs": [],
        "errors": [],
    }
    write_json(suite_manifest_path(args.suite_id), manifest)

    for preset in build_class_project_runs():
        run_id = build_run_id(args.suite_id, preset.workload_label, preset.policy)
        run_entry = {
            "run_id": run_id,
            "policy": preset.policy,
            "workload_label": preset.workload_label,
            "workload_file": preset.workload_file,
            "run_status": "planned",
            "summary": None,
        }
        manifest["runs"].append(run_entry)
        write_json(suite_manifest_path(args.suite_id), manifest)

        try:
            run_script(
                "create_run.py",
                "--policy", preset.policy,
                "--workload-file", preset.workload_file,
                "--run-id", run_id,
                "--notes", f"Preset suite {args.suite_id}: {preset.workload_label} + {preset.policy}",
                "--job-count", str(args.job_count),
                "--submit-interval-ms", str(args.submit_interval_ms),
            )
            run_script("start_run.py", "--run-id", run_id, api_base=args.api_base)
            run_script("collect_run.py", "--run-id", run_id, api_base=args.api_base)
            run_script("summarize_run.py", "--run-id", run_id)

            summary_path = RUNS_ROOT / run_id / "metrics" / "summary.json"
            run_entry["summary"] = json.loads(summary_path.read_text()) if summary_path.exists() else None
            run_entry["run_status"] = "completed"
        except Exception as exc:
            run_entry["run_status"] = "failed"
            manifest["errors"].append({"run_id": run_id, "error": str(exc)})
            manifest["status"] = "failed"
            write_json(suite_manifest_path(args.suite_id), manifest)
            break

        write_json(suite_manifest_path(args.suite_id), manifest)

    if manifest["status"] != "failed":
        manifest["status"] = "completed"
    manifest["finished_at"] = utc_now_iso()
    write_suite_summary_csv(args.suite_id, manifest)
    write_json(suite_manifest_path(args.suite_id), manifest)
    print(json.dumps({"suite_id": args.suite_id, "status": manifest["status"]}, indent=2))


if __name__ == "__main__":
    main()
