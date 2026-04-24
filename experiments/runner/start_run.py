#!/usr/bin/env python3
import argparse
import json
import time
from pathlib import Path

from common import REPO_ROOT, api_request, manifest_path, read_json, utc_now_iso, write_json


def submit_job(job: dict, api_base: str):
    payload = {
        "type": job["api_type"],
        "params": job["params"],
        "metadata": {"experiment_run": True},
    }
    submitted_at_runner = time.perf_counter()
    submitted_at_wall = utc_now_iso()
    response = api_request("POST", "/jobs", payload=payload, api_base=api_base)
    return response["job_id"], submitted_at_runner, submitted_at_wall


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--api-base", default=None)
    args = parser.parse_args()

    manifest_file = manifest_path(args.run_id)
    manifest = read_json(manifest_file)
    if manifest.get("run_status") != "planned":
        raise SystemExit(f"Run status must be planned, got {manifest.get('run_status')}")

    workload_file = REPO_ROOT / manifest["workload_file"]
    workload = json.loads(workload_file.read_text())

    policy = manifest["policy"]
    api_request("POST", "/routing-policy", payload={"policy": policy}, api_base=args.api_base)

    submitted_job_ids = []
    submission_records = {}
    for job in workload["jobs"]:
        job_id, submitted_at_runner, submitted_at_wall = submit_job(job, api_base=args.api_base)
        submitted_job_ids.append(job_id)
        submission_records[job_id] = {
            "runner_submitted_perf_counter": submitted_at_runner,
            "runner_submitted_at": submitted_at_wall,
            "job_request": job,
        }
        time.sleep(workload.get("submit_interval_ms", 0) / 1000.0)

    manifest["timestamp_start"] = utc_now_iso()
    manifest["run_status"] = "running"
    manifest["policy_applied"] = policy
    manifest["submitted_job_ids"] = submitted_job_ids
    manifest["job_count"] = len(submitted_job_ids)
    manifest["submission_records"] = submission_records
    write_json(manifest_file, manifest)

    print(json.dumps({
        "run_id": args.run_id,
        "status": manifest["run_status"],
        "submitted_jobs": len(submitted_job_ids),
        "policy_applied": policy,
    }, indent=2))


if __name__ == "__main__":
    main()
