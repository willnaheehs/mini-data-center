#!/usr/bin/env python3
import argparse
import concurrent.futures
import copy
import json
import random
import time
from pathlib import Path

from common import REPO_ROOT, api_request, manifest_path, read_json, utc_now_iso, write_json


def expand_workload_jobs(workload: dict, job_count_override: int = 0) -> list[dict]:
    if "job_groups" in workload:
        jobs = []
        for group in workload["job_groups"]:
            repeat = int(group.get("repeat", 1))
            if repeat < 1:
                continue
            job = group["job"]
            for _ in range(repeat):
                jobs.append(copy.deepcopy(job))
    else:
        jobs = copy.deepcopy(workload.get("jobs", []))

    if workload.get("shuffle_jobs"):
        rng = random.Random(workload.get("shuffle_seed", 42))
        rng.shuffle(jobs)

    if job_count_override > 0:
        if not jobs:
            return []
        if job_count_override <= len(jobs):
            jobs = jobs[:job_count_override]
        else:
            repeated = []
            while len(repeated) < job_count_override:
                repeated.extend(copy.deepcopy(jobs))
            jobs = repeated[:job_count_override]

    return copy.deepcopy(jobs)


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


def submit_job_on_schedule(index: int, job: dict, api_base: str, start_perf: float, submit_interval_ms: int):
    target_perf = start_perf + ((submit_interval_ms / 1000.0) * index)
    delay = target_perf - time.perf_counter()
    if delay > 0:
        time.sleep(delay)
    job_id, submitted_at_runner, submitted_at_wall = submit_job(job, api_base=api_base)
    return {
        "index": index,
        "job_id": job_id,
        "submitted_at_runner": submitted_at_runner,
        "submitted_at_wall": submitted_at_wall,
        "scheduled_offset_ms": index * submit_interval_ms,
        "job": job,
    }


def submit_jobs_via_server_batch(expanded_jobs: list[dict], policy: str, submit_interval_ms: int, api_base: str | None):
    payload = {
        "policy": policy,
        "submit_interval_ms": submit_interval_ms,
        "jobs": [
            {
                "api_type": job["api_type"],
                "params": job["params"],
                "metadata": {"experiment_run": True},
            }
            for job in expanded_jobs
        ],
    }
    response = api_request("POST", "/experiment-runs/submit-batch", payload=payload, api_base=api_base)
    submitted = []
    for item in response.get("submissions", []):
        submitted.append({
            "index": item["index"],
            "job_id": item["job_id"],
            "submitted_at_runner": None,
            "submitted_at_wall": item.get("submitted_at"),
            "scheduled_offset_ms": item.get("scheduled_offset_ms", item["index"] * submit_interval_ms),
            "job": expanded_jobs[item["index"]],
        })
    return {
        "policy_applied": response.get("policy_applied", policy),
        "submit_interval_ms_effective": response.get("submit_interval_ms_effective", submit_interval_ms),
        "submitted": submitted,
        "submission_pattern": "server_bulk",
    }


def submit_jobs_via_client_requests(expanded_jobs: list[dict], policy: str, submit_interval_ms: int, api_base: str | None):
    api_request("POST", "/routing-policy", payload={"policy": policy}, api_base=api_base)
    start_perf = time.perf_counter()
    max_workers = min(8, max(1, len(expanded_jobs)))
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for index, job in enumerate(expanded_jobs):
            futures.append(executor.submit(submit_job_on_schedule, index, job, api_base, start_perf, submit_interval_ms))

    submitted = []
    for future in futures:
        submitted.append(future.result())
    return {
        "policy_applied": policy,
        "submit_interval_ms_effective": submit_interval_ms,
        "submitted": submitted,
        "submission_pattern": "api_driven",
    }


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
    requested_job_count = int(manifest.get("job_count") or 0)
    expanded_jobs = expand_workload_jobs(workload, job_count_override=requested_job_count)
    manifest_submit_interval = manifest.get("submit_interval_ms")
    if manifest_submit_interval is None:
        submit_interval_ms = int(workload.get("submit_interval_ms", 0))
    else:
        submit_interval_ms = int(manifest_submit_interval)

    policy = manifest["policy"]

    try:
        submission_result = submit_jobs_via_server_batch(expanded_jobs, policy, submit_interval_ms, args.api_base)
    except RuntimeError as exc:
        if "failed (404)" not in str(exc):
            raise
        submission_result = submit_jobs_via_client_requests(expanded_jobs, policy, submit_interval_ms, args.api_base)

    submission_records = {}
    submitted = submission_result["submitted"]
    submitted.sort(key=lambda item: item["index"])
    submitted_job_ids = []
    for item in submitted:
        submitted_job_ids.append(item["job_id"])
        submission_records[item["job_id"]] = {
            "runner_submitted_perf_counter": item["submitted_at_runner"],
            "runner_submitted_at": item["submitted_at_wall"],
            "scheduled_offset_ms": item["scheduled_offset_ms"],
            "job_request": item["job"],
        }

    manifest["timestamp_start"] = utc_now_iso()
    manifest["run_status"] = "running"
    manifest["policy_applied"] = submission_result["policy_applied"]
    manifest["submitted_job_ids"] = submitted_job_ids
    manifest["job_count"] = len(submitted_job_ids)
    manifest["expanded_job_count"] = len(expanded_jobs)
    manifest["submit_interval_ms_effective"] = submission_result["submit_interval_ms_effective"]
    manifest["submission_pattern"] = submission_result["submission_pattern"]
    manifest["submission_records"] = submission_records
    write_json(manifest_file, manifest)

    print(json.dumps({
        "run_id": args.run_id,
        "status": manifest["run_status"],
        "submitted_jobs": len(submitted_job_ids),
        "policy_applied": submission_result["policy_applied"],
        "submit_interval_ms_effective": submission_result["submit_interval_ms_effective"],
        "submission_pattern": submission_result["submission_pattern"],
    }, indent=2))


if __name__ == "__main__":
    main()
