#!/usr/bin/env python3
import argparse
import concurrent.futures
import copy
import json
import random
import time
from pathlib import Path

from common import REPO_ROOT, api_request, manifest_path, read_json, utc_now_iso, write_json


def expand_sort_numbers(values_config: dict) -> list[int]:
    count = int(values_config.get("count", 0))
    if count <= 0:
        raise ValueError("sort_numbers count must be positive")
    pattern = values_config.get("pattern", "descending")
    start = int(values_config.get("start", count))
    if pattern == "descending":
        return list(range(start, start - count, -1))
    if pattern == "ascending":
        return list(range(start, start + count))
    if pattern == "random":
        rng = random.Random(values_config.get("seed", 42))
        values = list(range(start, start + count))
        rng.shuffle(values)
        return values
    raise ValueError(f"unsupported sort_numbers pattern: {pattern}")


def materialize_job(job: dict) -> dict:
    expanded = copy.deepcopy(job)
    params = expanded.get("params", {})
    if params.get("task") == "sort_numbers":
        task_params = params.setdefault("params", {})
        values_config = task_params.pop("values_config", None)
        if values_config and "values" not in task_params:
            task_params["values"] = expand_sort_numbers(values_config)
    return expanded


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

    return [materialize_job(job) for job in jobs]


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
    submit_interval_ms = int(manifest.get("submit_interval_ms") or workload.get("submit_interval_ms", 0))

    policy = manifest["policy"]
    api_request("POST", "/routing-policy", payload={"policy": policy}, api_base=args.api_base)

    submission_records = {}
    start_perf = time.perf_counter()
    max_workers = min(32, max(1, len(expanded_jobs)))
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for index, job in enumerate(expanded_jobs):
            futures.append(executor.submit(submit_job_on_schedule, index, job, args.api_base, start_perf, submit_interval_ms))

    submitted = [future.result() for future in futures]
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
    manifest["policy_applied"] = policy
    manifest["submitted_job_ids"] = submitted_job_ids
    manifest["job_count"] = len(submitted_job_ids)
    manifest["expanded_job_count"] = len(expanded_jobs)
    manifest["submit_interval_ms_effective"] = submit_interval_ms
    manifest["submission_records"] = submission_records
    write_json(manifest_file, manifest)

    print(json.dumps({
        "run_id": args.run_id,
        "status": manifest["run_status"],
        "submitted_jobs": len(submitted_job_ids),
        "policy_applied": policy,
        "submit_interval_ms_effective": submit_interval_ms,
    }, indent=2))


if __name__ == "__main__":
    main()
