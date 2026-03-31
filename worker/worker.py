import csv
import json
import os
import time
from datetime import datetime, timezone

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT_NUM", "6379"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "jobs")
WORKER_NAME = os.getenv("WORKER_NAME", os.uname().nodename)
RESULTS_PATH = os.getenv("RESULTS_PATH", "/data/job_results.csv")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

CSV_FIELDS = [
    "job_id",
    "job_type",
    "worker_node",
    "submitted_at",
    "started_at",
    "finished_at",
    "queue_wait_sec",
    "execution_sec",
    "total_latency_sec",
    "status",
    "result_summary",
]


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def ensure_results_file():
    results_dir = os.path.dirname(RESULTS_PATH)
    if results_dir:
        os.makedirs(results_dir, exist_ok=True)

    if not os.path.exists(RESULTS_PATH):
        with open(RESULTS_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()


def append_result_row(row: dict):
    ensure_results_file()
    with open(RESULTS_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(row)


def write_failed_result(job, started_at: datetime, error_msg: str):
    finished_at = datetime.now(timezone.utc)

    submitted_at_str = job.get("submitted_at", "")
    job_id = job.get("job_id", "unknown")
    job_type = job.get("job_type", "unknown")

    queue_wait_sec = ""
    total_latency_sec = ""

    if submitted_at_str:
        try:
            submitted_dt = parse_iso(submitted_at_str)
            queue_wait_sec = (started_at - submitted_dt).total_seconds()
            total_latency_sec = (finished_at - submitted_dt).total_seconds()
        except Exception:
            pass

    row = {
        "job_id": job_id,
        "job_type": job_type,
        "worker_node": WORKER_NAME,
        "submitted_at": submitted_at_str,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "queue_wait_sec": queue_wait_sec,
        "execution_sec": (finished_at - started_at).total_seconds(),
        "total_latency_sec": total_latency_sec,
        "status": "failed",
        "result_summary": error_msg,
    }
    append_result_row(row)


def validate_job(job: dict):
    required_keys = ["job_id", "job_type", "submitted_at", "params"]
    for key in required_keys:
        if key not in job:
            raise ValueError(f"missing required field: {key}")

    if not isinstance(job["params"], dict):
        raise ValueError("params must be a dictionary")

    parse_iso(job["submitted_at"])

    if job["job_type"] == "sleep":
        if "duration_sec" not in job["params"]:
            raise ValueError("missing params.duration_sec")
        duration = job["params"]["duration_sec"]
        if not isinstance(duration, (int, float)):
            raise ValueError("params.duration_sec must be numeric")
        if duration <= 0:
            raise ValueError("params.duration_sec must be positive")

    elif job["job_type"] == "cpu":
        if "work_units" not in job["params"]:
            raise ValueError("missing params.work_units")
        work_units = job["params"]["work_units"]
        if not isinstance(work_units, int):
            raise ValueError("params.work_units must be an integer")
        if work_units <= 0:
            raise ValueError("params.work_units must be positive")

    else:
        raise ValueError(f"unsupported job_type: {job['job_type']}")


def process_sleep_job(job: dict):
    duration = job["params"]["duration_sec"]
    submitted_dt = parse_iso(job["submitted_at"])
    started_at = datetime.now(timezone.utc)

    print(f"Worker {WORKER_NAME} received job {job['job_id']} of type sleep")
    print(f"Worker {WORKER_NAME} starting sleep job {job['job_id']} for {duration} sec")

    time.sleep(duration)

    finished_at = datetime.now(timezone.utc)

    queue_wait_sec = (started_at - submitted_dt).total_seconds()
    execution_sec = (finished_at - started_at).total_seconds()
    total_latency_sec = (finished_at - submitted_dt).total_seconds()

    row = {
        "job_id": job["job_id"],
        "job_type": job["job_type"],
        "worker_node": WORKER_NAME,
        "submitted_at": job["submitted_at"],
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "queue_wait_sec": queue_wait_sec,
        "execution_sec": execution_sec,
        "total_latency_sec": total_latency_sec,
        "status": "completed",
        "result_summary": f"slept for {duration} seconds",
    }
    append_result_row(row)

    print(f"Worker {WORKER_NAME} finished sleep job {job['job_id']} in {execution_sec:.2f} sec")


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False

    limit = int(n ** 0.5) + 1
    for i in range(3, limit, 2):
        if n % i == 0:
            return False
    return True


def process_cpu_job(job: dict):
    work_units = job["params"]["work_units"]
    submitted_dt = parse_iso(job["submitted_at"])
    started_at = datetime.now(timezone.utc)

    print(f"Worker {WORKER_NAME} received job {job['job_id']} of type cpu")
    print(f"Worker {WORKER_NAME} starting cpu job {job['job_id']} with work_units={work_units}")

    prime_count = 0
    for n in range(2, work_units + 1):
        if is_prime(n):
            prime_count += 1

    finished_at = datetime.now(timezone.utc)

    queue_wait_sec = (started_at - submitted_dt).total_seconds()
    execution_sec = (finished_at - started_at).total_seconds()
    total_latency_sec = (finished_at - submitted_dt).total_seconds()

    row = {
        "job_id": job["job_id"],
        "job_type": job["job_type"],
        "worker_node": WORKER_NAME,
        "submitted_at": job["submitted_at"],
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "queue_wait_sec": queue_wait_sec,
        "execution_sec": execution_sec,
        "total_latency_sec": total_latency_sec,
        "status": "completed",
        "result_summary": f"counted {prime_count} primes up to {work_units}",
    }
    append_result_row(row)

    print(f"Worker {WORKER_NAME} finished cpu job {job['job_id']} in {execution_sec:.2f} sec")


def main():
    ensure_results_file()
    print(f"Worker {WORKER_NAME} listening on queue '{QUEUE_NAME}'")

    while True:
        _, raw_job = r.brpop(QUEUE_NAME)
        started_at = datetime.now(timezone.utc)

        try:
            job = json.loads(raw_job)
            validate_job(job)

            if job["job_type"] == "sleep":
                process_sleep_job(job)
            elif job["job_type"] == "cpu":
                process_cpu_job(job)

        except Exception as e:
            error_msg = str(e)
            print(f"Worker {WORKER_NAME} failed to process job: {error_msg}")

            try:
                parsed_job = json.loads(raw_job) if isinstance(raw_job, str) else {"raw_job": str(raw_job)}
            except Exception:
                parsed_job = {"raw_job": str(raw_job)}

            write_failed_result(parsed_job, started_at, error_msg)


if __name__ == "__main__":
    main()
