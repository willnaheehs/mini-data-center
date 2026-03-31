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
NODE_NAME = os.getenv("NODE_NAME", os.uname().nodename)
RESULTS_PATH = os.getenv("RESULTS_PATH", "/data/job_results.csv")
BUSY_KEY = os.getenv("BUSY_KEY", f"busy-{WORKER_NAME}")
BUSY_KEY_TTL_SEC = int(os.getenv("BUSY_KEY_TTL_SEC", "300"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

CSV_FIELDS = [
    "job_id",
    "job_type",
    "policy",
    "target_worker",
    "target_queue",
    "worker_name",
    "node_name",
    "submit_time",
    "start_time",
    "finish_time",
    "wait_time",
    "service_time",
    "total_time",
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


def set_worker_busy(is_busy: bool):
    if is_busy:
        r.set(BUSY_KEY, 1, ex=BUSY_KEY_TTL_SEC)
    else:
        r.set(BUSY_KEY, 0)


def get_submit_time(job: dict) -> str:
    return job.get("submit_time") or job.get("submitted_at", "")


def build_result_row(job: dict, started_at: datetime, finished_at: datetime, status: str, result_summary: str):
    submit_time_str = get_submit_time(job)
    wait_time = ""
    total_time = ""

    if submit_time_str:
        try:
            submitted_dt = parse_iso(submit_time_str)
            wait_time = (started_at - submitted_dt).total_seconds()
            total_time = (finished_at - submitted_dt).total_seconds()
        except Exception:
            pass

    return {
        "job_id": job.get("job_id", "unknown"),
        "job_type": job.get("job_type", "unknown"),
        "policy": job.get("policy", "unknown"),
        "target_worker": job.get("target_worker", ""),
        "target_queue": job.get("target_queue", QUEUE_NAME),
        "worker_name": WORKER_NAME,
        "node_name": NODE_NAME,
        "submit_time": submit_time_str,
        "start_time": started_at.isoformat(),
        "finish_time": finished_at.isoformat(),
        "wait_time": wait_time,
        "service_time": (finished_at - started_at).total_seconds(),
        "total_time": total_time,
        "status": status,
        "result_summary": result_summary,
    }


def write_failed_result(job, started_at: datetime, error_msg: str):
    finished_at = datetime.now(timezone.utc)
    row = build_result_row(job, started_at, finished_at, "failed", error_msg)
    append_result_row(row)


def validate_job(job: dict):
    required_keys = ["job_id", "job_type", "params"]
    for key in required_keys:
        if key not in job:
            raise ValueError(f"missing required field: {key}")

    if not get_submit_time(job):
        raise ValueError("missing submit_time/submitted_at")

    if not isinstance(job["params"], dict):
        raise ValueError("params must be a dictionary")

    parse_iso(get_submit_time(job))

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
    started_at = datetime.now(timezone.utc)

    print(
        f"Worker {WORKER_NAME} received sleep job {job['job_id']} from queue {QUEUE_NAME} "
        f"targeted for {job.get('target_worker', 'unknown')}"
    )
    print(f"Worker {WORKER_NAME} starting sleep job {job['job_id']} for {duration} sec")

    time.sleep(duration)

    finished_at = datetime.now(timezone.utc)
    row = build_result_row(
        job,
        started_at,
        finished_at,
        "completed",
        f"slept for {duration} seconds",
    )
    append_result_row(row)

    print(f"Worker {WORKER_NAME} finished sleep job {job['job_id']} in {row['service_time']:.2f} sec")


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
    started_at = datetime.now(timezone.utc)

    print(
        f"Worker {WORKER_NAME} received cpu job {job['job_id']} from queue {QUEUE_NAME} "
        f"targeted for {job.get('target_worker', 'unknown')}"
    )
    print(f"Worker {WORKER_NAME} starting cpu job {job['job_id']} with work_units={work_units}")

    prime_count = 0
    for n in range(2, work_units + 1):
        if is_prime(n):
            prime_count += 1

    finished_at = datetime.now(timezone.utc)
    row = build_result_row(
        job,
        started_at,
        finished_at,
        "completed",
        f"counted {prime_count} primes up to {work_units}",
    )
    append_result_row(row)

    print(f"Worker {WORKER_NAME} finished cpu job {job['job_id']} in {row['service_time']:.2f} sec")


def main():
    ensure_results_file()
    set_worker_busy(False)
    print(f"Worker {WORKER_NAME} listening on queue '{QUEUE_NAME}' with busy key '{BUSY_KEY}'")

    while True:
        _, raw_job = r.brpop(QUEUE_NAME)
        started_at = datetime.now(timezone.utc)
        set_worker_busy(True)

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
        finally:
            set_worker_busy(False)


if __name__ == "__main__":
    main()
