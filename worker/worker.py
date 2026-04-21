import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import redis
from prometheus_client import Counter, Gauge, Histogram, start_http_server

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT_NUM", "6379"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "jobs")
WORKER_NAME = os.getenv("WORKER_NAME", os.uname().nodename)
NODE_NAME = os.getenv("NODE_NAME", os.uname().nodename)
RESULTS_PATH = os.getenv("RESULTS_PATH", "/data/job_results.csv")
BUSY_KEY = os.getenv("BUSY_KEY", f"busy-{WORKER_NAME}")
BUSY_KEY_TTL_SEC = int(os.getenv("BUSY_KEY_TTL_SEC", "300"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))
JOB_STATUS_PREFIX = os.getenv("JOB_STATUS_PREFIX", "job-status:")
JOB_RESULT_PREFIX = os.getenv("JOB_RESULT_PREFIX", "job-result:")
JOB_TTL_SEC = int(os.getenv("JOB_TTL_SEC", str(7 * 24 * 60 * 60)))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

CSV_FIELDS = [
    "job_id",
    "job_type",
    "policy",
    "target_worker",
    "actual_worker",
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

worker_jobs_processed_total = Counter(
    "mini_dc_worker_jobs_processed_total",
    "Total number of jobs processed by worker",
    ["worker_name", "node_name", "job_type", "status"],
)

worker_job_failures_total = Counter(
    "mini_dc_worker_job_failures_total",
    "Total number of worker job failures",
    ["worker_name", "node_name", "job_type"],
)

worker_busy = Gauge(
    "mini_dc_worker_busy",
    "Whether the worker is currently busy processing a job",
    ["worker_name", "node_name"],
)

worker_last_job_timestamp = Gauge(
    "mini_dc_worker_last_job_timestamp",
    "Unix timestamp of the last job completion or failure",
    ["worker_name", "node_name"],
)

worker_wait_seconds = Histogram(
    "mini_dc_worker_wait_seconds",
    "Time spent waiting between submission and execution start",
    ["worker_name", "node_name", "job_type"],
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60),
)

worker_service_seconds = Histogram(
    "mini_dc_worker_service_seconds",
    "Time spent actively executing a job",
    ["worker_name", "node_name", "job_type"],
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60),
)

worker_total_seconds = Histogram(
    "mini_dc_worker_total_seconds",
    "Total time from submission to completion",
    ["worker_name", "node_name", "job_type"],
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60),
)

worker_target_mismatch_total = Counter(
    "mini_dc_worker_target_mismatch_total",
    "Count of jobs processed by a worker other than their target worker",
    ["worker_name", "node_name", "target_worker"],
)

worker_sleep_duration_seconds = Gauge(
    "mini_dc_worker_sleep_duration_seconds",
    "Latest observed sleep job duration in seconds",
    ["worker_name", "node_name"],
)

worker_cpu_work_units = Gauge(
    "mini_dc_worker_cpu_work_units",
    "Latest observed CPU job work units",
    ["worker_name", "node_name"],
)


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


def redis_get_json(key: str):
    raw = r.get(key)
    if raw is None:
        return None
    return json.loads(raw)


def redis_set_json(key: str, payload: dict):
    r.set(key, json.dumps(payload))
    r.expire(key, JOB_TTL_SEC)


def set_worker_busy(is_busy: bool):
    busy_value = 1 if is_busy else 0
    if is_busy:
        r.set(BUSY_KEY, busy_value, ex=BUSY_KEY_TTL_SEC)
    else:
        r.set(BUSY_KEY, busy_value)
    worker_busy.labels(worker_name=WORKER_NAME, node_name=NODE_NAME).set(busy_value)


def get_submit_time(job: dict) -> str:
    return job.get("submit_time") or job.get("submitted_at", "")


def get_submit_datetime(job: dict):
    submit_time_str = get_submit_time(job)
    if not submit_time_str:
        return None
    try:
        return parse_iso(submit_time_str)
    except Exception:
        return None


def observe_job_size(job: dict):
    job_type = job.get("job_type", "unknown")
    params = job.get("params", {})

    if job_type == "sleep" and "duration_sec" in params:
        worker_sleep_duration_seconds.labels(worker_name=WORKER_NAME, node_name=NODE_NAME).set(params["duration_sec"])
    elif job_type == "cpu" and "work_units" in params:
        worker_cpu_work_units.labels(worker_name=WORKER_NAME, node_name=NODE_NAME).set(params["work_units"])


def observe_mismatch(job: dict):
    target_worker = job.get("target_worker", "")
    if target_worker and target_worker != WORKER_NAME:
        worker_target_mismatch_total.labels(
            worker_name=WORKER_NAME,
            node_name=NODE_NAME,
            target_worker=target_worker,
        ).inc()


def observe_result_metrics(job: dict, row: dict):
    job_type = job.get("job_type", "unknown")
    status = row.get("status", "unknown")

    worker_jobs_processed_total.labels(
        worker_name=WORKER_NAME,
        node_name=NODE_NAME,
        job_type=job_type,
        status=status,
    ).inc()

    if status == "failed":
        worker_job_failures_total.labels(
            worker_name=WORKER_NAME,
            node_name=NODE_NAME,
            job_type=job_type,
        ).inc()

    wait_time = row.get("wait_time")
    total_time = row.get("total_time")
    service_time = row.get("service_time")

    if isinstance(wait_time, (int, float)) and wait_time >= 0:
        worker_wait_seconds.labels(
            worker_name=WORKER_NAME,
            node_name=NODE_NAME,
            job_type=job_type,
        ).observe(wait_time)

    if isinstance(service_time, (int, float)) and service_time >= 0:
        worker_service_seconds.labels(
            worker_name=WORKER_NAME,
            node_name=NODE_NAME,
            job_type=job_type,
        ).observe(service_time)

    if isinstance(total_time, (int, float)) and total_time >= 0:
        worker_total_seconds.labels(
            worker_name=WORKER_NAME,
            node_name=NODE_NAME,
            job_type=job_type,
        ).observe(total_time)

    worker_last_job_timestamp.labels(worker_name=WORKER_NAME, node_name=NODE_NAME).set(time.time())


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
        "actual_worker": WORKER_NAME,
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


def build_result_payload(job: dict, row: dict, extra_result: dict | None = None):
    payload = {
        "job_id": row["job_id"],
        "job_type": job.get("api_job_type", job.get("job_type", "unknown")),
        "worker_job_type": job.get("job_type", "unknown"),
        "status": row["status"],
        "submitted_at": row["submit_time"],
        "started_at": row["start_time"],
        "finished_at": row["finish_time"],
        "worker_name": WORKER_NAME,
        "target_worker": job.get("target_worker") or None,
        "result_summary": row["result_summary"],
        "result": extra_result or {},
        "metrics": {
            "wait_time": row["wait_time"],
            "service_time": row["service_time"],
            "total_time": row["total_time"],
        },
        "metadata": job.get("metadata", {}),
    }
    return payload


def persist_job_status(job: dict, row: dict, extra_result: dict | None = None, error: str | None = None):
    job_id = row["job_id"]
    result_payload = build_result_payload(job, row, extra_result=extra_result)
    status_payload = {
        "job_id": job_id,
        "status": row["status"],
        "job_type": job.get("api_job_type", job.get("job_type", "unknown")),
        "submitted_at": row["submit_time"],
        "started_at": row["start_time"],
        "finished_at": row["finish_time"],
        "worker_name": WORKER_NAME,
        "target_worker": job.get("target_worker") or None,
        "error": error,
        "result": result_payload["result"] if row["status"] == "completed" else None,
    }
    redis_set_json(f"{JOB_STATUS_PREFIX}{job_id}", status_payload)
    redis_set_json(f"{JOB_RESULT_PREFIX}{job_id}", result_payload)


def write_failed_result(job, started_at: datetime, error_msg: str):
    finished_at = datetime.now(timezone.utc)
    row = build_result_row(job, started_at, finished_at, "failed", error_msg)
    append_result_row(row)
    observe_result_metrics(job, row)
    persist_job_status(job, row, error=error_msg)


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

    elif job["job_type"] == "file":
        operation = job["params"].get("operation")
        if operation not in {"word_count", "line_count", "byte_count"}:
            raise ValueError("file jobs require supported operation: word_count, line_count, or byte_count")
        file_path = job["params"].get("file_path")
        if not file_path or not isinstance(file_path, str):
            raise ValueError("file jobs require params.file_path")

    elif job["job_type"] == "ml":
        operation = job["params"].get("operation")
        if operation != "threshold_classify":
            raise ValueError("ml jobs currently support only threshold_classify")
        values = job["params"].get("values")
        if not isinstance(values, list) or not values:
            raise ValueError("ml jobs require a non-empty params.values list")
        threshold = job["params"].get("threshold")
        if not isinstance(threshold, (int, float)):
            raise ValueError("ml jobs require numeric params.threshold")

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
    observe_result_metrics(job, row)
    persist_job_status(
        job,
        row,
        extra_result={
            "duration_sec": duration,
            "message": f"slept for {duration} seconds",
        },
    )

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
    observe_result_metrics(job, row)
    persist_job_status(
        job,
        row,
        extra_result={
            "work_units": work_units,
            "prime_count": prime_count,
            "message": f"counted {prime_count} primes up to {work_units}",
        },
    )

    print(f"Worker {WORKER_NAME} finished cpu job {job['job_id']} in {row['service_time']:.2f} sec")


def process_file_job(job: dict):
    params = job["params"]
    operation = params["operation"]
    file_path = Path(params["file_path"])
    started_at = datetime.now(timezone.utc)

    if not file_path.exists():
        raise FileNotFoundError(f"input file not found: {file_path}")

    print(
        f"Worker {WORKER_NAME} received file job {job['job_id']} from queue {QUEUE_NAME} "
        f"targeted for {job.get('target_worker', 'unknown')}"
    )
    print(f"Worker {WORKER_NAME} starting file job {job['job_id']} with operation={operation} path={file_path}")

    content = file_path.read_bytes()
    if operation == "byte_count":
        result_value = len(content)
    else:
        text = content.decode("utf-8")
        if operation == "line_count":
            result_value = len(text.splitlines())
        elif operation == "word_count":
            result_value = len(text.split())
        else:
            raise ValueError(f"unsupported file operation: {operation}")

    finished_at = datetime.now(timezone.utc)
    row = build_result_row(
        job,
        started_at,
        finished_at,
        "completed",
        f"{operation}={result_value} for {file_path.name}",
    )
    append_result_row(row)
    observe_result_metrics(job, row)
    persist_job_status(
        job,
        row,
        extra_result={
            "operation": operation,
            "file_name": params.get("file_name", file_path.name),
            "file_path": str(file_path),
            "value": result_value,
        },
    )

    print(f"Worker {WORKER_NAME} finished file job {job['job_id']} in {row['service_time']:.2f} sec")


def process_ml_job(job: dict):
    params = job["params"]
    values = params["values"]
    threshold = params["threshold"]
    started_at = datetime.now(timezone.utc)

    print(
        f"Worker {WORKER_NAME} received ml job {job['job_id']} from queue {QUEUE_NAME} "
        f"targeted for {job.get('target_worker', 'unknown')}"
    )
    print(f"Worker {WORKER_NAME} starting ml job {job['job_id']} with threshold={threshold}")

    predictions = [1 if value >= threshold else 0 for value in values]
    positive_count = sum(predictions)

    finished_at = datetime.now(timezone.utc)
    row = build_result_row(
        job,
        started_at,
        finished_at,
        "completed",
        f"classified {len(values)} samples with {positive_count} positives",
    )
    append_result_row(row)
    observe_result_metrics(job, row)
    persist_job_status(
        job,
        row,
        extra_result={
            "operation": params.get("operation", "threshold_classify"),
            "threshold": threshold,
            "sample_count": len(values),
            "positive_count": positive_count,
            "predictions": predictions,
        },
    )

    print(f"Worker {WORKER_NAME} finished ml job {job['job_id']} in {row['service_time']:.2f} sec")


def main():
    start_http_server(METRICS_PORT)
    ensure_results_file()
    set_worker_busy(False)
    print(f"Worker {WORKER_NAME} metrics listening on port {METRICS_PORT}")
    print(f"Worker {WORKER_NAME} listening on queue '{QUEUE_NAME}' with busy key '{BUSY_KEY}'")

    while True:
        _, raw_job = r.brpop(QUEUE_NAME)
        started_at = datetime.now(timezone.utc)
        set_worker_busy(True)

        try:
            job = json.loads(raw_job)
            validate_job(job)
            observe_job_size(job)
            observe_mismatch(job)

            status_doc = redis_get_json(f"{JOB_STATUS_PREFIX}{job['job_id']}") if job.get("job_id") else None
            if status_doc is not None:
                status_doc["status"] = "running"
                status_doc["started_at"] = started_at.isoformat()
                status_doc["worker_name"] = WORKER_NAME
                status_doc["target_worker"] = job.get("target_worker") or None
                redis_set_json(f"{JOB_STATUS_PREFIX}{job['job_id']}", status_doc)

            if job["job_type"] == "sleep":
                process_sleep_job(job)
            elif job["job_type"] == "cpu":
                process_cpu_job(job)
            elif job["job_type"] == "file":
                process_file_job(job)
            elif job["job_type"] == "ml":
                process_ml_job(job)

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
