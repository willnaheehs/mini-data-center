import csv
import json
import math
import os
import subprocess
import sys
import tempfile
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
SCRIPT_WORKDIR_ROOT = os.getenv("SCRIPT_WORKDIR_ROOT", "/tmp/mini-dc-script-runs")

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
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

worker_service_seconds = Histogram(
    "mini_dc_worker_service_seconds",
    "Time spent actively executing a job",
    ["worker_name", "node_name", "job_type"],
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

worker_total_seconds = Histogram(
    "mini_dc_worker_total_seconds",
    "Total time from submission to completion",
    ["worker_name", "node_name", "job_type"],
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

worker_target_mismatch_total = Counter(
    "mini_dc_worker_target_mismatch_total",
    "Count of jobs processed by a worker other than their target worker",
    ["worker_name", "node_name", "target_worker"],
)

worker_compute_size = Gauge(
    "mini_dc_worker_compute_size",
    "Latest observed compute job size hint",
    ["worker_name", "node_name", "task"],
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


def observe_job_size(job: dict):
    job_type = job.get("job_type", "unknown")
    params = job.get("params", {})
    if job_type == "compute":
        task = params.get("task", "unknown")
        size_hint = params.get("params", {}).get("n") or params.get("params", {}).get("samples") or params.get("params", {}).get("size") or 0
        worker_compute_size.labels(worker_name=WORKER_NAME, node_name=NODE_NAME, task=task).set(float(size_hint or 0))


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
        worker_wait_seconds.labels(worker_name=WORKER_NAME, node_name=NODE_NAME, job_type=job_type).observe(wait_time)
    if isinstance(service_time, (int, float)) and service_time >= 0:
        worker_service_seconds.labels(worker_name=WORKER_NAME, node_name=NODE_NAME, job_type=job_type).observe(service_time)
    if isinstance(total_time, (int, float)) and total_time >= 0:
        worker_total_seconds.labels(worker_name=WORKER_NAME, node_name=NODE_NAME, job_type=job_type).observe(total_time)

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
    return {
        "job_id": row["job_id"],
        "job_type": job.get("api_job_type", job.get("job_type", "unknown")),
        "worker_job_type": job.get("job_type", "unknown"),
        "status": row["status"],
        "submitted_at": row["submit_time"],
        "dispatched_at": job.get("dispatched_at"),
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


def persist_job_status(job: dict, row: dict, extra_result: dict | None = None, error: str | None = None):
    job_id = row["job_id"]
    result_payload = build_result_payload(job, row, extra_result=extra_result)
    status_payload = {
        "job_id": job_id,
        "status": row["status"],
        "job_type": job.get("api_job_type", job.get("job_type", "unknown")),
        "submitted_at": row["submit_time"],
        "dispatched_at": job.get("dispatched_at"),
        "started_at": row["start_time"],
        "finished_at": row["finish_time"],
        "worker_name": WORKER_NAME,
        "target_worker": job.get("target_worker") or None,
        "error": error,
        "result": result_payload["result"] if row["status"] == "completed" else None,
    }
    redis_set_json(f"{JOB_STATUS_PREFIX}{job_id}", status_payload)
    redis_set_json(f"{JOB_RESULT_PREFIX}{job_id}", result_payload)


def update_job_running_status(job: dict, started_at: datetime):
    status_doc = redis_get_json(f"{JOB_STATUS_PREFIX}{job['job_id']}") if job.get("job_id") else None
    if status_doc is not None:
        status_doc["status"] = "running"
        status_doc["started_at"] = started_at.isoformat()
        status_doc["worker_name"] = WORKER_NAME
        status_doc["target_worker"] = job.get("target_worker") or None
        redis_set_json(f"{JOB_STATUS_PREFIX}{job['job_id']}", status_doc)


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

    if job["job_type"] == "compute":
        task = job["params"].get("task")
        if task not in {"prime_count", "monte_carlo_pi", "sort_numbers", "matrix_multiply"}:
            raise ValueError("unsupported compute task")
    elif job["job_type"] == "ml":
        operation = job["params"].get("operation")
        if operation != "linear_binary_classify":
            raise ValueError("ml jobs currently support only linear_binary_classify")
    elif job["job_type"] == "python_script":
        script_path = job["params"].get("script_path")
        if not script_path:
            raise ValueError("python_script job requires script_path")
    else:
        raise ValueError(f"unsupported job_type: {job['job_type']}")


def count_primes(limit: int) -> int:
    def is_prime(n: int) -> bool:
        if n < 2:
            return False
        if n == 2:
            return True
        if n % 2 == 0:
            return False
        upper = int(n ** 0.5) + 1
        for i in range(3, upper, 2):
            if n % i == 0:
                return False
        return True

    total = 0
    for n in range(2, limit + 1):
        if is_prime(n):
            total += 1
    return total


def process_compute_job(job: dict):
    params = job["params"]
    task = params["task"]
    task_params = params.get("params", {})
    started_at = datetime.now(timezone.utc)

    print(f"Worker {WORKER_NAME} starting compute job {job['job_id']} task={task}")

    if task == "prime_count":
        n = int(task_params.get("n", 10000))
        value = count_primes(n)
        result = {"task": task, "n": n, "prime_count": value}
        summary = f"counted {value} primes up to {n}"
    elif task == "monte_carlo_pi":
        samples = int(task_params.get("samples", 100000))
        inside = 0
        for i in range(samples):
            x = ((i * 9301 + 49297) % 233280) / 233280.0
            y = ((i * 233280 + 49297) % 9301) / 9301.0
            if x * x + y * y <= 1:
                inside += 1
        estimate = 4.0 * inside / samples
        result = {"task": task, "samples": samples, "pi_estimate": estimate}
        summary = f"estimated pi as {estimate:.6f} using {samples} samples"
    elif task == "sort_numbers":
        values = task_params.get("values")
        if not isinstance(values, list) or not values:
            raise ValueError("sort_numbers requires params.values list")
        sorted_values = sorted(values)
        result = {"task": task, "count": len(values), "sorted_values": sorted_values}
        summary = f"sorted {len(values)} numbers"
    elif task == "matrix_multiply":
        size = int(task_params.get("size", 12))
        a = [[(row + col) % 7 + 1 for col in range(size)] for row in range(size)]
        b = [[(row * col) % 5 + 1 for col in range(size)] for row in range(size)]
        c = [[0 for _ in range(size)] for _ in range(size)]
        for i in range(size):
            for j in range(size):
                total = 0
                for k in range(size):
                    total += a[i][k] * b[k][j]
                c[i][j] = total
        checksum = sum(sum(row) for row in c)
        result = {"task": task, "size": size, "checksum": checksum}
        summary = f"multiplied {size}x{size} matrices"
    else:
        raise ValueError(f"unsupported compute task: {task}")

    finished_at = datetime.now(timezone.utc)
    row = build_result_row(job, started_at, finished_at, "completed", summary)
    append_result_row(row)
    observe_result_metrics(job, row)
    persist_job_status(job, row, extra_result=result)


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def process_ml_job(job: dict):
    params = job["params"]
    features = params["features"]
    weights = params["weights"]
    bias = float(params.get("bias", 0.0))
    threshold = float(params.get("threshold", 0.5))
    started_at = datetime.now(timezone.utc)

    print(f"Worker {WORKER_NAME} starting ml job {job['job_id']} operation=linear_binary_classify")

    scores = []
    predictions = []
    for row in features:
        if len(row) != len(weights):
            raise ValueError("each feature row must match weights length")
        score = sum(value * weight for value, weight in zip(row, weights)) + bias
        probability = sigmoid(score)
        scores.append(probability)
        predictions.append(1 if probability >= threshold else 0)

    positive_count = sum(predictions)
    finished_at = datetime.now(timezone.utc)
    row = build_result_row(job, started_at, finished_at, "completed", f"classified {len(features)} samples")
    append_result_row(row)
    observe_result_metrics(job, row)
    persist_job_status(
        job,
        row,
        extra_result={
            "operation": "linear_binary_classify",
            "sample_count": len(features),
            "scores": scores,
            "predictions": predictions,
            "positive_count": positive_count,
            "threshold": threshold,
        },
    )


def process_python_script_job(job: dict):
    params = job["params"]
    script_path = Path(params["script_path"])
    timeout_seconds = int(params.get("timeout_seconds", 30))
    started_at = datetime.now(timezone.utc)

    if not script_path.exists():
        raise FileNotFoundError(f"script not found: {script_path}")

    os.makedirs(SCRIPT_WORKDIR_ROOT, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"{job['job_id']}-", dir=SCRIPT_WORKDIR_ROOT) as workdir:
        copied_script = Path(workdir) / script_path.name
        copied_script.write_bytes(script_path.read_bytes())

        completed = subprocess.run(
            [sys.executable, str(copied_script)],
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

        stdout_text = completed.stdout[-12000:]
        stderr_text = completed.stderr[-12000:]
        exit_code = completed.returncode
        status = "completed" if exit_code == 0 else "failed"
        summary = f"python script exited with code {exit_code}"
        finished_at = datetime.now(timezone.utc)
        row = build_result_row(job, started_at, finished_at, status, summary)
        append_result_row(row)
        observe_result_metrics(job, row)
        persist_job_status(
            job,
            row,
            extra_result={
                "script_name": params.get("script_name", script_path.name),
                "exit_code": exit_code,
                "stdout": stdout_text,
                "stderr": stderr_text,
            },
            error=None if exit_code == 0 else stderr_text or f"script exited with code {exit_code}",
        )

        if exit_code != 0:
            print(f"Worker {WORKER_NAME} python job {job['job_id']} failed with code {exit_code}")


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
            update_job_running_status(job, started_at)

            if job["job_type"] == "compute":
                process_compute_job(job)
            elif job["job_type"] == "ml":
                process_ml_job(job)
            elif job["job_type"] == "python_script":
                process_python_script_job(job)
        except subprocess.TimeoutExpired:
            try:
                parsed_job = json.loads(raw_job) if isinstance(raw_job, str) else {"raw_job": str(raw_job)}
            except Exception:
                parsed_job = {"raw_job": str(raw_job)}
            write_failed_result(parsed_job, started_at, "script execution timed out")
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
