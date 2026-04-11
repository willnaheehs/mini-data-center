import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT_NUM", "6379"))
ROUTING_POLICY = os.getenv("ROUTING_POLICY", "round_robin").strip().lower()
WORKER_QUEUES = [
    {
        "worker_name": "node2-worker",
        "queue_name": os.getenv("QUEUE_NODE2", "jobs-node2"),
        "busy_key": os.getenv("BUSY_KEY_NODE2", "busy-node2"),
    },
    {
        "worker_name": "node3-worker",
        "queue_name": os.getenv("QUEUE_NODE3", "jobs-node3"),
        "busy_key": os.getenv("BUSY_KEY_NODE3", "busy-node3"),
    },
]

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")
PROMETHEUS_JOB_NAME = os.getenv("PROMETHEUS_JOB_NAME", "mini-dc-dispatcher")
PROMETHEUS_PUSH_ENABLED = os.getenv("PROMETHEUS_PUSH_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
JOB_FILE_PATH = os.getenv("JOB_FILE_PATH", "").strip()
DEFAULT_SUBMIT_INTERVAL_MS = int(os.getenv("SUBMIT_INTERVAL_MS", "500"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
_round_robin_index = 0
_tie_break_index = 0

registry = CollectorRegistry()

dispatch_jobs_submitted_total = Counter(
    "mini_dc_dispatch_jobs_submitted_total",
    "Total number of jobs submitted by the dispatcher",
    ["policy", "job_type", "target_worker", "target_queue"],
    registry=registry,
)

dispatch_failures_total = Counter(
    "mini_dc_dispatch_failures_total",
    "Total number of dispatcher submission failures",
    ["policy", "reason"],
    registry=registry,
)

dispatch_queue_length = Gauge(
    "mini_dc_dispatch_queue_length",
    "Observed queue length per worker queue at dispatch time",
    ["worker", "queue"],
    registry=registry,
)

dispatch_worker_busy = Gauge(
    "mini_dc_dispatch_worker_busy",
    "Observed worker busy flag at dispatch time",
    ["worker", "busy_key"],
    registry=registry,
)

dispatch_estimated_load = Gauge(
    "mini_dc_dispatch_estimated_load",
    "Observed estimated load per worker at dispatch time",
    ["worker"],
    registry=registry,
)

dispatch_policy_decisions_total = Counter(
    "mini_dc_dispatch_policy_decisions_total",
    "Total routing decisions made by dispatcher policy",
    ["policy", "selected_worker", "tie_break_used"],
    registry=registry,
)

dispatch_sleep_duration_seconds = Gauge(
    "mini_dc_dispatch_sleep_duration_seconds",
    "Latest submitted sleep job duration in seconds",
    ["policy", "target_worker"],
    registry=registry,
)

dispatch_cpu_work_units = Gauge(
    "mini_dc_dispatch_cpu_work_units",
    "Latest submitted CPU job work units",
    ["policy", "target_worker"],
    registry=registry,
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def get_queue_length(queue_name: str) -> int:
    return int(r.llen(queue_name))


def get_busy_value(busy_key: str) -> int:
    raw_value = r.get(busy_key)
    if raw_value is None:
        return 0
    try:
        return int(raw_value)
    except ValueError:
        return 0


def observe_worker_snapshot(worker_name: str, queue_name: str, busy_key: str, queue_length: int, busy_value: int):
    estimated_load = queue_length + busy_value
    dispatch_queue_length.labels(worker=worker_name, queue=queue_name).set(queue_length)
    dispatch_worker_busy.labels(worker=worker_name, busy_key=busy_key).set(busy_value)
    dispatch_estimated_load.labels(worker=worker_name).set(estimated_load)
    return estimated_load


def observe_job_size(job_type: str, params: dict, policy: str, target_worker: str):
    if job_type == "sleep" and "duration_sec" in params:
        dispatch_sleep_duration_seconds.labels(policy=policy, target_worker=target_worker).set(params["duration_sec"])
    elif job_type == "cpu" and "work_units" in params:
        dispatch_cpu_work_units.labels(policy=policy, target_worker=target_worker).set(params["work_units"])


def push_metrics(grouping_key=None):
    if not PROMETHEUS_PUSH_ENABLED:
        return
    try:
        push_to_gateway(
            PUSHGATEWAY_URL,
            job=PROMETHEUS_JOB_NAME,
            registry=registry,
            grouping_key=grouping_key or {},
        )
    except Exception as exc:
        print(f"warning: failed to push dispatcher metrics to Pushgateway: {exc}")


def choose_round_robin_target():
    global _round_robin_index
    target = WORKER_QUEUES[_round_robin_index]
    _round_robin_index = (_round_robin_index + 1) % len(WORKER_QUEUES)

    queue_length = get_queue_length(target["queue_name"])
    busy_value = get_busy_value(target["busy_key"])
    estimated_load = observe_worker_snapshot(
        target["worker_name"],
        target["queue_name"],
        target["busy_key"],
        queue_length,
        busy_value,
    )

    decision = {
        "policy": "round_robin",
        "selection_reason": "alternating worker assignment",
        "tie_break_used": False,
        "load_snapshot": {
            target["worker_name"]: {
                "queue_name": target["queue_name"],
                "queue_length": queue_length,
                "busy_value": busy_value,
                "estimated_load": estimated_load,
            }
        },
    }
    return target, decision


def choose_state_aware_target():
    global _tie_break_index

    load_snapshot = []
    for worker in WORKER_QUEUES:
        queue_length = get_queue_length(worker["queue_name"])
        busy_value = get_busy_value(worker["busy_key"])
        estimated_load = observe_worker_snapshot(
            worker["worker_name"],
            worker["queue_name"],
            worker["busy_key"],
            queue_length,
            busy_value,
        )
        load_snapshot.append(
            {
                **worker,
                "queue_length": queue_length,
                "busy_value": busy_value,
                "estimated_load": estimated_load,
            }
        )

    min_load = min(item["estimated_load"] for item in load_snapshot)
    candidates = [item for item in load_snapshot if item["estimated_load"] == min_load]

    if len(candidates) == 1:
        selected = candidates[0]
        tie_break_used = False
    else:
        selected = candidates[_tie_break_index % len(candidates)]
        _tie_break_index = (_tie_break_index + 1) % len(candidates)
        tie_break_used = True

    decision = {
        "policy": "state_aware",
        "selection_reason": "lowest estimated load",
        "tie_break_used": tie_break_used,
        "load_snapshot": {
            item["worker_name"]: {
                "queue_name": item["queue_name"],
                "queue_length": item["queue_length"],
                "busy_value": item["busy_value"],
                "estimated_load": item["estimated_load"],
            }
            for item in load_snapshot
        },
    }
    dispatch_policy_decisions_total.labels(
        policy="state_aware",
        selected_worker=selected["worker_name"],
        tie_break_used=str(tie_break_used).lower(),
    ).inc()
    return selected, decision


def choose_target():
    if ROUTING_POLICY == "round_robin":
        target, decision = choose_round_robin_target()
        dispatch_policy_decisions_total.labels(
            policy="round_robin",
            selected_worker=target["worker_name"],
            tie_break_used="false",
        ).inc()
        return target, decision
    if ROUTING_POLICY == "state_aware":
        return choose_state_aware_target()

    raise ValueError(
        f"unsupported ROUTING_POLICY '{ROUTING_POLICY}'. Expected round_robin or state_aware"
    )


def normalize_job_entry(job: dict) -> dict:
    if not isinstance(job, dict):
        raise ValueError("each job entry must be an object")

    job_type = job.get("type") or job.get("job_type")
    if not job_type:
        raise ValueError("job entry is missing 'type'")

    normalized = {
        "job_type": str(job_type).strip().lower(),
        "params": {},
    }

    if normalized["job_type"] == "sleep":
        if "duration_sec" in job:
            duration = job["duration_sec"]
        else:
            duration = job.get("duration")
        if duration is None:
            raise ValueError("sleep job requires 'duration' or 'duration_sec'")
        normalized["params"]["duration_sec"] = duration
    elif normalized["job_type"] == "cpu":
        work_units = job.get("work_units")
        if work_units is None:
            raise ValueError("cpu job requires 'work_units'")
        normalized["params"]["work_units"] = work_units
    else:
        raise ValueError(f"unsupported job type '{normalized['job_type']}'")

    if "params" in job and isinstance(job["params"], dict):
        normalized["params"].update(job["params"])

    return normalized


def load_jobs_from_file(job_file_path: str):
    file_path = Path(job_file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"job file not found: {file_path}")

    payload = json.loads(file_path.read_text())

    if isinstance(payload, list):
        jobs = payload
        routing_policy = ROUTING_POLICY
        submit_interval_ms = DEFAULT_SUBMIT_INTERVAL_MS
    elif isinstance(payload, dict):
        jobs = payload.get("jobs", [])
        routing_policy = str(payload.get("routing_policy", ROUTING_POLICY)).strip().lower()
        submit_interval_ms = int(payload.get("submit_interval_ms", DEFAULT_SUBMIT_INTERVAL_MS))
    else:
        raise ValueError("job file must contain either a list of jobs or an object with a 'jobs' array")

    if not jobs:
        raise ValueError("job file does not contain any jobs")

    normalized_jobs = [normalize_job_entry(job) for job in jobs]
    return {
        "routing_policy": routing_policy,
        "submit_interval_ms": submit_interval_ms,
        "jobs": normalized_jobs,
        "source": str(file_path),
    }


def demo_jobs():
    return {
        "routing_policy": ROUTING_POLICY,
        "submit_interval_ms": DEFAULT_SUBMIT_INTERVAL_MS,
        "source": "built-in demo",
        "jobs": [
            {
                "job_type": "sleep",
                "params": {"duration_sec": 2},
            },
            {
                "job_type": "cpu",
                "params": {"work_units": 5000},
            },
            {
                "job_type": "cpu",
                "params": {"work_units": 20000},
            },
        ],
    }


def submit_job(job_type, params):
    policy_name = ROUTING_POLICY
    try:
        target, decision = choose_target()
        policy_name = decision["policy"]

        job = {
            "job_id": str(uuid.uuid4()),
            "job_type": job_type,
            "submit_time": utc_now(),
            "submitted_at": utc_now(),
            "params": params,
            "policy": decision["policy"],
            "target_worker": target["worker_name"],
            "target_queue": target["queue_name"],
            "dispatch_time": utc_now(),
            "dispatcher_decision": decision,
        }

        if "load_snapshot" in decision:
            for worker_name, metrics in decision["load_snapshot"].items():
                safe_name = worker_name.replace("-", "_")
                job[f"dispatch_queue_length_{safe_name}"] = metrics["queue_length"]
                job[f"dispatch_busy_{safe_name}"] = metrics["busy_value"]
                job[f"dispatch_estimated_load_{safe_name}"] = metrics["estimated_load"]

        r.lpush(target["queue_name"], json.dumps(job))
        dispatch_jobs_submitted_total.labels(
            policy=decision["policy"],
            job_type=job_type,
            target_worker=target["worker_name"],
            target_queue=target["queue_name"],
        ).inc()
        observe_job_size(job_type, params, decision["policy"], target["worker_name"])
        push_metrics(
            {
                "instance": REDIS_HOST,
                "policy": decision["policy"],
            }
        )

        if decision["policy"] == "state_aware":
            snapshot_text = ", ".join(
                f"{worker}={{queue:{metrics['queue_length']},busy:{metrics['busy_value']},load:{metrics['estimated_load']}}}"
                for worker, metrics in decision["load_snapshot"].items()
            )
            print(
                f"submitted {job['job_id']} using state_aware [{snapshot_text}] -> {target['worker_name']} ({target['queue_name']})"
            )
        else:
            print(
                f"submitted {job['job_id']} using round_robin -> {target['worker_name']} ({target['queue_name']})"
            )

        return job
    except Exception as exc:
        dispatch_failures_total.labels(policy=policy_name, reason=type(exc).__name__).inc()
        push_metrics(
            {
                "instance": REDIS_HOST,
                "policy": policy_name,
            }
        )
        raise


def main():
    global ROUTING_POLICY

    workload = load_jobs_from_file(JOB_FILE_PATH) if JOB_FILE_PATH else demo_jobs()
    ROUTING_POLICY = workload["routing_policy"]
    submit_interval_ms = workload["submit_interval_ms"]

    print(f"Dispatcher starting with ROUTING_POLICY={ROUTING_POLICY}")
    print(
        "Available worker queues: "
        + ", ".join(f"{item['worker_name']}={item['queue_name']}" for item in WORKER_QUEUES)
    )
    print(f"Job source: {workload['source']}")
    print(f"Jobs to submit: {len(workload['jobs'])}")
    print(f"Submit interval: {submit_interval_ms} ms")

    for job in workload["jobs"]:
        submit_job(
            job_type=job["job_type"],
            params=job["params"],
        )
        if submit_interval_ms > 0:
            time.sleep(submit_interval_ms / 1000.0)


if __name__ == "__main__":
    main()
