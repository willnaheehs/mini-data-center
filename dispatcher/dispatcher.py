import json
import os
import time
import uuid
from datetime import datetime, timezone

import redis

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

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
_round_robin_index = 0
_tie_break_index = 0


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


def choose_round_robin_target():
    global _round_robin_index
    target = WORKER_QUEUES[_round_robin_index]
    _round_robin_index = (_round_robin_index + 1) % len(WORKER_QUEUES)
    decision = {
        "policy": "round_robin",
        "selection_reason": "alternating worker assignment",
    }
    return target, decision


def choose_state_aware_target():
    global _tie_break_index

    load_snapshot = []
    for worker in WORKER_QUEUES:
        queue_length = get_queue_length(worker["queue_name"])
        busy_value = get_busy_value(worker["busy_key"])
        estimated_load = queue_length + busy_value
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
    return selected, decision


def choose_target():
    if ROUTING_POLICY == "round_robin":
        return choose_round_robin_target()
    if ROUTING_POLICY == "state_aware":
        return choose_state_aware_target()

    raise ValueError(
        f"unsupported ROUTING_POLICY '{ROUTING_POLICY}'. Expected round_robin or state_aware"
    )


def submit_job(job_type, params):
    target, decision = choose_target()

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


def main():
    print(f"Dispatcher starting with ROUTING_POLICY={ROUTING_POLICY}")
    print(
        "Available worker queues: "
        + ", ".join(f"{item['worker_name']}={item['queue_name']}" for item in WORKER_QUEUES)
    )

    jobs = [
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
    ]

    for job in jobs:
        submit_job(
            job_type=job["job_type"],
            params=job["params"],
        )
        time.sleep(0.5)


if __name__ == "__main__":
    main()
