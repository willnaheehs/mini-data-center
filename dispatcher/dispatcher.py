import json
import time
import uuid
from datetime import datetime, timezone
import os

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT_NUM", "6379"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "jobs")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def submit_job(job_type, params):
    job = {
        "job_id": str(uuid.uuid4()),
        "job_type": job_type,
        "submitted_at": utc_now(),
        "params": params,
    }
    r.lpush(QUEUE_NAME, json.dumps(job))
    print(f"submitted {job['job_id']} -> {job}")

def main():
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
