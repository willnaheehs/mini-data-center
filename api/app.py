import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

import redis
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT_NUM", "6379"))
ROUTING_POLICY = os.getenv("ROUTING_POLICY", "shortest_queue").strip().lower()
WORKER_QUEUES = [
    {
        "worker_name": os.getenv("WORKER_NODE2_NAME", "node2-worker"),
        "queue_name": os.getenv("QUEUE_NODE2", "jobs-node2"),
        "busy_key": os.getenv("BUSY_KEY_NODE2", "busy-node2"),
    },
    {
        "worker_name": os.getenv("WORKER_NODE3_NAME", "node3-worker"),
        "queue_name": os.getenv("QUEUE_NODE3", "jobs-node3"),
        "busy_key": os.getenv("BUSY_KEY_NODE3", "busy-node3"),
    },
]
JOB_STATUS_PREFIX = os.getenv("JOB_STATUS_PREFIX", "job-status:")
JOB_RESULT_PREFIX = os.getenv("JOB_RESULT_PREFIX", "job-result:")
JOB_PAYLOAD_PREFIX = os.getenv("JOB_PAYLOAD_PREFIX", "job-payload:")
JOB_INDEX_KEY = os.getenv("JOB_INDEX_KEY", "jobs-api:index")
JOB_TTL_SEC = int(os.getenv("JOB_TTL_SEC", str(7 * 24 * 60 * 60)))
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/data/uploads")
RESULT_BASE_URL = os.getenv("RESULT_BASE_URL", "")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
app = FastAPI(title="mini-data-center api", version="0.1.0")


class ComputeParams(BaseModel):
    work_units: int = Field(..., gt=0)


class FileParams(BaseModel):
    operation: Literal["word_count", "line_count", "byte_count"] = "word_count"
    file_name: str
    file_path: Optional[str] = None


class MlParams(BaseModel):
    operation: Literal["threshold_classify"] = "threshold_classify"
    values: list[float] = Field(..., min_length=1)
    threshold: float = 0.5


class JobCreateRequest(BaseModel):
    type: Literal["compute", "file", "ml"]
    params: Dict[str, Any]
    client_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class JobCreateResponse(BaseModel):
    job_id: str
    status: str
    status_url: str
    result_url: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    job_type: Optional[str] = None
    submitted_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    worker_name: Optional[str] = None
    target_worker: Optional[str] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class FileUploadResponse(BaseModel):
    file_name: str
    file_path: str
    bytes_written: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def status_key(job_id: str) -> str:
    return f"{JOB_STATUS_PREFIX}{job_id}"


def result_key(job_id: str) -> str:
    return f"{JOB_RESULT_PREFIX}{job_id}"


def payload_key(job_id: str) -> str:
    return f"{JOB_PAYLOAD_PREFIX}{job_id}"


def ensure_upload_dir() -> None:
    os.makedirs(UPLOAD_DIR, exist_ok=True)


def redis_set_json(key: str, payload: dict) -> None:
    r.set(key, json.dumps(payload))
    r.expire(key, JOB_TTL_SEC)


def redis_get_json(key: str) -> Optional[dict]:
    raw = r.get(key)
    if raw is None:
        return None
    return json.loads(raw)


def build_status_urls(job_id: str) -> tuple[str, str]:
    base = RESULT_BASE_URL.rstrip("/")
    if base:
        return f"{base}/jobs/{job_id}", f"{base}/jobs/{job_id}/result"
    return f"/jobs/{job_id}", f"/jobs/{job_id}/result"


def get_queue_length(queue_name: str) -> int:
    return int(r.llen(queue_name))


def get_busy_value(busy_key: str) -> int:
    if not busy_key:
        return 0
    raw = r.get(busy_key)
    if raw is None:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def choose_target() -> dict:
    if ROUTING_POLICY == "round_robin":
        counter = r.incr("mini-dc-api:round-robin-index") - 1
        return WORKER_QUEUES[counter % len(WORKER_QUEUES)]

    candidates = []
    for worker in WORKER_QUEUES:
        queue_length = get_queue_length(worker["queue_name"])
        busy_value = get_busy_value(worker["busy_key"]) if ROUTING_POLICY in {"state_aware", "adaptive"} else 0
        estimated_load = queue_length + busy_value
        candidates.append(
            {
                **worker,
                "queue_length": queue_length,
                "busy_value": busy_value,
                "estimated_load": estimated_load,
            }
        )

    if ROUTING_POLICY == "power_of_two":
        import random

        sampled = random.sample(candidates, k=min(2, len(candidates)))
        sampled.sort(key=lambda item: (item["estimated_load"], item["queue_length"], item["worker_name"]))
        return sampled[0]

    if ROUTING_POLICY in {"shortest_queue", "state_aware", "adaptive"}:
        candidates.sort(key=lambda item: (item["estimated_load"], item["queue_length"], item["worker_name"]))
        return candidates[0]

    raise HTTPException(status_code=500, detail=f"unsupported ROUTING_POLICY: {ROUTING_POLICY}")


def normalize_job(request: JobCreateRequest, uploaded_file_path: Optional[str] = None) -> dict:
    job_id = str(uuid.uuid4())
    submitted_at = utc_now()

    if request.type == "compute":
        parsed = ComputeParams.model_validate(request.params)
        worker_job = {
            "job_type": "cpu",
            "params": {"work_units": parsed.work_units},
        }
    elif request.type == "file":
        params = dict(request.params)
        if uploaded_file_path and "file_path" not in params:
            params["file_path"] = uploaded_file_path
        parsed = FileParams.model_validate(params)
        worker_job = {
            "job_type": "file",
            "params": parsed.model_dump(),
        }
    elif request.type == "ml":
        parsed = MlParams.model_validate(request.params)
        worker_job = {
            "job_type": "ml",
            "params": parsed.model_dump(),
        }
    else:
        raise HTTPException(status_code=400, detail=f"unsupported job type: {request.type}")

    target = choose_target()
    return {
        "job_id": job_id,
        "job_type": worker_job["job_type"],
        "api_job_type": request.type,
        "submit_time": submitted_at,
        "submitted_at": submitted_at,
        "params": worker_job["params"],
        "policy": ROUTING_POLICY,
        "target_worker": target["worker_name"],
        "target_queue": target["queue_name"],
        "client_id": request.client_id,
        "metadata": request.metadata,
        "status": "queued",
        "dispatcher_decision": {
            "policy": ROUTING_POLICY,
            "selection_reason": "api submission",
            "load_snapshot": {
                target["worker_name"]: {
                    "queue_name": target["queue_name"],
                    "queue_length": target.get("queue_length", 0),
                    "busy_value": target.get("busy_value", 0),
                    "estimated_load": target.get("estimated_load", 0),
                }
            },
        },
    }


def store_initial_job_state(job: dict) -> None:
    job_id = job["job_id"]
    status_doc = {
        "job_id": job_id,
        "status": "queued",
        "job_type": job.get("api_job_type", job.get("job_type")),
        "submitted_at": job["submitted_at"],
        "started_at": None,
        "finished_at": None,
        "worker_name": None,
        "target_worker": job.get("target_worker") or None,
        "error": None,
        "result": None,
    }
    redis_set_json(status_key(job_id), status_doc)
    redis_set_json(payload_key(job_id), job)
    r.zadd(JOB_INDEX_KEY, {job_id: time.time()})


@app.get("/healthz")
def healthz() -> dict:
    try:
        r.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}") from exc
    return {"status": "ok"}


@app.post("/files", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)) -> FileUploadResponse:
    ensure_upload_dir()
    safe_name = f"{uuid.uuid4()}-{os.path.basename(file.filename or 'upload.bin')}"
    destination = os.path.join(UPLOAD_DIR, safe_name)

    bytes_written = 0
    with open(destination, "wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
            bytes_written += len(chunk)

    return FileUploadResponse(file_name=file.filename or safe_name, file_path=destination, bytes_written=bytes_written)


@app.post("/jobs", response_model=JobCreateResponse)
def create_job(request: JobCreateRequest) -> JobCreateResponse:
    job = normalize_job(request)
    store_initial_job_state(job)
    r.lpush(job["target_queue"], json.dumps(job))
    status_url, result_url = build_status_urls(job["job_id"])
    return JobCreateResponse(job_id=job["job_id"], status="queued", status_url=status_url, result_url=result_url)


@app.post("/jobs/file", response_model=JobCreateResponse)
async def create_file_job(
    operation: str = Form("word_count"),
    client_id: Optional[str] = Form(None),
    metadata_json: str = Form("{}"),
    file: UploadFile = File(...),
) -> JobCreateResponse:
    ensure_upload_dir()
    safe_name = f"{uuid.uuid4()}-{os.path.basename(file.filename or 'upload.bin')}"
    destination = os.path.join(UPLOAD_DIR, safe_name)

    with open(destination, "wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)

    try:
        metadata = json.loads(metadata_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="metadata_json must be valid JSON") from exc

    request = JobCreateRequest(
        type="file",
        params={
            "operation": operation,
            "file_name": file.filename or safe_name,
            "file_path": destination,
        },
        client_id=client_id,
        metadata=metadata,
    )
    job = normalize_job(request, uploaded_file_path=destination)
    store_initial_job_state(job)
    r.lpush(job["target_queue"], json.dumps(job))
    status_url, result_url = build_status_urls(job["job_id"])
    return JobCreateResponse(job_id=job["job_id"], status="queued", status_url=status_url, result_url=result_url)


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str) -> JobStatusResponse:
    status_doc = redis_get_json(status_key(job_id))
    if status_doc is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobStatusResponse(**status_doc)


@app.get("/jobs/{job_id}/result")
def get_job_result(job_id: str) -> dict:
    result_doc = redis_get_json(result_key(job_id))
    if result_doc is None:
        status_doc = redis_get_json(status_key(job_id))
        if status_doc is None:
            raise HTTPException(status_code=404, detail="job not found")
        if status_doc.get("status") != "completed":
            raise HTTPException(status_code=409, detail=f"job is {status_doc.get('status', 'unknown')}")
        raise HTTPException(status_code=404, detail="job result not found")
    return result_doc
