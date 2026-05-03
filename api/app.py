import hmac
import ipaddress
import json
import mimetypes
import os
import random
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Literal, Optional

import redis
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from common.storage import artifact_url, download_bytes, ensure_bucket_exists, upload_bytes
from experiments.runner.presets import class_project_suite_definition

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT_NUM", "6379"))
DEFAULT_ROUTING_POLICY = os.getenv("ROUTING_POLICY", "shortest_queue").strip().lower()
ROUTING_POLICY_KEY = os.getenv("ROUTING_POLICY_KEY", "mini-dc-api:routing-policy")
WORKER_QUEUES = [
    {
        "worker_name": os.getenv("WORKER_NODE2_NAME", "node2-worker"),
        "queue_name": os.getenv("QUEUE_NODE2", "jobs-node2"),
        "busy_key": os.getenv("BUSY_KEY_NODE2", "busy-node2"),
        "temp_key": os.getenv("TEMP_KEY_NODE2", "cpu-temp-node2-worker"),
    },
    {
        "worker_name": os.getenv("WORKER_NODE3_NAME", "node3-worker"),
        "queue_name": os.getenv("QUEUE_NODE3", "jobs-node3"),
        "busy_key": os.getenv("BUSY_KEY_NODE3", "busy-node3"),
        "temp_key": os.getenv("TEMP_KEY_NODE3", "cpu-temp-node3-worker"),
    },
]
SUPPORTED_ROUTING_POLICIES = ["round_robin", "random", "shortest_queue", "adaptive", "power_of_two", "state_aware"]
JOB_STATUS_PREFIX = os.getenv("JOB_STATUS_PREFIX", "job-status:")
JOB_RESULT_PREFIX = os.getenv("JOB_RESULT_PREFIX", "job-result:")
JOB_PAYLOAD_PREFIX = os.getenv("JOB_PAYLOAD_PREFIX", "job-payload:")
JOB_INDEX_KEY = os.getenv("JOB_INDEX_KEY", "jobs-api:index")
JOB_TTL_SEC = int(os.getenv("JOB_TTL_SEC", str(7 * 24 * 60 * 60)))
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/data/uploads")
SCRIPT_MAX_BYTES = int(os.getenv("SCRIPT_MAX_BYTES", str(256 * 1024)))
RESULT_BASE_URL = os.getenv("RESULT_BASE_URL", "")
MAX_INPUT_FILES = int(os.getenv("MAX_INPUT_FILES", "8"))
MAX_INPUT_FILE_BYTES = int(os.getenv("MAX_INPUT_FILE_BYTES", str(5 * 1024 * 1024)))
FILE_PROCESS_MAX_BYTES = int(os.getenv("FILE_PROCESS_MAX_BYTES", str(5 * 1024 * 1024)))
RECENT_JOBS_LIMIT = int(os.getenv("RECENT_JOBS_LIMIT", "20"))
MINI_DC_RUNS_ROOT = Path(os.getenv("MINI_DC_RUNS_ROOT", "/data/experiments/runs"))
API_AUTH_TOKEN = os.getenv("MINI_DC_API_AUTH_TOKEN", "")
API_AUTH_HEADER = "x-api-key"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
app = FastAPI(title="mini-data-center api", version="0.3.0")


class ComputeParams(BaseModel):
    task: Literal["prime_count", "monte_carlo_pi", "sort_numbers", "matrix_multiply"]
    params: Dict[str, Any] = Field(default_factory=dict)


class MlParams(BaseModel):
    operation: Literal["linear_binary_classify"] = "linear_binary_classify"
    features: list[list[float]] = Field(..., min_length=1)
    weights: list[float] = Field(..., min_length=1)
    bias: float = 0.0
    threshold: float = 0.5


class FileProcessParams(BaseModel):
    operation: Literal["text_summary"] = "text_summary"
    input_artifact: Dict[str, Any]


class JobCreateRequest(BaseModel):
    type: Literal["compute", "ml", "file_process"]
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
    dispatched_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    worker_name: Optional[str] = None
    target_worker: Optional[str] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class RoutingPolicyUpdateRequest(BaseModel):
    policy: Literal["round_robin", "random", "shortest_queue", "adaptive", "power_of_two", "state_aware"]


class FileUploadResponse(BaseModel):
    file_name: str
    file_path: str
    bytes_written: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def auth_enabled() -> bool:
    return bool(API_AUTH_TOKEN)


def client_is_trusted(request: Request) -> bool:
    host = (request.client.host if request.client else "") or ""
    if not host:
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return host in {"localhost"}
    return ip.is_loopback or ip.is_private


def require_api_auth(request: Request) -> None:
    if not auth_enabled() or client_is_trusted(request):
        return
    provided = request.headers.get(API_AUTH_HEADER, "") or request.query_params.get("api_key", "")
    if not provided or not hmac.compare_digest(provided, API_AUTH_TOKEN):
        raise HTTPException(status_code=401, detail=f"missing or invalid {API_AUTH_HEADER} header")


def status_key(job_id: str) -> str:
    return f"{JOB_STATUS_PREFIX}{job_id}"


def result_key(job_id: str) -> str:
    return f"{JOB_RESULT_PREFIX}{job_id}"


def payload_key(job_id: str) -> str:
    return f"{JOB_PAYLOAD_PREFIX}{job_id}"


def ensure_upload_dir() -> None:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    ensure_bucket_exists()


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


def get_current_routing_policy() -> str:
    stored = r.get(ROUTING_POLICY_KEY)
    if stored and stored in SUPPORTED_ROUTING_POLICIES:
        return stored
    return DEFAULT_ROUTING_POLICY


def set_current_routing_policy(policy: str) -> str:
    if policy not in SUPPORTED_ROUTING_POLICIES:
        raise HTTPException(status_code=400, detail=f"unsupported routing policy: {policy}")
    r.set(ROUTING_POLICY_KEY, policy)
    return policy


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


def get_temperature_value(temp_key: str) -> float | None:
    if not temp_key:
        return None
    raw = r.get(temp_key)
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def temperature_penalty_seconds(temp_celsius: float | None) -> float:
    if temp_celsius is None:
        return 0.0
    mild = max(0.0, temp_celsius - 60.0) * 0.12
    moderate = max(0.0, temp_celsius - 70.0) * 0.25
    severe = max(0.0, temp_celsius - 80.0) * 0.75
    return mild + moderate + severe


def temperature_state(temp_celsius: float | None) -> str:
    if temp_celsius is None:
        return "unknown"
    if temp_celsius >= 80:
        return "hot"
    if temp_celsius >= 70:
        return "warm"
    return "cool"


def get_cluster_snapshot() -> dict:
    runtime_stats = get_recent_worker_service_stats()
    workers = []
    for worker in WORKER_QUEUES:
        queue_length = get_queue_length(worker["queue_name"])
        busy_value = get_busy_value(worker["busy_key"])
        cpu_temperature_celsius = get_temperature_value(worker.get("temp_key", ""))
        avg_service_time = runtime_stats.get(worker["worker_name"], {}).get("avg_service_time", 1.0)
        estimated_load = queue_length + busy_value
        estimated_completion_seconds = (queue_length + busy_value + 1) * avg_service_time
        temp_penalty = temperature_penalty_seconds(cpu_temperature_celsius)
        workers.append(
            {
                "worker_name": worker["worker_name"],
                "queue_name": worker["queue_name"],
                "busy_key": worker["busy_key"],
                "temp_key": worker.get("temp_key", ""),
                "queue_length": queue_length,
                "busy": bool(busy_value),
                "busy_value": busy_value,
                "estimated_load": estimated_load,
                "avg_service_time": avg_service_time,
                "estimated_completion_seconds": estimated_completion_seconds,
                "cpu_temperature_celsius": cpu_temperature_celsius,
                "temperature_penalty_seconds": temp_penalty,
                "temperature_state": temperature_state(cpu_temperature_celsius),
                "adaptive_score": estimated_completion_seconds + temp_penalty,
            }
        )
    return {
        "routing_policy": get_current_routing_policy(),
        "workers": workers,
        "updated_at": utc_now(),
    }


def get_recent_worker_service_stats(limit: int = 40) -> dict[str, dict[str, float]]:
    job_ids = r.zrevrange(JOB_INDEX_KEY, 0, max(0, limit - 1))
    grouped: dict[str, list[float]] = {}
    for job_id in job_ids:
        result_doc = redis_get_json(result_key(job_id)) or {}
        worker_name = result_doc.get("worker_name")
        metrics = result_doc.get("metrics") or {}
        service_time = metrics.get("service_time")
        if not worker_name or service_time in (None, ""):
            continue
        try:
            value = float(service_time)
        except (TypeError, ValueError):
            continue
        if value < 0:
            continue
        grouped.setdefault(worker_name, []).append(value)

    stats: dict[str, dict[str, float]] = {}
    for worker_name, values in grouped.items():
        if values:
            stats[worker_name] = {
                "avg_service_time": sum(values) / len(values),
                "sample_count": float(len(values)),
            }
    return stats


def build_worker_candidate(worker: dict, runtime_stats: dict[str, dict[str, float]], default_service_time: float) -> dict:
    queue_length = get_queue_length(worker["queue_name"])
    busy_value = get_busy_value(worker["busy_key"])
    cpu_temperature_celsius = get_temperature_value(worker.get("temp_key", ""))
    avg_service_time = runtime_stats.get(worker["worker_name"], {}).get("avg_service_time", default_service_time)
    estimated_load = queue_length + busy_value
    estimated_completion_seconds = (queue_length + busy_value + 1) * avg_service_time
    temp_penalty = temperature_penalty_seconds(cpu_temperature_celsius)
    return {
        **worker,
        "queue_length": queue_length,
        "busy_value": busy_value,
        "estimated_load": estimated_load,
        "avg_service_time": avg_service_time,
        "estimated_completion_seconds": estimated_completion_seconds,
        "cpu_temperature_celsius": cpu_temperature_celsius,
        "temperature_penalty_seconds": temp_penalty,
        "temperature_state": temperature_state(cpu_temperature_celsius),
        "adaptive_score": estimated_completion_seconds + temp_penalty,
    }


def choose_target() -> dict:
    """Choose a worker using the active routing policy.

    adaptive = estimated completion time + CPU temperature penalty
    state_aware = estimated completion time without thermal penalty
    """
    routing_policy = get_current_routing_policy()
    runtime_stats = get_recent_worker_service_stats()
    default_service_time = 1.0

    if routing_policy == "round_robin":
        counter = r.incr("mini-dc-api:round-robin-index") - 1
        target = build_worker_candidate(WORKER_QUEUES[counter % len(WORKER_QUEUES)], runtime_stats, default_service_time)
        return {**target, "policy": routing_policy}

    candidates = [build_worker_candidate(worker, runtime_stats, default_service_time) for worker in WORKER_QUEUES]

    if routing_policy == "random":
        target = random.choice(candidates)
        return {**target, "policy": routing_policy}

    if routing_policy == "power_of_two":
        sampled = random.sample(candidates, k=min(2, len(candidates)))
        sampled.sort(key=lambda item: (item["estimated_load"], item["queue_length"], item["worker_name"]))
        return {**sampled[0], "policy": routing_policy}

    if routing_policy == "shortest_queue":
        candidates.sort(key=lambda item: (item["queue_length"], item["busy_value"], item["worker_name"]))
        return {**candidates[0], "policy": routing_policy}

    if routing_policy == "state_aware":
        candidates.sort(key=lambda item: (item["estimated_completion_seconds"], item["queue_length"], item["worker_name"]))
        selected = {**candidates[0], "policy": routing_policy}
        selected["policy_detail"] = "queue_length_busy_and_recent_runtime"
        return selected

    if routing_policy == "adaptive":
        candidates.sort(key=lambda item: (item["adaptive_score"], item["estimated_completion_seconds"], item["queue_length"], item["worker_name"]))
        selected = {**candidates[0], "policy": routing_policy}
        selected["policy_detail"] = "estimated_completion_plus_temperature_penalty"
        return selected

    raise HTTPException(status_code=500, detail=f"unsupported ROUTING_POLICY: {routing_policy}")


def normalize_job(request: JobCreateRequest) -> dict:
    job_id = str(uuid.uuid4())
    submitted_at = utc_now()

    if request.type == "compute":
        parsed = ComputeParams.model_validate(request.params)
        worker_job = {
            "job_type": "compute",
            "params": parsed.model_dump(),
        }
    elif request.type == "ml":
        parsed = MlParams.model_validate(request.params)
        worker_job = {
            "job_type": "ml",
            "params": parsed.model_dump(),
        }
    elif request.type == "file_process":
        parsed = FileProcessParams.model_validate(request.params)
        worker_job = {
            "job_type": "file_process",
            "params": parsed.model_dump(),
        }
    else:
        raise HTTPException(status_code=400, detail=f"unsupported job type: {request.type}")

    target = choose_target()
    policy = target.get("policy", get_current_routing_policy())
    dispatched_at = utc_now()
    return {
        "job_id": job_id,
        "job_type": worker_job["job_type"],
        "api_job_type": request.type,
        "submit_time": submitted_at,
        "submitted_at": submitted_at,
        "dispatched_at": dispatched_at,
        "params": worker_job["params"],
        "policy": policy,
        "target_worker": target["worker_name"],
        "target_queue": target["queue_name"],
        "client_id": request.client_id,
        "metadata": request.metadata,
        "artifacts": {"inputs": [], "outputs": []},
        "status": "dispatched",
        "dispatcher_decision": {
            "policy": policy,
            "selection_reason": "api submission",
            "load_snapshot": {
                item["worker_name"]: {
                    "queue_name": item["queue_name"],
                    "queue_length": item["queue_length"],
                    "busy_value": item["busy_value"],
                    "estimated_load": item["estimated_load"],
                }
                for item in get_cluster_snapshot()["workers"]
            },
        },
    }


def store_initial_job_state(job: dict) -> None:
    job_id = job["job_id"]
    status_doc = {
        "job_id": job_id,
        "status": "dispatched",
        "job_type": job.get("api_job_type", job.get("job_type")),
        "submitted_at": job["submitted_at"],
        "dispatched_at": job.get("dispatched_at"),
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


def build_object_key(job_id: str, category: str, file_name: str) -> str:
    safe_name = os.path.basename(file_name) or f"{category}.bin"
    return f"jobs/{job_id}/{category}/{uuid.uuid4()}-{safe_name}"


def guess_content_type(file_name: str) -> str:
    guessed, _ = mimetypes.guess_type(file_name)
    return guessed or "application/octet-stream"


async def persist_upload(job_id: str, upload: UploadFile, category: str, max_bytes: int) -> dict:
    data = await upload.read()
    if len(data) > max_bytes:
        raise HTTPException(status_code=400, detail=f"{category} exceeds {max_bytes} bytes limit")

    object_key = build_object_key(job_id, category, upload.filename or f"{category}.bin")
    artifact = upload_bytes(object_key, data, content_type=guess_content_type(upload.filename or object_key))
    artifact.update(
        {
            "name": upload.filename or Path(object_key).name,
            "category": category,
            "url": artifact_url(object_key),
        }
    )
    return artifact


def get_recent_jobs(limit: int = RECENT_JOBS_LIMIT) -> list[dict]:
    job_ids = r.zrevrange(JOB_INDEX_KEY, 0, max(0, limit - 1))
    jobs = []
    for job_id in job_ids:
        status_doc = redis_get_json(status_key(job_id))
        payload_doc = redis_get_json(payload_key(job_id))
        if status_doc is None:
            continue
        jobs.append(
            {
                "job_id": job_id,
                "status": status_doc.get("status"),
                "job_type": status_doc.get("job_type"),
                "submitted_at": status_doc.get("submitted_at"),
                "started_at": status_doc.get("started_at"),
                "finished_at": status_doc.get("finished_at"),
                "worker_name": status_doc.get("worker_name"),
                "target_worker": status_doc.get("target_worker"),
                "policy": (payload_doc or {}).get("policy"),
                "target_queue": (payload_doc or {}).get("target_queue"),
                "deletable": status_doc.get("status") == "dispatched",
                "result_summary": (redis_get_json(result_key(job_id)) or {}).get("result_summary"),
            }
        )
    return jobs


@app.get("/")
def index() -> FileResponse:
    return FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))


@app.get("/experiments/ui")
def experiments_ui() -> FileResponse:
    return FileResponse(os.path.join(os.path.dirname(__file__), "experiments.html"))


@app.get("/healthz")
def healthz() -> dict:
    try:
        r.ping()
        ensure_bucket_exists()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"dependency unavailable: {exc}") from exc
    return {
        "status": "ok",
        "routing_policy": get_current_routing_policy(),
    }


@app.get("/cluster/status")
def cluster_status(request: Request) -> dict:
    require_api_auth(request)
    return {
        **get_cluster_snapshot(),
        "recent_jobs": get_recent_jobs(),
        "supported_routing_policies": SUPPORTED_ROUTING_POLICIES,
    }


@app.get("/routing-policy")
def get_routing_policy(request: Request) -> dict:
    require_api_auth(request)
    return {
        "policy": get_current_routing_policy(),
        "supported": SUPPORTED_ROUTING_POLICIES,
    }


@app.post("/routing-policy")
def update_routing_policy(request_data: RoutingPolicyUpdateRequest, request: Request) -> dict:
    require_api_auth(request)
    policy = set_current_routing_policy(request_data.policy)
    return {
        "policy": policy,
        "supported": SUPPORTED_ROUTING_POLICIES,
        "updated_at": utc_now(),
    }


@app.post("/jobs", response_model=JobCreateResponse)
def create_job(request_data: JobCreateRequest, request: Request) -> JobCreateResponse:
    require_api_auth(request)
    job = normalize_job(request_data)
    store_initial_job_state(job)
    r.lpush(job["target_queue"], json.dumps(job))
    status_url, result_url = build_status_urls(job["job_id"])
    return JobCreateResponse(job_id=job["job_id"], status=job["status"], status_url=status_url, result_url=result_url)


@app.post("/jobs/file", response_model=JobCreateResponse)
async def create_file_job(
    request: Request,
    client_id: Optional[str] = Form(None),
    metadata_json: str = Form("{}"),
    operation: str = Form("text_summary"),
    input_file: UploadFile = File(...),
) -> JobCreateResponse:
    require_api_auth(request)
    ensure_upload_dir()

    try:
        metadata = json.loads(metadata_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="metadata_json must be valid JSON") from exc

    if operation != "text_summary":
        raise HTTPException(status_code=400, detail=f"unsupported file operation: {operation}")

    job_id = str(uuid.uuid4())
    submitted_at = utc_now()
    input_artifact = await persist_upload(job_id, input_file, "input", FILE_PROCESS_MAX_BYTES)

    target = choose_target()
    policy = target.get("policy", get_current_routing_policy())
    cluster_snapshot = get_cluster_snapshot()
    job = {
        "job_id": job_id,
        "job_type": "file_process",
        "api_job_type": "file_process",
        "submit_time": submitted_at,
        "submitted_at": submitted_at,
        "dispatched_at": utc_now(),
        "params": {
            "operation": operation,
            "input_artifact": input_artifact,
        },
        "policy": policy,
        "target_worker": target["worker_name"],
        "target_queue": target["queue_name"],
        "client_id": client_id,
        "metadata": metadata,
        "artifacts": {
            "inputs": [input_artifact],
            "outputs": [],
        },
        "status": "dispatched",
        "dispatcher_decision": {
            "policy": policy,
            "selection_reason": "api submission",
            "load_snapshot": {
                item["worker_name"]: {
                    "queue_name": item["queue_name"],
                    "queue_length": item["queue_length"],
                    "busy_value": item["busy_value"],
                    "estimated_load": item["estimated_load"],
                }
                for item in cluster_snapshot["workers"]
            },
        },
    }
    store_initial_job_state(job)
    r.lpush(job["target_queue"], json.dumps(job))
    status_url, result_url = build_status_urls(job_id)
    return JobCreateResponse(job_id=job_id, status="dispatched", status_url=status_url, result_url=result_url)


@app.post("/jobs/python", response_model=JobCreateResponse)
async def create_python_job(
    request: Request,
    client_id: Optional[str] = Form(None),
    metadata_json: str = Form("{}"),
    timeout_seconds: int = Form(30),
    script: UploadFile = File(...),
    input_files: list[UploadFile] | None = File(None),
) -> JobCreateResponse:
    require_api_auth(request)
    ensure_upload_dir()

    if not (script.filename or "").endswith(".py"):
        raise HTTPException(status_code=400, detail="only .py files are supported")

    try:
        metadata = json.loads(metadata_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="metadata_json must be valid JSON") from exc

    job_id = str(uuid.uuid4())
    submitted_at = utc_now()
    script_artifact = await persist_upload(job_id, script, "script", SCRIPT_MAX_BYTES)

    uploaded_inputs = []
    for upload in input_files or []:
        if len(uploaded_inputs) >= MAX_INPUT_FILES:
            raise HTTPException(status_code=400, detail=f"too many input files, max {MAX_INPUT_FILES}")
        uploaded_inputs.append(await persist_upload(job_id, upload, "input", MAX_INPUT_FILE_BYTES))

    target = choose_target()
    policy = target.get("policy", get_current_routing_policy())
    cluster_snapshot = get_cluster_snapshot()
    job = {
        "job_id": job_id,
        "job_type": "python_script",
        "api_job_type": "python_script",
        "submit_time": submitted_at,
        "submitted_at": submitted_at,
        "dispatched_at": utc_now(),
        "params": {
            "script_name": script.filename or script_artifact["name"],
            "script_artifact": script_artifact,
            "input_artifacts": uploaded_inputs,
            "timeout_seconds": max(1, min(timeout_seconds, 300)),
        },
        "policy": policy,
        "target_worker": target["worker_name"],
        "target_queue": target["queue_name"],
        "client_id": client_id,
        "metadata": metadata,
        "artifacts": {
            "inputs": [script_artifact, *uploaded_inputs],
            "outputs": [],
        },
        "status": "dispatched",
        "dispatcher_decision": {
            "policy": policy,
            "selection_reason": "api submission",
            "load_snapshot": {
                item["worker_name"]: {
                    "queue_name": item["queue_name"],
                    "queue_length": item["queue_length"],
                    "busy_value": item["busy_value"],
                    "estimated_load": item["estimated_load"],
                }
                for item in cluster_snapshot["workers"]
            },
        },
    }
    store_initial_job_state(job)
    r.lpush(job["target_queue"], json.dumps(job))
    status_url, result_url = build_status_urls(job_id)
    return JobCreateResponse(job_id=job_id, status="dispatched", status_url=status_url, result_url=result_url)


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str, request: Request) -> JobStatusResponse:
    require_api_auth(request)
    status_doc = redis_get_json(status_key(job_id))
    if status_doc is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobStatusResponse(**status_doc)


@app.delete("/jobs/{job_id}")
def delete_queued_job(job_id: str, request: Request) -> dict:
    require_api_auth(request)
    status_doc = redis_get_json(status_key(job_id))
    payload_doc = redis_get_json(payload_key(job_id))
    if status_doc is None or payload_doc is None:
        raise HTTPException(status_code=404, detail="job not found")

    if status_doc.get("status") != "dispatched":
        raise HTTPException(status_code=409, detail=f"only queued jobs can be deleted, job is {status_doc.get('status', 'unknown')}")

    target_queue = payload_doc.get("target_queue")
    if not target_queue:
        raise HTTPException(status_code=500, detail="job target queue missing")

    removed = r.lrem(target_queue, 1, json.dumps(payload_doc))
    if removed == 0:
        raise HTTPException(status_code=409, detail="job is no longer queued")

    status_doc["status"] = "deleted"
    status_doc["finished_at"] = utc_now()
    status_doc["error"] = "deleted from queue by user"
    redis_set_json(status_key(job_id), status_doc)

    result_payload = {
        "job_id": job_id,
        "job_type": status_doc.get("job_type"),
        "worker_job_type": payload_doc.get("job_type"),
        "status": "deleted",
        "submitted_at": status_doc.get("submitted_at"),
        "dispatched_at": status_doc.get("dispatched_at"),
        "started_at": None,
        "finished_at": status_doc.get("finished_at"),
        "worker_name": None,
        "target_worker": status_doc.get("target_worker"),
        "result_summary": "deleted from queue by user",
        "result": {},
        "metrics": {},
        "metadata": payload_doc.get("metadata", {}),
        "artifacts": payload_doc.get("artifacts", {}),
    }
    redis_set_json(result_key(job_id), result_payload)
    return {
        "job_id": job_id,
        "status": "deleted",
        "removed_from_queue": target_queue,
    }


@app.get("/jobs/{job_id}/result")
def get_job_result(job_id: str, request: Request) -> dict:
    require_api_auth(request)
    result_doc = redis_get_json(result_key(job_id))
    if result_doc is None:
        status_doc = redis_get_json(status_key(job_id))
        if status_doc is None:
            raise HTTPException(status_code=404, detail="job not found")
        if status_doc.get("status") != "completed":
            raise HTTPException(status_code=409, detail=f"job is {status_doc.get('status', 'unknown')}")
        raise HTTPException(status_code=404, detail="job result not found")
    return result_doc




EXPERIMENTS_ROOT = MINI_DC_RUNS_ROOT
RUNNER_DIR = REPO_ROOT / "experiments" / "runner"
SUITES_ROOT = MINI_DC_RUNS_ROOT / "_suites"


def ensure_experiments_runtime_available() -> None:
    missing = []
    expected_paths = [
        RUNNER_DIR / "create_run.py",
        RUNNER_DIR / "start_run.py",
        RUNNER_DIR / "collect_run.py",
        RUNNER_DIR / "summarize_run.py",
        REPO_ROOT / "experiments" / "workloads",
    ]
    for path in expected_paths:
        if not path.exists():
            missing.append(str(path))
    if missing:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "experiment runtime files are not available inside the API container",
                "missing_paths": missing,
                "hint": "Rebuild and redeploy the API image so /app/experiments is included.",
            },
        )


class ExperimentCreateRequest(BaseModel):
    policy: Literal["round_robin", "random", "shortest_queue", "adaptive", "power_of_two", "state_aware"]
    workload_file: str
    run_id: str
    notes: str = ""
    job_count: int = 0
    submit_interval_ms: int = 0


class ExperimentSuiteCreateRequest(BaseModel):
    suite_id: Optional[str] = None
    submit_interval_ms: int = 250
    job_count: int = 0


def validate_run_id(run_id: str) -> str:
    cleaned = run_id.strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="run_id is required")
    if "/" in cleaned or "\\" in cleaned:
        raise HTTPException(status_code=400, detail="run_id cannot contain slashes; use dashes or underscores instead")
    if cleaned in {".", ".."} or ".." in cleaned:
        raise HTTPException(status_code=400, detail="run_id cannot contain path traversal segments")
    return cleaned


def run_experiment_script(script_name: str, *args: str) -> tuple[int, str, str]:
    import subprocess

    ensure_experiments_runtime_available()
    script_path = RUNNER_DIR / script_name
    env = os.environ.copy()
    env["MINI_DC_RUNS_ROOT"] = str(MINI_DC_RUNS_ROOT)
    cmd = [sys.executable, str(script_path), *args]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def read_experiment_manifest(run_id: str) -> dict:
    manifest = EXPERIMENTS_ROOT / run_id / "manifest.json"
    if not manifest.exists():
        raise HTTPException(status_code=404, detail=f"experiment run not found: {run_id}")
    return json.loads(manifest.read_text())


def list_experiment_artifacts(run_id: str) -> dict:
    run_dir = EXPERIMENTS_ROOT / run_id
    categories = ["config", "metrics", "logs", "results", "screenshots"]
    out = {}
    for category in categories:
        base = run_dir / category
        files = []
        if base.exists():
            files = [str(path.relative_to(run_dir)) for path in sorted(base.rglob('*')) if path.is_file()]
        out[f"{category}_files"] = files
    return out


def suite_manifest_path(suite_id: str) -> Path:
    return SUITES_ROOT / suite_id / "suite_manifest.json"


def suite_artifacts_payload(suite_id: str) -> dict:
    suite_root = SUITES_ROOT / suite_id
    artifacts = {}
    for file_name in ["suite_manifest.json", "suite_summary.csv", "suite_job_results.csv"]:
        file_path = suite_root / file_name
        if file_path.exists():
            artifacts[file_name] = {
                "file_name": file_name,
                "url": f"/experiment-suites/{suite_id}/artifacts/{file_name}",
            }
    return artifacts


def read_suite_manifest(suite_id: str) -> dict:
    manifest = suite_manifest_path(suite_id)
    if not manifest.exists():
        raise HTTPException(status_code=404, detail=f"experiment suite not found: {suite_id}")
    data = json.loads(manifest.read_text())
    data["artifacts"] = suite_artifacts_payload(suite_id)
    return data


def list_suite_manifests() -> list[dict]:
    SUITES_ROOT.mkdir(parents=True, exist_ok=True)
    suites = []
    for manifest in sorted(SUITES_ROOT.glob("*/suite_manifest.json"), reverse=True):
        suite_id = manifest.parent.name
        data = json.loads(manifest.read_text())
        data["artifacts"] = suite_artifacts_payload(suite_id)
        suites.append(data)
    return suites


def launch_suite_runner(suite_id: str, submit_interval_ms: int, job_count: int) -> None:
    ensure_experiments_runtime_available()
    env = os.environ.copy()
    env["MINI_DC_RUNS_ROOT"] = str(MINI_DC_RUNS_ROOT)
    cmd = [
        sys.executable,
        str(RUNNER_DIR / "run_suite.py"),
        "--suite-id", suite_id,
        "--submit-interval-ms", str(submit_interval_ms),
        "--job-count", str(job_count),
    ]
    subprocess.Popen(cmd, cwd=str(REPO_ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@app.get("/experiments")
def list_experiments(request: Request) -> dict:
    require_api_auth(request)
    EXPERIMENTS_ROOT.mkdir(parents=True, exist_ok=True)
    runs = []
    for manifest in EXPERIMENTS_ROOT.glob('*/manifest.json'):
        data = json.loads(manifest.read_text())
        runs.append({
            "run_id": data.get("run_id"),
            "policy": data.get("policy"),
            "workload_file": data.get("workload_file"),
            "run_status": data.get("run_status"),
            "timestamp_start": data.get("timestamp_start"),
            "timestamp_end": data.get("timestamp_end"),
            "job_count": data.get("job_count"),
            "submit_interval_ms": data.get("submit_interval_ms"),
        })
    runs.sort(key=lambda run: (
        run.get("timestamp_start") or run.get("timestamp_end") or "",
        run.get("run_id") or "",
    ), reverse=True)
    return {"runs": runs}


@app.get("/experiment-presets")
def get_experiment_presets(request: Request) -> dict:
    require_api_auth(request)
    return {"presets": [class_project_suite_definition()]}


@app.get("/experiment-suites")
def get_experiment_suites(request: Request) -> dict:
    require_api_auth(request)
    return {"suites": list_suite_manifests()}


@app.get("/experiment-suites/{suite_id}")
def get_experiment_suite(suite_id: str, request: Request) -> dict:
    require_api_auth(request)
    return read_suite_manifest(validate_run_id(suite_id))


@app.get("/experiment-suites/{suite_id}/artifacts/{artifact_name}")
def download_experiment_suite_artifact(suite_id: str, artifact_name: str, request: Request):
    require_api_auth(request)
    suite_id = validate_run_id(suite_id)
    allowed = {"suite_manifest.json", "suite_summary.csv", "suite_job_results.csv"}
    if artifact_name not in allowed:
        raise HTTPException(status_code=404, detail="artifact not found")
    file_path = (SUITES_ROOT / suite_id / artifact_name).resolve()
    suite_root = (SUITES_ROOT / suite_id).resolve()
    try:
        file_path.relative_to(suite_root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid artifact path") from exc
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="artifact not found")
    return FileResponse(str(file_path), filename=file_path.name)


@app.post("/experiment-suites/class-project-routing-suite")
def create_class_project_experiment_suite(request_data: ExperimentSuiteCreateRequest, request: Request) -> dict:
    require_api_auth(request)
    if request_data.submit_interval_ms < 0:
        raise HTTPException(status_code=400, detail="submit_interval_ms must be 0 or greater")
    suite_id = request_data.suite_id or f"suite-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    suite_id = validate_run_id(suite_id)
    if suite_manifest_path(suite_id).exists():
        raise HTTPException(status_code=409, detail=f"suite already exists: {suite_id}")
    launch_suite_runner(suite_id, request_data.submit_interval_ms, request_data.job_count)
    return {
        "suite_id": suite_id,
        "status": "started",
        "preset_id": "class-project-routing-suite",
        "submit_interval_ms": request_data.submit_interval_ms,
        "job_count": request_data.job_count,
    }


@app.get("/experiments/{run_id}")
def get_experiment(run_id: str, request: Request) -> dict:
    require_api_auth(request)
    manifest = read_experiment_manifest(run_id)
    run_dir = EXPERIMENTS_ROOT / run_id
    summary_path = run_dir / "metrics" / "summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else None
    return {
        "run_id": run_id,
        "manifest": manifest,
        "summary": summary,
        "artifacts": list_experiment_artifacts(run_id),
    }


@app.get("/experiments/{run_id}/files/{file_path:path}")
def get_experiment_file(run_id: str, file_path: str, request: Request):
    require_api_auth(request)
    run_dir = (EXPERIMENTS_ROOT / run_id).resolve()
    file_full_path = (run_dir / file_path).resolve()
    try:
        file_full_path.relative_to(run_dir)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid experiment file path") from exc
    if not file_full_path.exists() or not file_full_path.is_file():
        raise HTTPException(status_code=404, detail="experiment file not found")
    return FileResponse(str(file_full_path), filename=file_full_path.name)


@app.post("/experiments")
def create_experiment(request_data: ExperimentCreateRequest, request: Request) -> dict:
    require_api_auth(request)
    run_id = validate_run_id(request_data.run_id)
    code, stdout, stderr = run_experiment_script(
        "create_run.py",
        "--policy", request_data.policy,
        "--workload-file", request_data.workload_file,
        "--run-id", run_id,
        "--notes", request_data.notes,
        "--job-count", str(request_data.job_count),
        "--submit-interval-ms", str(request_data.submit_interval_ms),
    )
    if code != 0:
        raise HTTPException(status_code=400, detail=stderr or stdout or "failed to create experiment")
    manifest = read_experiment_manifest(run_id)
    return {
        "run_id": run_id,
        "run_status": manifest.get("run_status"),
        "run_dir": stdout,
    }


@app.post("/experiments/{run_id}/start")
def start_experiment(run_id: str, request: Request) -> dict:
    require_api_auth(request)
    run_id = validate_run_id(run_id)
    code, stdout, stderr = run_experiment_script("start_run.py", "--run-id", run_id)
    if code != 0:
        raise HTTPException(status_code=400, detail=stderr or stdout or "failed to start experiment")

    collect_code, collect_stdout, collect_stderr = run_experiment_script("collect_run.py", "--run-id", run_id)
    if collect_code != 0:
        raise HTTPException(status_code=400, detail=collect_stderr or collect_stdout or "failed to collect experiment results")

    summarize_code, summarize_stdout, summarize_stderr = run_experiment_script("summarize_run.py", "--run-id", run_id)
    if summarize_code != 0:
        raise HTTPException(status_code=400, detail=summarize_stderr or summarize_stdout or "failed to summarize experiment")

    manifest = read_experiment_manifest(run_id)
    summary_path = EXPERIMENTS_ROOT / run_id / "metrics" / "summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else None
    start_payload = None
    try:
        start_payload = json.loads(stdout)
    except Exception:
        start_payload = {"output": stdout}
    return {
        "run_id": run_id,
        "start": start_payload,
        "collect": collect_stdout,
        "summary": summary,
        "run_status": manifest.get("run_status"),
    }


@app.delete("/experiments/{run_id}")
def delete_experiment(run_id: str, request: Request) -> dict:
    require_api_auth(request)
    run_id = validate_run_id(run_id)
    run_path = (EXPERIMENTS_ROOT / run_id).resolve()
    try:
        run_path.relative_to(EXPERIMENTS_ROOT.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid experiment path") from exc

    manifest_path = run_path / "manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail=f"experiment run not found: {run_id}")

    trash_root = EXPERIMENTS_ROOT / ".trash"
    trash_root.mkdir(parents=True, exist_ok=True)
    archived_name = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{run_id}"
    archived_path = trash_root / archived_name
    shutil.move(str(run_path), str(archived_path))
    return {
        "run_id": run_id,
        "status": "archived",
        "archived_to": str(archived_path),
    }


@app.get("/artifacts/{artifact_path:path}")
def get_artifact(artifact_path: str, request: Request):
    require_api_auth(request)
    try:
        data = download_bytes(artifact_path)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"artifact not found: {artifact_path}") from exc

    file_name = Path(artifact_path).name
    media_type = guess_content_type(file_name)
    headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
    return StreamingResponse(iter([data]), media_type=media_type, headers=headers)
