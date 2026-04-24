import json
import mimetypes
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Literal, Optional

import redis
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from common.storage import artifact_url, download_bytes, ensure_bucket_exists, upload_bytes

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT_NUM", "6379"))
DEFAULT_ROUTING_POLICY = os.getenv("ROUTING_POLICY", "shortest_queue").strip().lower()
ROUTING_POLICY_KEY = os.getenv("ROUTING_POLICY_KEY", "mini-dc-api:routing-policy")
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
SUPPORTED_ROUTING_POLICIES = ["shortest_queue", "round_robin", "power_of_two", "state_aware", "adaptive"]
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
    policy: Literal["shortest_queue", "round_robin", "power_of_two", "state_aware", "adaptive"]


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


def get_cluster_snapshot() -> dict:
    workers = []
    for worker in WORKER_QUEUES:
        queue_length = get_queue_length(worker["queue_name"])
        busy_value = get_busy_value(worker["busy_key"])
        workers.append(
            {
                "worker_name": worker["worker_name"],
                "queue_name": worker["queue_name"],
                "busy_key": worker["busy_key"],
                "queue_length": queue_length,
                "busy": bool(busy_value),
                "busy_value": busy_value,
                "estimated_load": queue_length + busy_value,
            }
        )
    return {
        "routing_policy": get_current_routing_policy(),
        "workers": workers,
        "updated_at": utc_now(),
    }


def choose_target() -> dict:
    routing_policy = get_current_routing_policy()
    if routing_policy == "round_robin":
        counter = r.incr("mini-dc-api:round-robin-index") - 1
        target = WORKER_QUEUES[counter % len(WORKER_QUEUES)]
        return {
            **target,
            "queue_length": get_queue_length(target["queue_name"]),
            "busy_value": get_busy_value(target["busy_key"]),
            "estimated_load": get_queue_length(target["queue_name"]) + get_busy_value(target["busy_key"]),
            "policy": routing_policy,
        }

    candidates = []
    for worker in WORKER_QUEUES:
        queue_length = get_queue_length(worker["queue_name"])
        busy_value = get_busy_value(worker["busy_key"]) if routing_policy in {"state_aware", "adaptive"} else get_busy_value(worker["busy_key"]) if routing_policy == "power_of_two" else 0
        estimated_load = queue_length + busy_value
        candidates.append(
            {
                **worker,
                "queue_length": queue_length,
                "busy_value": busy_value,
                "estimated_load": estimated_load,
            }
        )

    if routing_policy == "power_of_two":
        sampled = random.sample(candidates, k=min(2, len(candidates)))
        sampled.sort(key=lambda item: (item["estimated_load"], item["queue_length"], item["worker_name"]))
        return {**sampled[0], "policy": routing_policy}

    if routing_policy in {"shortest_queue", "state_aware", "adaptive"}:
        candidates.sort(key=lambda item: (item["estimated_load"], item["queue_length"], item["worker_name"]))
        return {**candidates[0], "policy": routing_policy}

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
def cluster_status() -> dict:
    return {
        **get_cluster_snapshot(),
        "recent_jobs": get_recent_jobs(),
        "supported_routing_policies": SUPPORTED_ROUTING_POLICIES,
    }


@app.get("/routing-policy")
def get_routing_policy() -> dict:
    return {
        "policy": get_current_routing_policy(),
        "supported": SUPPORTED_ROUTING_POLICIES,
    }


@app.post("/routing-policy")
def update_routing_policy(request: RoutingPolicyUpdateRequest) -> dict:
    policy = set_current_routing_policy(request.policy)
    return {
        "policy": policy,
        "supported": SUPPORTED_ROUTING_POLICIES,
        "updated_at": utc_now(),
    }


@app.post("/jobs", response_model=JobCreateResponse)
def create_job(request: JobCreateRequest) -> JobCreateResponse:
    job = normalize_job(request)
    store_initial_job_state(job)
    r.lpush(job["target_queue"], json.dumps(job))
    status_url, result_url = build_status_urls(job["job_id"])
    return JobCreateResponse(job_id=job["job_id"], status=job["status"], status_url=status_url, result_url=result_url)


@app.post("/jobs/file", response_model=JobCreateResponse)
async def create_file_job(
    client_id: Optional[str] = Form(None),
    metadata_json: str = Form("{}"),
    operation: str = Form("text_summary"),
    input_file: UploadFile = File(...),
) -> JobCreateResponse:
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
    client_id: Optional[str] = Form(None),
    metadata_json: str = Form("{}"),
    timeout_seconds: int = Form(30),
    script: UploadFile = File(...),
    input_files: list[UploadFile] | None = File(None),
) -> JobCreateResponse:
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
def get_job_status(job_id: str) -> JobStatusResponse:
    status_doc = redis_get_json(status_key(job_id))
    if status_doc is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobStatusResponse(**status_doc)


@app.delete("/jobs/{job_id}")
def delete_queued_job(job_id: str) -> dict:
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




EXPERIMENTS_ROOT = REPO_ROOT / "experiments" / "runs"
RUNNER_DIR = REPO_ROOT / "experiments" / "runner"


class ExperimentCreateRequest(BaseModel):
    policy: Literal["shortest_queue", "round_robin", "power_of_two", "state_aware", "adaptive"]
    workload_file: str
    run_id: str
    notes: str = ""


def run_experiment_script(script_name: str, *args: str) -> tuple[int, str, str]:
    import subprocess

    cmd = [sys.executable, str(RUNNER_DIR / script_name), *args]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
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


@app.get("/experiments")
def list_experiments() -> dict:
    EXPERIMENTS_ROOT.mkdir(parents=True, exist_ok=True)
    runs = []
    for manifest in sorted(EXPERIMENTS_ROOT.glob('*/manifest.json'), reverse=True):
        data = json.loads(manifest.read_text())
        runs.append({
            "run_id": data.get("run_id"),
            "policy": data.get("policy"),
            "workload_file": data.get("workload_file"),
            "run_status": data.get("run_status"),
            "timestamp_start": data.get("timestamp_start"),
            "timestamp_end": data.get("timestamp_end"),
        })
    return {"runs": runs}


@app.get("/experiments/{run_id}")
def get_experiment(run_id: str) -> dict:
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


@app.post("/experiments")
def create_experiment(request: ExperimentCreateRequest) -> dict:
    code, stdout, stderr = run_experiment_script(
        "create_run.py",
        "--policy", request.policy,
        "--workload-file", request.workload_file,
        "--run-id", request.run_id,
        "--notes", request.notes,
    )
    if code != 0:
        raise HTTPException(status_code=400, detail=stderr or stdout or "failed to create experiment")
    manifest = read_experiment_manifest(request.run_id)
    return {
        "run_id": request.run_id,
        "run_status": manifest.get("run_status"),
        "run_dir": stdout,
    }


@app.post("/experiments/{run_id}/start")
def start_experiment(run_id: str) -> dict:
    code, stdout, stderr = run_experiment_script("start_run.py", "--run-id", run_id)
    if code != 0:
        raise HTTPException(status_code=400, detail=stderr or stdout or "failed to start experiment")
    try:
        return json.loads(stdout)
    except Exception:
        return {"run_id": run_id, "output": stdout}


@app.get("/artifacts/{artifact_path:path}")
def get_artifact(artifact_path: str):
    try:
        data = download_bytes(artifact_path)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"artifact not found: {artifact_path}") from exc

    file_name = Path(artifact_path).name
    media_type = guess_content_type(file_name)
    headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
    return StreamingResponse(iter([data]), media_type=media_type, headers=headers)
