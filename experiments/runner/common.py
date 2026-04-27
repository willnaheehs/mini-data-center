#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import request, error

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENTS_ROOT = REPO_ROOT / "experiments"
RUNS_ROOT = Path(os.getenv("MINI_DC_RUNS_ROOT", str(EXPERIMENTS_ROOT / "runs"))).resolve()
DEFAULT_API_BASE = os.getenv("MINI_DC_API_BASE", "http://node1-control:30080")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_runs_root() -> Path:
    RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    return RUNS_ROOT


def run_dir(run_id: str) -> Path:
    return ensure_runs_root() / run_id


def manifest_path(run_id: str) -> Path:
    return run_dir(run_id) / "manifest.json"


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def api_request(method: str, path: str, payload: dict[str, Any] | None = None, api_base: str | None = None) -> dict[str, Any]:
    base = (api_base or DEFAULT_API_BASE).rstrip("/")
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
      data = json.dumps(payload).encode("utf-8")
      headers["Content-Type"] = "application/json"
    req = request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            detail = json.loads(body)
        except Exception:
            detail = {"error": body or str(exc)}
        raise RuntimeError(f"API {method} {path} failed: {detail}") from exc
