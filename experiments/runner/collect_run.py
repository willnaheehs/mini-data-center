#!/usr/bin/env python3
import argparse
import time
from pathlib import Path

from common import api_request, manifest_path, read_json, utc_now_iso, write_json

TERMINAL = {"completed", "failed", "deleted"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--api-base", default=None)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    args = parser.parse_args()

    manifest_file = manifest_path(args.run_id)
    manifest = read_json(manifest_file)
    run_root = manifest_file.parent
    results_dir = run_root / "results" / "job-results"
    results_dir.mkdir(parents=True, exist_ok=True)

    manifest["run_status"] = "collecting"
    write_json(manifest_file, manifest)

    remaining = set(manifest.get("submitted_job_ids", []))
    statuses = {}
    while remaining:
        finished = []
        for job_id in list(remaining):
            status = api_request("GET", f"/jobs/{job_id}", api_base=args.api_base)
            statuses[job_id] = status
            if status.get("status") in TERMINAL:
                result_payload = None
                try:
                    result_payload = api_request("GET", f"/jobs/{job_id}/result", api_base=args.api_base)
                except Exception:
                    result_payload = {"job_id": job_id, "status": status.get("status"), "result_missing": True}
                write_json(results_dir / f"{job_id}.json", {"status": status, "result": result_payload})
                finished.append(job_id)
        for job_id in finished:
            remaining.remove(job_id)
        if remaining:
            time.sleep(args.poll_seconds)

    manifest = read_json(manifest_file)
    manifest["timestamp_end"] = utc_now_iso()
    manifest["artifact_collection_status"] = "completed"
    manifest["run_status"] = "completed"
    write_json(manifest_file, manifest)
    print(f"Collected results for {len(statuses)} jobs into {results_dir}")


if __name__ == "__main__":
    main()
