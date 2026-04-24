#!/usr/bin/env python3
import argparse
import shutil
from pathlib import Path

from common import REPO_ROOT, run_dir, write_json


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", required=True)
    parser.add_argument("--workload-file", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--notes", default="")
    parser.add_argument("--job-count", type=int, default=0)
    parser.add_argument("--submission-pattern", default="api-driven")
    args = parser.parse_args()

    target = run_dir(args.run_id)
    if target.exists():
        raise SystemExit(f"Run already exists: {target}")

    for subdir in ["config", "metrics", "logs", "results", "screenshots"]:
        (target / subdir).mkdir(parents=True, exist_ok=True)

    manifest = {
        "run_id": args.run_id,
        "experiment_group": "routing-policy-comparison",
        "timestamp_start": "",
        "timestamp_end": "",
        "policy": args.policy,
        "workload_file": args.workload_file,
        "job_count": args.job_count,
        "submission_pattern": args.submission_pattern,
        "run_status": "planned",
        "submitted_job_ids": [],
        "policy_applied": "",
        "artifact_collection_status": "pending",
        "summary_status": "pending",
        "anomalies": [],
        "notes": args.notes,
        "artifacts": {
            "results_dir": "results/",
            "logs_dir": "logs/",
            "metrics_dir": "metrics/",
            "screenshots": [
                f"screenshots/{args.run_id}-cluster-overview.png",
                f"screenshots/{args.run_id}-node2-detail.png",
                f"screenshots/{args.run_id}-node3-detail.png",
                f"screenshots/{args.run_id}-worker-execution.png",
            ],
        },
    }
    write_json(target / "manifest.json", manifest)
    (target / "notes.md").write_text(f"# Run Notes\n\n## Run ID\n- {args.run_id}\n\n## Notes\n- {args.notes}\n")

    for path in [
        REPO_ROOT / "observability/control-node/prometheus/prometheus.yml",
        REPO_ROOT / "observability/node-collectors/node-exporter-compose.yml",
    ]:
        if path.exists():
            shutil.copy2(path, target / "config" / path.name)

    workload_path = REPO_ROOT / args.workload_file
    if workload_path.exists():
        workload_target = target / "config" / "workloads"
        workload_target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(workload_path, workload_target / workload_path.name)

    print(target)


if __name__ == "__main__":
    main()
