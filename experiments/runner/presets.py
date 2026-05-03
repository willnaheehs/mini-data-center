from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExperimentPresetRun:
    policy: str
    workload_file: str
    workload_label: str


CLASS_PROJECT_POLICIES = [
    "shortest_queue",
    "random",
    "round_robin",
    "adaptive",
]

CLASS_PROJECT_WORKLOADS = [
    ("light_compute", "experiments/workloads/light-compute.json"),
    ("heavy_compute", "experiments/workloads/heavy-compute.json"),
    ("mixed_compute", "experiments/workloads/mixed-real.json"),
]


def build_class_project_runs() -> list[ExperimentPresetRun]:
    runs: list[ExperimentPresetRun] = []
    for workload_label, workload_file in CLASS_PROJECT_WORKLOADS:
        for policy in CLASS_PROJECT_POLICIES:
            runs.append(
                ExperimentPresetRun(
                    policy=policy,
                    workload_file=workload_file,
                    workload_label=workload_label,
                )
            )
    return runs


def class_project_suite_definition() -> dict:
    runs = build_class_project_runs()
    return {
        "preset_id": "class-project-routing-suite",
        "title": "Class project routing suite",
        "description": "12 prebuilt runs covering 4 routing policies across light, heavy, and mixed compute workloads.",
        "run_count": len(runs),
        "policies": CLASS_PROJECT_POLICIES,
        "workloads": [
            {
                "label": workload_label,
                "workload_file": workload_file,
                "job_type": "compute",
            }
            for workload_label, workload_file in CLASS_PROJECT_WORKLOADS
        ],
        "runs": [run.__dict__ for run in runs],
    }
