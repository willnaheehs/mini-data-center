# Experiments

This directory holds the repeatable experiment workflow for the mini data center project.

## Layout

- `templates/` - starter files used to seed each run folder
- `scripts/` - automation helpers for creating and collecting experiment runs
- `runs/` - per-run raw artifacts and notes
- `analysis/` - derived analysis scripts/notebooks/tables
- `plots/` - final report-quality plots

## V1 workflow

1. Create a run folder with `scripts/new_run.sh`
2. Run the experiment with `scripts/run_experiment.sh`
3. Collect artifacts with `scripts/collect_artifacts.sh`
4. Summarize results with `scripts/summarize_results.py`
5. Capture screenshots with `scripts/capture_screenshots.sh`

## Screenshot naming

Screenshots should include the run id in the filename so every artifact is self-identifying.
Examples:
- `2026-04-24-light-round-robin-run01-cluster-overview.png`
- `2026-04-24-light-round-robin-run01-node2-detail.png`
- `2026-04-24-light-round-robin-run01-worker-execution.png`
