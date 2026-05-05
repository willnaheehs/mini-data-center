# Experiments

This directory holds the repeatable experiment workflow for the mini data center project.

## Layout

- `templates/` - starter files used to seed each run folder
- `scripts/` - shell helpers for scaffolding and artifact handling
- `runner/` - independent HTTP-driven experiment orchestration scripts
- `runs/` - per-run raw artifacts and notes
- `analysis/` - derived analysis scripts/notebooks/tables
- `plots/` - final report-quality plots

## V1 workflow

1. Create a run folder with `runner/create_run.py` or `scripts/new_run.sh`
2. Start the experiment with `runner/start_run.py`
3. Collect job results with `runner/collect_run.py`
4. Summarize results with `runner/summarize_run.py`
5. Capture screenshots with `scripts/capture_screenshots.sh`

