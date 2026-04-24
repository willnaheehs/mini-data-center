# Experiment Runner V1

This experiment system is intentionally built as an orchestration layer around the existing API rather than a rewrite of the current app.

## V1 principles

- Do not disturb the working platform unnecessarily.
- Use the real API-supported workloads, not old dispatcher-only synthetic jobs.
- Treat the existing HTTP API as the execution engine.
- Store run metadata and artifacts under `experiments/runs/<run-id>/`.

## V1 workload files

- `experiments/workloads/light-compute.json`
- `experiments/workloads/heavy-compute.json`
- `experiments/workloads/mixed-real.json`

These are API-native workload definitions intended for the experiment runner.

## V1 runner responsibilities

1. Create a run folder and manifest
2. Set routing policy through the current API
3. Submit jobs through the current API
4. Poll job status/result endpoints until completion
5. Save raw result JSONs and summarized metrics
6. Keep screenshot naming tied to the run id

## Out of scope for V1

- UI integration
- automatic Grafana screenshot capture
- invasive changes to the current app
- file-processing experiments until that path is validated for honest analysis
