# API MVP

This service exposes a simple HTTP front door for the mini-data-center cluster.

## Endpoints
- `GET /healthz`
- `POST /jobs` for JSON jobs
- `POST /jobs/file` for file-processing jobs with upload
- `GET /jobs/{job_id}` for status
- `GET /jobs/{job_id}/result` for final result

## JSON job types
### Compute
```json
{
  "type": "compute",
  "params": {
    "work_units": 25000
  }
}
```

### ML
```json
{
  "type": "ml",
  "params": {
    "operation": "threshold_classify",
    "values": [0.1, 0.6, 0.9],
    "threshold": 0.5
  }
}
```

### File
Either upload with `POST /jobs/file` or submit a JSON file job that references a path visible to the worker.

## Flow
1. Client submits job.
2. API stores initial status in Redis.
3. API enqueues job in Redis.
4. Worker executes the job.
5. Worker writes status and result back to Redis.
6. Client polls for completion.
