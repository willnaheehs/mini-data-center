# Mini Data Center API

This FastAPI service accepts jobs, stores job artifacts in MinIO, pushes execution metadata into Redis, and serves the browser UI.

## Artifact flow

- User uploads scripts and input files to the API.
- API stores them in the `mini-dc-artifacts` MinIO bucket.
- Job payloads in Redis contain artifact object keys, not node-local filesystem paths.
- Workers download artifacts from MinIO, execute jobs locally, then upload outputs back to MinIO.
- API exposes artifact download endpoints for the UI.
