import io
import os
from typing import BinaryIO, Optional

from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "CHANGE_ME_MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "CHANGE_ME_MINIO_ACCESS_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mini-dc-artifacts")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").strip().lower() in {"1", "true", "yes", "on"}

_client: Optional[Minio] = None


def get_minio_client() -> Minio:
    global _client
    if _client is None:
        _client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
    return _client


def ensure_bucket_exists() -> None:
    client = get_minio_client()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)


def upload_bytes(object_key: str, data: bytes, content_type: str = "application/octet-stream") -> dict:
    ensure_bucket_exists()
    client = get_minio_client()
    stream = io.BytesIO(data)
    result = client.put_object(
        MINIO_BUCKET,
        object_key,
        stream,
        length=len(data),
        content_type=content_type,
    )
    return {
        "bucket": MINIO_BUCKET,
        "object_key": object_key,
        "etag": result.etag,
        "version_id": getattr(result, "version_id", None),
        "content_type": content_type,
        "size_bytes": len(data),
    }


def upload_file(object_key: str, file_path: str, content_type: str = "application/octet-stream") -> dict:
    with open(file_path, "rb") as f:
        data = f.read()
    return upload_bytes(object_key=object_key, data=data, content_type=content_type)


def download_bytes(object_key: str) -> bytes:
    client = get_minio_client()
    response = client.get_object(MINIO_BUCKET, object_key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def object_exists(object_key: str) -> bool:
    client = get_minio_client()
    try:
        client.stat_object(MINIO_BUCKET, object_key)
        return True
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
            return False
        raise


def artifact_url(object_key: str) -> str:
    return f"/artifacts/{object_key}"
