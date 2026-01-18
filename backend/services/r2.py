import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import Optional, Dict, Any


def get_s3_client():
    endpoint = os.environ["R2_ENDPOINT"]
    access_key = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]

    # R2 is S3-compatible. Signature v4 is required.
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def bucket_name() -> str:
    return os.environ["R2_BUCKET"]


def presign_put(key: str, content_type: str, expires_seconds: int = 3600) -> str:
    s3 = get_s3_client()
    return s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": bucket_name(), "Key": key, "ContentType": content_type},
        ExpiresIn=expires_seconds,
    )


def presign_get(key: str, expires_seconds: int = 3600) -> str:
    s3 = get_s3_client()
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket_name(), "Key": key},
        ExpiresIn=expires_seconds,
    )


def put_json(key: str, data: Dict[str, Any]) -> None:
    import json

    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket_name(),
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )


def get_json(key: str) -> Optional[Dict[str, Any]]:
    import json

    s3 = get_s3_client()
    try:
        obj = s3.get_object(Bucket=bucket_name(), Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return None
        raise
