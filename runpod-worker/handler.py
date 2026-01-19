import os
import json
import tempfile
from typing import Any, Dict, Optional, List

import boto3
from botocore.config import Config
from ultralytics import YOLO
import cv2
import runpod


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def s3_client():
    # Cloudflare R2 is S3-compatible, region is "auto"
    return boto3.client(
        "s3",
        endpoint_url=require_env("R2_ENDPOINT"),
        aws_access_key_id=require_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=require_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


BUCKET = os.getenv("R2_BUCKET", "")
MODEL_NAME = os.getenv("YOLO_MODEL", "yolov8n.pt")
FRAME_STRIDE = int(os.getenv("FRAME_STRIDE", "10"))


def job_key(job_id: str) -> str:
    return f"jobs/{job_id}.json"


def results_key(job_id: str) -> str:
    return f"results/{job_id}.json"


def load_job(s3, job_id: str) -> Dict[str, Any]:
    obj = s3.get_object(Bucket=BUCKET, Key=job_key(job_id))
    return json.loads(obj["Body"].read().decode("utf-8"))


def save_job(s3, job: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=job_key(job["job_id"]),
        Body=json.dumps(job).encode("utf-8"),
        ContentType="application/json",
    )


def write_results(s3, job_id: str, data: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=results_key(job_id),
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )


def _get_input(event: Dict[str, Any]) -> Dict[str, Any]:
    # RunPod typically provides {"input": {...}}
    inp = event.get("input")
    if isinstance(inp, dict):
        return inp
    return {}


def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    RunPod Serverless handler.

    Expected input:
      {
        "input": {
          "job_id": "...",
          "video_key": "..."
        }
      }
    """
    if not BUCKET:
        # Fail fast (can't do anything without the bucket name)
        return {"error": "Missing required env var: R2_BUCKET"}

    inp = _get_input(event)
    job_id = inp.get("job_id")
    video_key = inp.get("video_key")

    if not job_id or not video_key:
        return {"error": "job_id and video_key are required"}

    s3 = s3_client()

    # Load job, mark running
    job: Optional[Dict[str, Any]] = None
    try:
        job = load_job(s3, job_id)
        job["status"] = "running"
        job["error"] = None
        save_job(s3, job)

        model = YOLO(MODEL_NAME)

        detections: List[Dict[str, Any]] = []

        with tempfile.TemporaryDirectory() as td:
            video_path = os.path.join(td, "video.mp4")
            s3.download_file(BUCKET, video_key, video_path)

            cap = cv2.VideoCapture(video_path)
            try:
                frame_idx = 0
                while True:
                    ok, frame = cap.read()
                    if not ok:
                        break

                    if FRAME_STRIDE > 0 and (frame_idx % FRAME_STRIDE == 0):
                        results = model.predict(source=frame, verbose=False)
                        frame_out: List[Dict[str, Any]] = []

                        for r in results:
                            if getattr(r, "boxes", None) is None:
                                continue

                            for b in r.boxes:
                                # Ultralytics tensor fields
                                cls = int(b.cls.item())
                                conf = float(b.conf.item())
                                xyxy = [float(x) for x in b.xyxy[0].tolist()]
                                frame_out.append(
                                    {"class": cls, "conf": conf, "xyxy": xyxy}
                                )

                        detections.append({"frame": frame_idx, "detections": frame_out})

                    frame_idx += 1
            finally:
                cap.release()

        out = {
            "job_id": job_id,
            "video_key": video_key,
            "frame_stride": FRAME_STRIDE,
            "detections": detections,
        }
        write_results(s3, job_id, out)

        # Mark completed
        job["status"] = "completed"
        job["results_key"] = results_key(job_id)
        save_job(s3, job)

        return {"status": "completed", "job_id": job_id, "results_key": job["results_key"]}

    except Exception as e:
        # Best-effort mark job failed
        try:
            if job is None:
                job = load_job(s3, job_id)
            job["status"] = "failed"
            job["error"] = str(e)
            save_job(s3, job)
        except Exception:
            pass

        return {"status": "failed", "job_id": job_id, "error": str(e)}


runpod.serverless.start({"handler": handler})
