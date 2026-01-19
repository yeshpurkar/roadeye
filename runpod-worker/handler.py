import os
import json
import tempfile
from typing import Any, Dict, Optional, List

import runpod
import boto3
from botocore.config import Config
from ultralytics import YOLO
import cv2


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


R2_ENDPOINT = require_env("R2_ENDPOINT")
R2_ACCESS_KEY_ID = require_env("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = require_env("R2_SECRET_ACCESS_KEY")
R2_BUCKET = require_env("R2_BUCKET")

MODEL_NAME = os.environ.get("YOLO_MODEL", "yolov8n.pt")
FRAME_STRIDE = int(os.environ.get("FRAME_STRIDE", "10"))

# Optional tuning
YOLO_CONF = float(os.environ.get("YOLO_CONF", "0.25"))
YOLO_IOU = float(os.environ.get("YOLO_IOU", "0.45"))
YOLO_MAX_DET = int(os.environ.get("YOLO_MAX_DET", "300"))


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def job_key(job_id: str) -> str:
    return f"jobs/{job_id}.json"


def results_key(job_id: str) -> str:
    return f"results/{job_id}.json"


def load_job(s3, job_id: str) -> Dict[str, Any]:
    obj = s3.get_object(Bucket=R2_BUCKET, Key=job_key(job_id))
    return json.loads(obj["Body"].read().decode("utf-8"))


def save_job(s3, job: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=R2_BUCKET,
        Key=job_key(job["job_id"]),
        Body=json.dumps(job).encode("utf-8"),
        ContentType="application/json",
    )


def write_results(s3, job_id: str, data: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=R2_BUCKET,
        Key=results_key(job_id),
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )


def fail_job(s3, job: Dict[str, Any], message: str) -> Dict[str, Any]:
    job["status"] = "failed"
    job["error"] = message
    save_job(s3, job)
    return {"status": "failed", "job_id": job.get("job_id"), "error": message}


def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    inp = event.get("input") or {}
    job_id = inp.get("job_id")
    video_key = inp.get("video_key")

    if not job_id or not video_key:
        return {"status": "failed", "error": "job_id and video_key are required in event.input"}

    s3 = s3_client()

    try:
        job = load_job(s3, job_id)
    except Exception as e:
        return {"status": "failed", "job_id": job_id, "error": f"Unable to load job from R2: {e}"}

    # Mark running
    job["status"] = "running"
    job["error"] = None
    save_job(s3, job)

    try:
        model = YOLO(MODEL_NAME)

        with tempfile.TemporaryDirectory() as td:
            video_path = os.path.join(td, "video.mp4")
            s3.download_file(R2_BUCKET, video_key, video_path)

            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                return fail_job(s3, job, "OpenCV could not open the video file")

            frame_idx = 0
            frames: List[Dict[str, Any]] = []

            while True:
                ok, frame = cap.read()
                if not ok:
                    break

                if frame_idx % FRAME_STRIDE == 0:
                    results = model.predict(
                        source=frame,
                        verbose=False,
                        conf=YOLO_CONF,
                        iou=YOLO_IOU,
                        max_det=YOLO_MAX_DET,
                    )

                    frame_out: List[Dict[str, Any]] = []
                    for r in results:
                        if getattr(r, "boxes", None) is None:
                            continue
                        for b in r.boxes:
                            cls = int(b.cls.item())
                            conf = float(b.conf.item())
                            xyxy = [float(x) for x in b.xyxy[0].tolist()]
                            frame_out.append({"class": cls, "conf": conf, "xyxy": xyxy})

                    frames.append({"frame": frame_idx, "detections": frame_out})

                frame_idx += 1

            cap.release()

        out = {
            "job_id": job_id,
            "video_key": video_key,
            "frame_stride": FRAME_STRIDE,
            "yolo": {"model": MODEL_NAME, "conf": YOLO_CONF, "iou": YOLO_IOU, "max_det": YOLO_MAX_DET},
            "frames": frames,
        }

        write_results(s3, job_id, out)

        job["status"] = "completed"
        job["results_key"] = results_key(job_id)
        job["error"] = None
        save_job(s3, job)

        return {"status": "completed", "job_id": job_id, "results_key": job["results_key"]}

    except Exception as e:
        return fail_job(s3, job, f"Worker exception: {e}")


runpod.serverless.start({"handler": handler})
