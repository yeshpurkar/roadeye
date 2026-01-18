import os
import json
import tempfile

import runpod
import boto3
from botocore.config import Config
from ultralytics import YOLO
import cv2


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


BUCKET = os.environ.get("R2_BUCKET", "")
MODEL_NAME = os.environ.get("YOLO_MODEL", "yolov8n.pt")
FRAME_STRIDE = int(os.environ.get("FRAME_STRIDE", "10"))


def job_key(job_id: str) -> str:
    return f"jobs/{job_id}.json"


def results_key(job_id: str) -> str:
    return f"results/{job_id}.json"


def load_job(s3, job_id: str):
    obj = s3.get_object(Bucket=BUCKET, Key=job_key(job_id))
    return json.loads(obj["Body"].read().decode("utf-8"))


def save_job(s3, job: dict):
    s3.put_object(Bucket=BUCKET, Key=job_key(job["job_id"]), Body=json.dumps(job).encode("utf-8"), ContentType="application/json")


def write_results(s3, job_id: str, data: dict):
    s3.put_object(Bucket=BUCKET, Key=results_key(job_id), Body=json.dumps(data).encode("utf-8"), ContentType="application/json")


def handler(event):
    job_id = event["input"].get("job_id")
    video_key = event["input"].get("video_key")
    if not job_id or not video_key:
        return {"error": "job_id and video_key are required"}

    s3 = s3_client()
    job = load_job(s3, job_id)
    job["status"] = "running"
    job["error"] = None
    save_job(s3, job)

    model = YOLO(MODEL_NAME)

    with tempfile.TemporaryDirectory() as td:
        video_path = os.path.join(td, "video.mp4")
        s3.download_file(BUCKET, video_key, video_path)

        cap = cv2.VideoCapture(video_path)
        frame_idx = 0
        detections = []

        while True:
            ok, frame = cap.read()
            if not ok:
                break

            if frame_idx % FRAME_STRIDE == 0:
                results = model.predict(source=frame, verbose=False)
                # store minimal results per frame
                frame_out = []
                for r in results:
                    if r.boxes is None:
                        continue
                    for b in r.boxes:
                        cls = int(b.cls.item())
                        conf = float(b.conf.item())
                        xyxy = [float(x) for x in b.xyxy[0].tolist()]
                        frame_out.append({"class": cls, "conf": conf, "xyxy": xyxy})
                detections.append({"frame": frame_idx, "detections": frame_out})

            frame_idx += 1

        cap.release()

    out = {"job_id": job_id, "video_key": video_key, "frame_stride": FRAME_STRIDE, "detections": detections}
    write_results(s3, job_id, out)

    job["status"] = "completed"
    job["results_key"] = results_key(job_id)
    save_job(s3, job)

    return {"status": "completed", "job_id": job_id, "results_key": job["results_key"]}


runpod.serverless.start({"handler": handler})
