import os
import time
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from threading import Lock

STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "/storage")).resolve()
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", "2"))
JOBS_PATH = STORAGE_DIR / "jobs.json"
MEM_LOCK = Lock()

# Extraction knobs
SAMPLE_FPS = float(os.getenv("SAMPLE_FPS", "1.0"))          # frames per second to analyze
MAX_SAMPLED_FRAMES = int(os.getenv("MAX_FRAMES", "300"))    # safety cap per job
YOLO_MODEL = os.getenv("YOLO_MODEL", "yolov8n.pt")          # can be yolov8n.pt or a local path
YOLO_CONF = float(os.getenv("YOLO_CONF", "0.25"))           # confidence threshold
YOLO_IOU = float(os.getenv("YOLO_IOU", "0.45"))             # iou threshold for NMS
YOLO_MAX_DET = int(os.getenv("YOLO_MAX_DET", "100"))        # max detections per frame

# OpenCV
try:
    import cv2  # type: ignore
except Exception as e:
    cv2 = None
    CV2_IMPORT_ERROR = str(e)
else:
    CV2_IMPORT_ERROR = None

# YOLO
try:
    from ultralytics import YOLO  # type: ignore
except Exception as e:
    YOLO = None
    YOLO_IMPORT_ERROR = str(e)
else:
    YOLO_IMPORT_ERROR = None

# Global model cache so we load YOLO only once per worker process
_MODEL = None


def _now() -> float:
    return time.time()


def _load_jobs() -> Dict[str, Any]:
    if not JOBS_PATH.exists():
        return {}
    try:
        return json.loads(JOBS_PATH.read_text())
    except Exception:
        return {}


def _save_jobs(jobs: Dict[str, Any]) -> None:
    tmp = STORAGE_DIR / "jobs.json.tmp"
    tmp.write_text(json.dumps(jobs, indent=2))
    tmp.replace(JOBS_PATH)


def _job_dir(job_id: str) -> Path:
    d = STORAGE_DIR / job_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_results(job_id: str, results: List[Dict[str, Any]]) -> str:
    out_dir = _job_dir(job_id)
    out_path = out_dir / "results.json"
    out_path.write_text(json.dumps(results, indent=2))
    return str(out_path)


def _safe_float(x, default: float) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _load_model():
    global _MODEL
    if _MODEL is not None:
        return _MODEL

    if YOLO is None:
        raise RuntimeError(
            "Ultralytics YOLO is not available. "
            f"Import error: {YOLO_IMPORT_ERROR}"
        )

    print(f"[worker] Loading YOLO model: {YOLO_MODEL}")
    # If YOLO_MODEL is like 'yolov8n.pt', ultralytics will download it if not present.
    # If your environment is offline, set YOLO_MODEL to a local path mounted into the container.
    _MODEL = YOLO(YOLO_MODEL)
    return _MODEL


def _yolo_detect(frame_bgr, frame_index: int, timestamp_sec: float) -> List[Dict[str, Any]]:
    """
    Run YOLO on a single frame and return detections in a consistent schema:
      {
        "asset_type": "<class_name>",
        "confidence": float,
        "frame": int,
        "timestamp_sec": float,
        "bbox": [x1,y1,x2,y2],
        "source": "yolo",
        "class_id": int
      }
    """
    model = _load_model()

    # Ultralytics can accept numpy arrays directly (BGR is fine); keep as-is for speed.
    results = model.predict(
        source=frame_bgr,
        verbose=False,
        conf=YOLO_CONF,
        iou=YOLO_IOU,
        max_det=YOLO_MAX_DET,
    )

    dets: List[Dict[str, Any]] = []
    if not results:
        return dets

    r0 = results[0]
    names = getattr(r0, "names", {}) or {}
    boxes = getattr(r0, "boxes", None)
    if boxes is None:
        return dets

    # boxes.xyxy, boxes.conf, boxes.cls are torch tensors
    try:
        xyxy = boxes.xyxy.cpu().numpy()
        conf = boxes.conf.cpu().numpy()
        cls = boxes.cls.cpu().numpy()
    except Exception:
        # Fallback if CPU conversion fails
        return dets

    for i in range(len(xyxy)):
        x1, y1, x2, y2 = xyxy[i].tolist()
        c = float(conf[i])
        class_id = int(cls[i])
        class_name = names.get(class_id, str(class_id))

        dets.append(
            {
                "asset_type": class_name,  # for Option 1, treat detected class as asset_type
                "confidence": round(c, 4),
                "frame": int(frame_index),
                "timestamp_sec": round(float(timestamp_sec), 3),
                "bbox": [int(x1), int(y1), int(x2), int(y2)],
                "source": "yolo",
                "class_id": class_id,
            }
        )

    return dets


def _extract_from_video(video_path: str) -> List[Dict[str, Any]]:
    """
    Frame sampling + YOLO inference.

    Returns a flat list of detections across sampled frames.
    """
    if cv2 is None:
        raise RuntimeError(
            "OpenCV (cv2) is not available in the worker container. "
            f"Import error: {CV2_IMPORT_ERROR}"
        )

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Failed to open video with OpenCV: {video_path}")

    fps = _safe_float(cap.get(cv2.CAP_PROP_FPS), 0.0)
    if fps <= 0:
        fps = 30.0  # fallback if codec reports 0

    sample_every_n_frames = max(int(round(fps / max(SAMPLE_FPS, 0.1))), 1)

    detections: List[Dict[str, Any]] = []
    frame_idx = -1
    sampled = 0

    while True:
        ok, frame = cap.read()
        if not ok:
            break

        frame_idx += 1

        if frame_idx % sample_every_n_frames != 0:
            continue

        timestamp_sec = frame_idx / fps
        detections.extend(_yolo_detect(frame, frame_idx, timestamp_sec))

        sampled += 1
        if sampled >= MAX_SAMPLED_FRAMES:
            break

    cap.release()
    return detections


def process_job(job_id: str, job: Dict[str, Any]) -> Dict[str, Any]:
    video_path = job.get("video_path")
    if not video_path or not Path(video_path).exists():
        job["status"] = "error"
        job["error"] = "uploaded video file is missing"
        job["finished_at"] = _now()
        return job

    # Mark processing
    job["status"] = "processing"
    job["started_at"] = _now()
    job["error"] = None

    try:
        results = _extract_from_video(video_path)

        results_path = _write_results(job_id, results)
        job["status"] = "done"
        job["results"] = results
        job["results_path"] = results_path
        job["finished_at"] = _now()
        return job

    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)
        job["finished_at"] = _now()
        return job


def main():
    print(f"[worker] Starting. STORAGE_DIR={STORAGE_DIR} POLL_SECONDS={POLL_SECONDS}")
    print(f"[worker] Extraction config: SAMPLE_FPS={SAMPLE_FPS} MAX_FRAMES={MAX_SAMPLED_FRAMES}")
    print(f"[worker] YOLO config: YOLO_MODEL={YOLO_MODEL} YOLO_CONF={YOLO_CONF} YOLO_IOU={YOLO_IOU} YOLO_MAX_DET={YOLO_MAX_DET}")

    while True:
        try:
            with MEM_LOCK:
                jobs = _load_jobs()
                queued = [(jid, j) for jid, j in jobs.items() if j.get("status") == "queued"]

                if queued:
                    job_id, job = queued[0]
                    print(f"[worker] Processing job {job_id}")

                    jobs[job_id] = process_job(job_id, job)
                    _save_jobs(jobs)

        except Exception as e:
            print(f"[worker] ERROR: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
