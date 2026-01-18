import time
import uuid
from typing import Any, Dict, List, Optional

from .r2 import get_json, put_json


def now() -> float:
    return time.time()


def job_key(job_id: str) -> str:
    return f"jobs/{job_id}.json"


def results_key(job_id: str) -> str:
    return f"results/{job_id}.json"


def video_key(job_id: str, filename: str) -> str:
    # keep a stable prefix per job; randomize to avoid collisions
    ext = ""
    if "." in filename:
        ext = "." + filename.split(".")[-1]
    return f"videos/{job_id}/{uuid.uuid4().hex}{ext or '.mp4'}"


def create_job(asset_types: List[str], filename_hint: Optional[str]) -> Dict[str, Any]:
    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "asset_types": asset_types,
        "filename_hint": filename_hint,
        "status": "created",
        "video_key": None,
        "results_key": None,
        "error": None,
        "created_at": now(),
        "started_at": None,
        "finished_at": None,
    }
    put_json(job_key(job_id), job)
    return job


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return get_json(job_key(job_id))


def save_job(job: Dict[str, Any]) -> None:
    put_json(job_key(job["job_id"]), job)
