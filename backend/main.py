from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import os
import uuid
import time
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from threading import Lock

app = FastAPI(title="RoadEye API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # OK for local dev; tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage")).resolve()
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

JOBS_PATH = STORAGE_DIR / "jobs.json"
JOBS_LOCK = Lock()


def _now() -> float:
    return time.time()


def _load_jobs() -> Dict[str, Any]:
    if not JOBS_PATH.exists():
        return {}
    try:
        return json.loads(JOBS_PATH.read_text())
    except Exception:
        # If jobs.json is corrupted, fail safe to empty (you can restore later)
        return {}


def _save_jobs(jobs: Dict[str, Any]) -> None:
    tmp = STORAGE_DIR / "jobs.json.tmp"
    tmp.write_text(json.dumps(jobs, indent=2))
    tmp.replace(JOBS_PATH)


def _get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with JOBS_LOCK:
        jobs = _load_jobs()
        j = jobs.get(job_id)
        return dict(j) if j else None


def _set_job(job_id: str, patch: Dict[str, Any]) -> None:
    with JOBS_LOCK:
        jobs = _load_jobs()
        if job_id not in jobs:
            return
        jobs[job_id].update(patch)
        _save_jobs(jobs)


def _job_dir(job_id: str) -> Path:
    d = STORAGE_DIR / job_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _results_path(job_id: str) -> Path:
    return _job_dir(job_id) / "results.json"


@app.get("/health")
def health():
    return {"status": "ok"}


# -----------------------
# Existing Job Endpoints
# -----------------------

@app.post("/jobs")
def create_job(payload: dict):
    """
    Accepts either:
      { "asset_types": ["mileposts", ...] }   (old)
    or
      { "assets": ["mileposts", ...], "filename": "x.mp4" }  (frontend)
    """
    asset_types = payload.get("asset_types")
    if asset_types is None:
        asset_types = payload.get("assets")

    if not isinstance(asset_types, list) or not asset_types:
        raise HTTPException(status_code=400, detail="assets/asset_types must be a non-empty list")

    filename_hint = payload.get("filename")

    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "asset_types": asset_types,
        "status": "created",
        "video_path": None,
        "filename_hint": filename_hint,
        "results": None,
        "results_path": None,
        "error": None,
        "created_at": _now(),
        "started_at": None,
        "finished_at": None,
    }

    with JOBS_LOCK:
        jobs = _load_jobs()
        jobs[job_id] = job
        _save_jobs(jobs)

    return {"job_id": job_id}


@app.post("/jobs/{job_id}/upload")
async def upload_video(
    job_id: str,
    file: Optional[UploadFile] = File(None),   # frontend sends "file"
    video: Optional[UploadFile] = File(None),  # old version sends "video"
):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    upload = file or video
    if upload is None:
        raise HTTPException(status_code=400, detail="Missing upload field: file (or video)")

    ext = Path(upload.filename).suffix or ".mp4"
    filename = f"{job_id}{ext}"
    dest = STORAGE_DIR / filename

    # Stream to disk in chunks so large files don't explode RAM
    try:
        with dest.open("wb") as f:
            while True:
                chunk = await upload.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                f.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {e}")

    _set_job(job_id, {"video_path": str(dest), "status": "uploaded", "error": None})
    return {"job_id": job_id, "status": "uploaded", "video_path": str(dest)}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.post("/jobs/{job_id}/process")
def process_job(job_id: str):
    """
    Queue the job for the worker. The worker will set:
      queued -> processing -> done/error
    """
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    status = job.get("status")

    if status == "created":
        raise HTTPException(status_code=400, detail="job must be uploaded before processing")

    if status == "uploaded":
        _set_job(job_id, {"status": "queued", "error": None})
        return {"job_id": job_id, "status": "queued"}

    if status in ("queued", "processing"):
        return {"job_id": job_id, "status": status}

    if status == "done":
        results = job.get("results") or []
        return {"job_id": job_id, "status": "done", "detections": len(results)}

    if status == "error":
        return {"job_id": job_id, "status": "error", "error": job.get("error")}

    return {"job_id": job_id, "status": status or "unknown"}


# -----------------------
# New API Endpoints
# -----------------------

@app.get("/jobs")
def list_jobs(
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
    newest_first: bool = True,
):
    """
    List jobs for UI dashboards / refresh after reload.

    Query params:
      - limit (default 50, max 500)
      - offset (default 0)
      - status (optional: created/uploaded/queued/processing/done/error)
      - newest_first (default True)
    """
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    if limit > 500:
        limit = 500
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0")

    with JOBS_LOCK:
        jobs = _load_jobs()

    items = list(jobs.values())

    if status:
        items = [j for j in items if (j.get("status") == status)]

    # Sort by created_at (fallback 0)
    items.sort(key=lambda j: j.get("created_at") or 0, reverse=newest_first)

    total = len(items)
    page = items[offset: offset + limit]

    # Keep response light by default (results can be fetched separately)
    for j in page:
        if isinstance(j.get("results"), list):
            # results might be large; leave count only
            j["results_count"] = len(j["results"])
            j["results"] = None

    return {"total": total, "limit": limit, "offset": offset, "items": page}


@app.get("/jobs/{job_id}/results")
def get_job_results(job_id: str):
    """
    Return results JSON. Source order:
      1) /storage/<job_id>/results.json
      2) job["results"] in jobs.json
    """
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    rp = _results_path(job_id)
    if rp.exists():
        try:
            data = json.loads(rp.read_text())
            return {"job_id": job_id, "results": data}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read results.json: {e}")

    if isinstance(job.get("results"), list):
        return {"job_id": job_id, "results": job["results"]}

    # If not done yet, return useful status instead of 404
    status = job.get("status")
    if status in ("queued", "processing", "uploaded", "created"):
        return JSONResponse(
            status_code=202,
            content={"job_id": job_id, "status": status, "message": "Results not ready yet"},
        )

    if status == "error":
        raise HTTPException(status_code=500, detail=job.get("error") or "job failed")

    raise HTTPException(status_code=404, detail="results not found")


@app.get("/jobs/{job_id}/video")
def get_job_video(job_id: str):
    """
    Stream the uploaded video back (debug endpoint).
    """
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    vp = job.get("video_path")
    if not vp:
        raise HTTPException(status_code=404, detail="video not uploaded")

    path = Path(vp)
    if not path.exists():
        raise HTTPException(status_code=404, detail="video file missing on disk")

    # FastAPI will stream this efficiently
    return FileResponse(path, media_type="video/mp4", filename=path.name)
