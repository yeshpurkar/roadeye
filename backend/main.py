from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import uuid
from pathlib import Path
from typing import Optional

app = FastAPI(title="RoadEye API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # OK for local dev; tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage"))
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

# In-memory jobs for MVP
JOBS = {}

@app.get("/health")
def health():
    return {"status": "ok"}

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
    JOBS[job_id] = {
        "job_id": job_id,
        "asset_types": asset_types,
        "status": "created",
        "video_path": None,
        "filename_hint": filename_hint,
    }

    # Return minimal response expected by frontend
    return {"job_id": job_id}

@app.post("/jobs/{job_id}/upload")
async def upload_video(
    job_id: str,
    file: Optional[UploadFile] = File(None),   # frontend sends "file"
    video: Optional[UploadFile] = File(None),  # your old version sends "video"
):
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="job not found")

    upload = file or video
    if upload is None:
        raise HTTPException(status_code=400, detail="Missing upload field: file (or video)")

    ext = Path(upload.filename).suffix or ".mp4"
    filename = f"{job_id}{ext}"
    dest = STORAGE_DIR / filename

    contents = await upload.read()
    dest.write_bytes(contents)

    JOBS[job_id]["video_path"] = str(dest)
    JOBS[job_id]["status"] = "uploaded"

    return {"job_id": job_id, "status": "uploaded", "video_path": str(dest)}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="job not found")
    return JOBS[job_id]
