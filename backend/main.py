from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import requests
from typing import Optional, List, Dict, Any

from services.r2 import presign_put, presign_get, put_json, get_json
from services.jobs import create_job, get_job, save_job, results_key, video_key as make_video_key


app = FastAPI(title="RoadEye API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # OK for POC; tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/jobs")
def api_create_job(payload: dict):
    asset_types = payload.get("asset_types")
    if asset_types is None:
        asset_types = payload.get("assets")

    if not isinstance(asset_types, list) or not asset_types:
        raise HTTPException(status_code=400, detail="assets/asset_types must be a non-empty list")

    filename_hint = payload.get("filename")
    job = create_job(asset_types=asset_types, filename_hint=filename_hint)
    return {"job_id": job["job_id"]}


@app.get("/jobs/{job_id}")
def api_get_job(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


# Canonical upload flow
@app.post("/jobs/{job_id}/upload-url")
def api_upload_url(job_id: str, payload: dict):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    filename = payload.get("filename") or job.get("filename_hint") or "upload.mp4"
    content_type = payload.get("content_type") or "application/octet-stream"

    key = make_video_key(job_id, filename)
    url = presign_put(key=key, content_type=content_type, expires_seconds=3600)
    return {"upload_url": url, "video_key": key}


@app.post("/jobs/{job_id}/upload-complete")
def api_upload_complete(job_id: str, payload: dict):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    vkey = payload.get("video_key")
    if not vkey:
        raise HTTPException(status_code=400, detail="video_key is required")

    job["video_key"] = vkey
    job["status"] = "uploaded"
    job["error"] = None
    save_job(job)

    return {"job_id": job_id, "status": "uploaded", "video_key": vkey}


# Keep legacy upload route working (fallback): client uploads to backend, backend writes to R2 via presign PUT
# This avoids local disk dependency.
from fastapi import UploadFile, File  # noqa: E402


@app.post("/jobs/{job_id}/upload")
async def legacy_upload(job_id: str, file: Optional[UploadFile] = File(None), video: Optional[UploadFile] = File(None)):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    upload = file or video
    if upload is None:
        raise HTTPException(status_code=400, detail="Missing upload field: file (or video)")

    filename = upload.filename or "upload.mp4"
    content_type = upload.content_type or "application/octet-stream"
    key = make_video_key(job_id, filename)
    url = presign_put(key=key, content_type=content_type, expires_seconds=3600)

    # stream from client upload to R2 PUT
    resp = requests.put(url, data=await upload.read(), headers={"Content-Type": content_type})
    if not resp.ok:
        raise HTTPException(status_code=500, detail=f"Failed to upload to R2: {resp.status_code}")

    job["video_key"] = key
    job["status"] = "uploaded"
    job["error"] = None
    save_job(job)

    return {"job_id": job_id, "status": "uploaded", "video_key": key}


@app.post("/jobs/{job_id}/submit")
def api_submit(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    if job.get("status") == "created":
        raise HTTPException(status_code=400, detail="job must be uploaded before submit")

    if not job.get("video_key"):
        raise HTTPException(status_code=400, detail="job missing video_key (upload not completed)")

    if job.get("status") in ("queued", "running"):
        return {"job_id": job_id, "status": job["status"]}

    if job.get("status") == "completed":
        return {"job_id": job_id, "status": "completed"}

    # mark queued before submission
    job["status"] = "queued"
    job["error"] = None
    save_job(job)

    runpod_api_key = require_env("RUNPOD_API_KEY")
    endpoint_id = require_env("RUNPOD_ENDPOINT_ID")

    # RunPod REST submit
    submit_url = f"https://api.runpod.ai/v2/{endpoint_id}/run"
    headers = {"Authorization": f"Bearer {runpod_api_key}", "Content-Type": "application/json"}
    payload = {"input": {"job_id": job_id, "video_key": job["video_key"]}}

    r = requests.post(submit_url, headers=headers, json=payload, timeout=30)
    if not r.ok:
        job["status"] = "failed"
        job["error"] = f"RunPod submit failed: {r.status_code}"
        save_job(job)
        raise HTTPException(status_code=500, detail=job["error"])

    job["status"] = "running"
    save_job(job)
    return {"job_id": job_id, "status": job["status"]}


# Keep legacy route /process as alias to /submit
@app.post("/jobs/{job_id}/process")
def legacy_process(job_id: str):
    return api_submit(job_id)


@app.get("/jobs/{job_id}/results")
def api_results(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    rk = job.get("results_key") or results_key(job_id)
    data = get_json(rk)

    status = job.get("status")
    if data is None:
        if status in ("created", "uploaded", "queued", "running"):
            return JSONResponse(status_code=202, content={"job_id": job_id, "status": status, "message": "Results not ready yet"})
        if status == "failed":
            raise HTTPException(status_code=500, detail=job.get("error") or "job failed")
        raise HTTPException(status_code=404, detail="results not found")

    return {"job_id": job_id, "results": data}


@app.get("/jobs/{job_id}/video")
def api_video(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    vkey = job.get("video_key")
    if not vkey:
        raise HTTPException(status_code=404, detail="video not uploaded")
    return {"job_id": job_id, "video_url": presign_get(vkey, expires_seconds=3600)}
