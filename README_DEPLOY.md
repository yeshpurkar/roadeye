# RoadEye Cloud-Native Deployment Guide

## Architecture
- Frontend: Cloudflare Pages (Vite/React)
- Backend: Fly.io (FastAPI)
- Storage: Cloudflare R2
- GPU Worker: RunPod Serverless (YOLO + OpenCV headless)

## Required environment variables (do not commit secrets)
Backend (Fly):
- R2_ENDPOINT
- R2_ACCESS_KEY_ID
- R2_SECRET_ACCESS_KEY
- R2_BUCKET
- RUNPOD_API_KEY
- RUNPOD_ENDPOINT_ID

Worker (RunPod):
- R2_ENDPOINT
- R2_ACCESS_KEY_ID
- R2_SECRET_ACCESS_KEY
- R2_BUCKET
- YOLO_MODEL (default yolov8n.pt)
- FRAME_STRIDE (optional)

Frontend (Pages):
- VITE_API_BASE

## Canonical upload flow
1) Create job: `POST /jobs`
2) Get presigned PUT: `POST /jobs/{job_id}/upload-url`
3) Upload to R2 via PUT to `upload_url`
4) Finalize: `POST /jobs/{job_id}/upload-complete`
5) Submit: `POST /jobs/{job_id}/submit`
6) Poll: `GET /jobs/{job_id}` and `GET /jobs/{job_id}/results`

## Smoke test (example)
```bash
curl -s -X POST https://<fly-app>.fly.dev/jobs \
  -H "Content-Type: application/json" \
  -d '{"assets":["mileposts"],"filename":"demo.mp4"}'


---

## 3K — Check status
Now run:

```bash
git status

