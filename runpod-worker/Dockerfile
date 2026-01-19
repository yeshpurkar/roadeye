# RunPod Serverless base image (CUDA + Python)
FROM runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# System deps for OpenCV headless
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Set workdir
WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt /app/requirements.txt

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker code
COPY handler.py /app/handler.py

# RunPod expects this
CMD ["python", "-u", "handler.py"]
