FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libx11-6 \
    libxext6 \
    libxrender1 \
    libxft2 \
    libfreetype6 \
    python3-tk \
    tk \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app/ app/
COPY seed.py .
COPY scripts/ scripts/

CMD ["python", "app/main.py"]
