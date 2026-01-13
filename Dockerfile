FROM python:3.9-slim

WORKDIR /app

# Copy dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app/ app/

CMD ["python", "app/main.py"]
