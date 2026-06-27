FROM python:3.12-slim

WORKDIR /app

# Dependencies first (for Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY produce.py .

# -u: log output unbuffered (shows up immediately in container logs)
CMD ["python", "-u", "produce.py"]
