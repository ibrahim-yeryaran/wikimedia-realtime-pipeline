FROM python:3.12-slim

WORKDIR /app

# Önce bağımlılıklar (Docker katman cache'i için)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY produce.py .

# -u: çıktıyı tamponlamadan logla (container loglarında anında görünsün)
CMD ["python", "-u", "produce.py"]
