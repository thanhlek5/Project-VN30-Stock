FROM python:3.11-slim

WORKDIR /app

# Cài dependencies hệ thống (cần cho một số thư viện Python)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy và cài đặt các thư viện Python cần thiết cho ETL
COPY requirements-etl.txt .
RUN pip install --no-cache-dir -r requirements-etl.txt

# Copy source code ETL
COPY src/ ./src/

CMD ["python", "src/etl/bronze_ingestion.py"]
