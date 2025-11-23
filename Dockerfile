# Dockerfile
FROM python:3.11-slim

# Create app directory
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install basic SSL certs (for HTTPS to ESI)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy dependency list and install
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code and config
COPY ./corp_ledger.py ./config.yaml ./

# By default just show help; we'll override in the run script
CMD ["python", "corp_ledger.py", "--help"]
