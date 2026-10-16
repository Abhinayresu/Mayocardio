FROM python:3.11-slim

# Prevent Python from writing .pyc and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    CLOUD_DB_PATH=/data/cloud_data.db

WORKDIR /app

# Create non-root user and persistent data mount directory
RUN useradd -m -u 1000 appuser && \
    mkdir -p /data && \
    chown -R appuser:appuser /data /app

# Copy application files
COPY setup.py /app/
COPY cloud /app/cloud
COPY pipeline /app/pipeline
COPY biosignal_mock /app/biosignal_mock

# Install package
RUN pip install --no-cache-dir -e .

# Switch to non-root user
USER appuser

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request, os; p = os.environ.get('PORT', '8080'); urllib.request.urlopen(f'http://127.0.0.1:{p}/healthz')" || exit 1

CMD ["python", "-m", "cloud.server"]
