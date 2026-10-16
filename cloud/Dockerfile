FROM python:3.11-slim

WORKDIR /app

# Copy application files
COPY setup.py /app/
COPY cloud /app/cloud
COPY pipeline /app/pipeline
COPY biosignal_mock /app/biosignal_mock

# Install package
RUN pip install --no-cache-dir -e .

EXPOSE 8080

CMD ["python", "-m", "cloud.server", "--port", "8080"]
