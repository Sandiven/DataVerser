FROM python:3.13.7-slim

# avoid Python buffered output (helps logs)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install build deps (needed for compiling many Python packages), then clean apt lists
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker layer cache
COPY backend/requirements.txt ./requirements.txt

# Upgrade packaging tools then install requirements.
# If pip install fails, the "python -m pip debug --verbose" prints helpful diagnostics.
RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir -r requirements.txt || (python -m pip debug --verbose && false)

# Remove build tools to reduce final image size
RUN apt-get purge -y --auto-remove build-essential gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy application code
COPY backend/. .

# Run app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
