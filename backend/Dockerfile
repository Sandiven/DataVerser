FROM python:3.13.7-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean

# Copy requirements
COPY backend/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Copy all backend files
COPY backend/. .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
