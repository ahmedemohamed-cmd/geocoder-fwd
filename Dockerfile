FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (including libexpat for osmium, curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libexpat1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy only necessary application directories
COPY services/ /app/services/
COPY shared/ /app/shared/
COPY run.py /app/

# Create data directory
RUN mkdir -p /app/data

# Default command (can be overridden in docker-compose)
CMD ["python", "-m", "services.geocoder"]
