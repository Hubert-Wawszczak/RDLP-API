FROM python:3.13.0-slim-bullseye

WORKDIR /app

# Install system dependencies for PostGIS, Shapely, and GeoPandas
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    g++ \
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    gdal-bin \
    python3-gdal \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

CMD ["python", "main.py"]