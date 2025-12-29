# Use slim Python base image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffer logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (Chromium + Chromedriver for Selenium)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg unzip ca-certificates \
    chromium chromium-driver \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Environment variables Selenium expects
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY ["School Scraper/docs/requirements.txt", "./requirements.txt"]
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy the School Scraper directory contents
COPY ["School Scraper/", "./"]

# Verify assets/data/state_counties directory exists
RUN ls -la assets/data/state_counties/ | head -5 || echo "Warning: assets/data/state_counties directory not found"

# Railway listens on port from environment variable (default 8080)
ENV PORT=8080
EXPOSE 8080

# Run the Flask API server with Waitress for production
# Waitress is a pure-Python WSGI server that uses a fixed thread pool, eliminating thread exhaustion issues
# It's designed for production workloads and handles long-running processes better than Flask's dev server
# Using shell form to allow Railway's PORT environment variable to be used
CMD waitress-serve --host=0.0.0.0 --port=${PORT:-8080} --threads=1 --channel-timeout=300 external_services.api:app

