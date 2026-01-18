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
COPY ["npsa-scraper/School Scraper/docs/requirements.txt", "./requirements.txt"]
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy the School Scraper directory contents
COPY ["npsa-scraper/School Scraper/", "./"]

# Verify assets/data/state_counties directory exists
RUN ls -la assets/data/state_counties/ | head -5 || echo "Warning: assets/data/state_counties directory not found"

# Railway listens on port from environment variable (default 8080)
ENV PORT=8080
EXPOSE 8080

# Run the Flask API server
CMD ["python3", "external_services/api.py"]
