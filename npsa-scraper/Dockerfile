# Use slim Python base image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffer logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (Chromium + Chromedriver for Selenium)
# Pin versions to fix Selenium Manager network issues
# Install dumb-init to properly reap zombie processes (fixes Chrome zombie accumulation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg unzip ca-certificates \
    chromium chromium-driver \
    dumb-init \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Note: Pinning specific versions requires checking available versions in apt
# For now, we install latest but Selenium will use explicit path to bypass Manager
# Environment variables Selenium expects
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY ["requirements.txt", "./requirements.txt"]
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy the School Scraper directory contents
COPY School\ Scraper/ ./

# Copy startup script that ensures dumb-init runs as PID 1
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh && \
    ls -la /app/start.sh || echo "ERROR: start.sh was not copied!" && \
    test -f /app/start.sh || (echo "ERROR: start.sh does not exist!" && exit 1)

# Verify assets/data/state_counties directory exists
RUN ls -la assets/data/state_counties/ | head -5 || echo "Warning: assets/data/state_counties directory not found"

# Railway listens on port from environment variable (default 8080)
ENV PORT=8080
EXPOSE 8080

# Run the Flask API server with Waitress for production
# Waitress is a pure-Python WSGI server that uses a fixed thread pool, eliminating thread exhaustion issues
# It's designed for production workloads and handles long-running processes better than Flask's dev server
# dumb-init acts as PID 1 and automatically reaps zombie processes (fixes Chrome zombie accumulation)
# Use exec form to ensure dumb-init is PID 1 and properly reaps zombie processes
# The start.sh script provides additional safety checks
ENTRYPOINT ["dumb-init", "--"]
CMD ["/app/start.sh"]

