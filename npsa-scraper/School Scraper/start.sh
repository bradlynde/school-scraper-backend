#!/bin/bash
# Startup script that ensures dumb-init runs as PID 1
# This script is executed by the container and ensures proper process reaping

# Check if this is a restart (after a run completed)
# We detect this by checking if there's a marker file from a previous run
RESTART_MARKER="/tmp/run_completed_marker"
if [ -f "$RESTART_MARKER" ]; then
    echo "Restart successful"
    rm -f "$RESTART_MARKER"
else
    echo "Launch successful"
fi

# Find dumb-init in common locations
DUMB_INIT_PATH=""
for path in /usr/bin/dumb-init /usr/local/bin/dumb-init /bin/dumb-init; do
    if [ -x "$path" ]; then
        DUMB_INIT_PATH="$path"
        break
    fi
done

# Also try which if available
if [ -z "$DUMB_INIT_PATH" ] && command -v dumb-init >/dev/null 2>&1; then
    DUMB_INIT_PATH=$(command -v dumb-init)
fi

# If we're PID 1 and we're not dumb-init, exec into dumb-init
CURRENT_PID=$$
PID1_NAME=$(ps -p 1 -o comm= 2>/dev/null || echo "unknown")
if [ "$CURRENT_PID" = "1" ] && [ "$PID1_NAME" != "dumb-init" ] && [ -n "$DUMB_INIT_PATH" ]; then
        exec "$DUMB_INIT_PATH" -- "$0" "$@"
fi

# If PID 1 is not dumb-init and we're not PID 1, try to exec into dumb-init
if [ "$PID1_NAME" != "dumb-init" ] && [ "$CURRENT_PID" != "1" ] && [ -n "$DUMB_INIT_PATH" ]; then
        exec "$DUMB_INIT_PATH" -- "$0" "$@"
fi

# Get port from environment variable (default 8080)
PORT=${PORT:-8080}

# Exec waitress-serve (exec replaces this shell process, so waitress becomes child of dumb-init)
# This ensures waitress is a direct child of dumb-init for proper signal handling
exec waitress-serve --host=0.0.0.0 --port=$PORT --threads=4 --channel-timeout=300 external_services.api:app
