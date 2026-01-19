#!/bin/bash
# Startup script that ensures dumb-init runs as PID 1
# This script is executed by the container and ensures proper process reaping

# Diagnostic: Check what PID 1 is
PID1_NAME=$(ps -p 1 -o comm= 2>/dev/null || echo "unknown")
CURRENT_PID=$$

echo "[STARTUP] Current PID: $$, PID 1: $PID1_NAME"

# If we're PID 1 and we're not dumb-init, something is wrong
# This means Railway overrode the ENTRYPOINT
if [ "$CURRENT_PID" = "1" ] && [ "$PID1_NAME" != "dumb-init" ]; then
    echo "[STARTUP] ERROR: Running as PID 1 but not dumb-init! Execing into dumb-init..."
    # Exec into dumb-init, which will then run this script again
    exec dumb-init -- "$0" "$@"
fi

# If PID 1 is not dumb-init and we're not PID 1, try to exec into dumb-init
# (This shouldn't happen if ENTRYPOINT is correct, but handle it)
if [ "$PID1_NAME" != "dumb-init" ] && [ "$CURRENT_PID" != "1" ]; then
    echo "[STARTUP] WARNING: PID 1 is not dumb-init, attempting to exec into dumb-init..."
    exec dumb-init -- "$0" "$@"
fi

# Get port from environment variable (default 8080)
PORT=${PORT:-8080}

echo "[STARTUP] Starting waitress-serve on port $PORT (PID 1: $PID1_NAME)"

# Exec waitress-serve (exec replaces this shell process, so waitress becomes child of dumb-init)
# This ensures waitress is a direct child of dumb-init for proper signal handling
exec waitress-serve --host=0.0.0.0 --port=$PORT --threads=4 --channel-timeout=300 external_services.api:app
