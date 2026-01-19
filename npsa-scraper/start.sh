#!/bin/bash
# Startup script that ensures dumb-init runs as PID 1
# This script is executed by the container and ensures proper process reaping

# Diagnostic: Check what PID 1 is
PID1_NAME=$(ps -p 1 -o comm= 2>/dev/null || echo "unknown")
CURRENT_PID=$$

echo "[STARTUP] Current PID: $$, PID 1: $PID1_NAME"

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

# If we're PID 1 and we're not dumb-init, something is wrong
# This means Railway overrode the ENTRYPOINT
if [ "$CURRENT_PID" = "1" ] && [ "$PID1_NAME" != "dumb-init" ]; then
    echo "[STARTUP] ERROR: Running as PID 1 but not dumb-init! Execing into dumb-init..."
    if [ -n "$DUMB_INIT_PATH" ]; then
        echo "[STARTUP] Found dumb-init at $DUMB_INIT_PATH"
        # Exec into dumb-init, which will then run this script again
        exec "$DUMB_INIT_PATH" -- "$0" "$@"
    else
        echo "[STARTUP] ERROR: dumb-init not found! Searched: /usr/bin/dumb-init /usr/local/bin/dumb-init /bin/dumb-init"
        echo "[STARTUP] Process reaping will not work correctly. Continuing anyway..."
    fi
fi

# If PID 1 is not dumb-init and we're not PID 1, try to exec into dumb-init
# (This shouldn't happen if ENTRYPOINT is correct, but handle it)
if [ "$PID1_NAME" != "dumb-init" ] && [ "$CURRENT_PID" != "1" ]; then
    echo "[STARTUP] WARNING: PID 1 is not dumb-init, attempting to exec into dumb-init..."
    if [ -n "$DUMB_INIT_PATH" ]; then
        exec "$DUMB_INIT_PATH" -- "$0" "$@"
    else
        echo "[STARTUP] WARNING: dumb-init not found, cannot fix PID 1 issue"
    fi
fi

# Get port from environment variable (default 8080)
PORT=${PORT:-8080}

echo "[STARTUP] Starting waitress-serve on port $PORT (PID 1: $PID1_NAME)"

# Exec waitress-serve (exec replaces this shell process, so waitress becomes child of dumb-init)
# This ensures waitress is a direct child of dumb-init for proper signal handling
exec waitress-serve --host=0.0.0.0 --port=$PORT --threads=4 --channel-timeout=300 external_services.api:app
