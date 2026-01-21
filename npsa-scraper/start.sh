#!/bin/bash
# Startup script that ensures dumb-init runs as PID 1
# This script is executed by the container and ensures proper process reaping

# Force diagnostic output to both stdout and stderr so it appears in logs
# Railway may capture either, so send to both
exec > >(tee -a /proc/1/fd/1 /proc/1/fd/2) 2>&1

# Diagnostic: Check what PID 1 is
PID1_NAME=$(ps -p 1 -o comm= 2>/dev/null || echo "unknown")
CURRENT_PID=$$

echo "[STARTUP] Current PID: $$, PID 1: $PID1_NAME"

# Find dumb-init in common locations
DUMB_INIT_PATH=""
for path in /usr/bin/dumb-init /usr/local/bin/dumb-init /bin/dumb-init; do
    if [ -x "$path" ]; then
        DUMB_INIT_PATH="$path"
        echo "[STARTUP] Found dumb-init at $DUMB_INIT_PATH"
        break
    fi
done

# Also try which if available
if [ -z "$DUMB_INIT_PATH" ] && command -v dumb-init >/dev/null 2>&1; then
    DUMB_INIT_PATH=$(command -v dumb-init)
    echo "[STARTUP] Found dumb-init via which: $DUMB_INIT_PATH"
fi

# Verify dumb-init exists and is executable
if [ -z "$DUMB_INIT_PATH" ]; then
    echo "[STARTUP] ERROR: dumb-init not found! Searched: /usr/bin/dumb-init /usr/local/bin/dumb-init /bin/dumb-init"
    echo "[STARTUP] Attempting to verify installation..."
    dpkg -l | grep dumb-init || echo "[STARTUP] dumb-init not in package list"
    ls -la /usr/bin/dumb-init /usr/local/bin/dumb-init /bin/dumb-init 2>&1 || true
    echo "[STARTUP] Process reaping will not work correctly. Continuing anyway..."
else
    echo "[STARTUP] Verified dumb-init exists and is executable: $DUMB_INIT_PATH"
    # Test execution
    $DUMB_INIT_PATH --version 2>&1 || echo "[STARTUP] WARNING: dumb-init exists but cannot execute"
fi

# CRITICAL FIX: If PID 1 is not dumb-init, we MUST exec into dumb-init
# This handles cases where Railway overrides ENTRYPOINT or runs container differently
if [ "$PID1_NAME" != "dumb-init" ]; then
    echo "[STARTUP] CRITICAL: PID 1 is '$PID1_NAME', not 'dumb-init'!"
    if [ -n "$DUMB_INIT_PATH" ]; then
        echo "[STARTUP] Execing into dumb-init to fix PID 1..."
        echo "[STARTUP] This will restart the script with dumb-init as parent"
        # Exec into dumb-init, which will then run this script again as a child
        # This ensures dumb-init becomes the effective init system
        exec "$DUMB_INIT_PATH" -- "$0" "$@"
    else
        echo "[STARTUP] ERROR: Cannot exec into dumb-init - not found!"
        echo "[STARTUP] Zombie processes may accumulate. This is a critical issue."
    fi
fi

# If we reach here, dumb-init should be PID 1
# Verify one more time
PID1_NAME_AFTER=$(ps -p 1 -o comm= 2>/dev/null || echo "unknown")
if [ "$PID1_NAME_AFTER" != "dumb-init" ]; then
    echo "[STARTUP] WARNING: After checks, PID 1 is still '$PID1_NAME_AFTER', not 'dumb-init'"
    echo "[STARTUP] Process reaping may not work correctly"
fi

# Get port from environment variable (default 8080)
PORT=${PORT:-8080}

echo "[STARTUP] Starting waitress-serve on port $PORT"
echo "[STARTUP] Final check - PID 1: $(ps -p 1 -o comm= 2>/dev/null || echo 'unknown'), Current PID: $$"

# Exec waitress-serve (exec replaces this shell process, so waitress becomes child of dumb-init)
# This ensures waitress is a direct child of dumb-init for proper signal handling
exec waitress-serve --host=0.0.0.0 --port=$PORT --threads=4 --channel-timeout=300 external_services.api:app
