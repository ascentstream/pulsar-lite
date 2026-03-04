#!/bin/bash
# Pulsar Lite Service Management Script
# Usage: ./pulsar-lite.sh {start|stop|restart|status}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="${SCRIPT_DIR}/target/release/pulsar-lite"
PID_FILE="/tmp/pulsar-lite.pid"
LOG_FILE="/tmp/pulsar-lite.log"
CONFIG_FILE="${SCRIPT_DIR}/pulsar-lite.toml"

# Log level configuration
export RUST_LOG=${RUST_LOG:-info}

start() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo "Pulsar Lite is already running (PID: $pid)"
            return 1
        fi
    fi

    if [ ! -f "$BINARY" ]; then
        echo "Error: Binary not found at $BINARY"
        echo "Please build the project first: cd rust && cargo build --release"
        return 1
    fi

    echo "Starting Pulsar Lite..."
    echo "Log file: $LOG_FILE"
    echo "Log level: $RUST_LOG"
    echo "Config file: $CONFIG_FILE"

    # Change to script directory to ensure relative paths work
    cd "$SCRIPT_DIR"

    # Start service in background
    nohup "$BINARY" > "$LOG_FILE" 2>&1 &
    local pid=$!

    # Save PID
    echo $pid > "$PID_FILE"

    # Wait a moment and check if process is still running
    sleep 1
    if ps -p "$pid" > /dev/null 2>&1; then
        echo "Pulsar Lite started successfully (PID: $pid)"
        echo "Use 'tail -f $LOG_FILE' to view logs"
    else
        echo "Failed to start Pulsar Lite. Check logs at $LOG_FILE"
        rm -f "$PID_FILE"
        return 1
    fi
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "Pulsar Lite is not running (no PID file found)"
        return 1
    fi

    local pid=$(cat "$PID_FILE")

    if ! ps -p "$pid" > /dev/null 2>&1; then
        echo "Pulsar Lite is not running (PID: $pid)"
        rm -f "$PID_FILE"
        return 1
    fi

    echo "Stopping Pulsar Lite (PID: $pid)..."
    kill "$pid"

    # Wait for process to terminate
    local count=0
    while ps -p "$pid" > /dev/null 2>&1; do
        sleep 1
        count=$((count + 1))
        if [ $count -eq 10 ]; then
            echo "Process didn't stop gracefully, forcing kill..."
            kill -9 "$pid"
            break
        fi
    done

    rm -f "$PID_FILE"
    echo "Pulsar Lite stopped successfully"
}

restart() {
    echo "Restarting Pulsar Lite..."
    stop
    sleep 1
    start
}

status() {
    if [ ! -f "$PID_FILE" ]; then
        echo "Pulsar Lite is not running (no PID file found)"
        return 1
    fi

    local pid=$(cat "$PID_FILE")

    if ps -p "$pid" > /dev/null 2>&1; then
        echo "Pulsar Lite is running (PID: $pid)"
        echo "Log file: $LOG_FILE"
        echo ""
        echo "Recent logs:"
        tail -n 5 "$LOG_FILE"
    else
        echo "Pulsar Lite is not running (stale PID file found)"
        rm -f "$PID_FILE"
        return 1
    fi
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        echo ""
        echo "Environment variables:"
        echo "  RUST_LOG - Log level (default: info)"
        echo "             Options: error, warn, info, debug, trace"
        echo ""
        echo "Examples:"
        echo "  $0 start                  # Start with info log level"
        echo "  RUST_LOG=debug $0 start   # Start with debug log level"
        echo "  $0 status                 # Check status and view recent logs"
        echo "  $0 stop                   # Stop the service"
        exit 1
        ;;
esac
