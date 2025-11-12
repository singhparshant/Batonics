#!/usr/bin/env bash

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Cleanup function
cleanup() {
    log_warn "Shutting down services..."
    if [ -n "${MAIN_PID:-}" ] && kill -0 "$MAIN_PID" 2>/dev/null; then
        log_info "Stopping main server (PID: $MAIN_PID)"
        kill -TERM "$MAIN_PID" 2>/dev/null || true
        wait "$MAIN_PID" 2>/dev/null || true
    fi
    if [ -n "${STREAM_PID:-}" ] && kill -0 "$STREAM_PID" 2>/dev/null; then
        log_info "Stopping TCP stream server (PID: $STREAM_PID)"
        kill -TERM "$STREAM_PID" 2>/dev/null || true
        wait "$STREAM_PID" 2>/dev/null || true
    fi
    log_info "Cleanup complete"
    exit 0
}

# Trap signals
trap cleanup SIGINT SIGTERM EXIT

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || {
    log_error "Failed to change to script directory: $SCRIPT_DIR"
    exit 1
}

log_info "Starting batonics services from $SCRIPT_DIR"

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    log_error "cargo not found. Please install Rust: https://rustup.rs/"
    exit 1
fi

# Build the project
log_info "Building project..."
if ! cargo build --release 2>&1 | grep -E "(error|warning: .*generated [0-9]+ warning)" | head -20; then
    if [ "${PIPESTATUS[0]}" -ne 0 ]; then
        log_error "Build failed. Please check compilation errors above."
        exit 1
    fi
fi
log_info "Build complete"

# Check if binaries exist
MAIN_BIN="./target/release/batonics"
STREAM_BIN="./target/release/stream_tcp"

if [ ! -f "$MAIN_BIN" ]; then
    log_error "Main binary not found at $MAIN_BIN"
    exit 1
fi

if [ ! -f "$STREAM_BIN" ]; then
    log_error "Stream TCP binary not found at $STREAM_BIN"
    exit 1
fi

# Check if DBN file exists
INPUT_FILE="${INPUT_PATH:-CLX5_mbo.dbn}"
if [ ! -f "$INPUT_FILE" ]; then
    log_error "Input file not found: $INPUT_FILE"
    log_error "Set INPUT_PATH environment variable or place CLX5_mbo.dbn in current directory"
    exit 1
fi
log_info "Using input file: $INPUT_FILE"

# Check if postgres is available (optional, warn only)
if ! command -v psql &> /dev/null; then
    log_warn "PostgreSQL client (psql) not found. Storage writer may fail if postgres is not configured."
    log_warn "Install postgres or ensure DATABASE_URL is correctly set."
else
    DB_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost/orderbook_snapshots}"
    log_info "Database URL: $DB_URL"
fi

# Environment defaults
export SERVER_ADDR="${SERVER_ADDR:-127.0.0.1:8080}"
export TCP_BIND_ADDR="${TCP_BIND_ADDR:-127.0.0.1:9090}"
export INPUT_PATH="$INPUT_FILE"

log_info "Configuration:"
log_info "  HTTP Server: $SERVER_ADDR"
log_info "  TCP Stream Server: $TCP_BIND_ADDR"
log_info "  Input File: $INPUT_PATH"
log_info "  Database: ${DATABASE_URL:-postgres://postgres:postgres@localhost/orderbook_snapshots}"

# Start TCP stream server in background
log_info "Starting TCP stream server..."
"$STREAM_BIN" > >(sed 's/^/[stream_tcp] /') 2> >(sed 's/^/[stream_tcp] /' >&2) &
STREAM_PID=$!

if ! kill -0 "$STREAM_PID" 2>/dev/null; then
    log_error "Failed to start TCP stream server"
    exit 1
fi
log_info "TCP stream server started (PID: $STREAM_PID)"

# Give stream server a moment to initialize
sleep 1

if ! kill -0 "$STREAM_PID" 2>/dev/null; then
    log_error "TCP stream server died immediately after start. Check logs above."
    exit 1
fi

# Start main server in background
log_info "Starting main orderbook server..."
"$MAIN_BIN" > >(sed 's/^/[main] /') 2> >(sed 's/^/[main] /' >&2) &
MAIN_PID=$!

if ! kill -0 "$MAIN_PID" 2>/dev/null; then
    log_error "Failed to start main server"
    cleanup
    exit 1
fi
log_info "Main orderbook server started (PID: $MAIN_PID)"

# Give main server a moment to initialize
sleep 2

if ! kill -0 "$MAIN_PID" 2>/dev/null; then
    log_error "Main server died immediately after start. Check logs above."
    cleanup
    exit 1
fi

log_info "All services started successfully!"
log_info ""
log_info "Services running:"
log_info "  - Main HTTP API: http://$SERVER_ADDR/snapshot"
log_info "  - Health check: http://$SERVER_ADDR/healthz"
log_info "  - TCP Stream: $TCP_BIND_ADDR"
log_info ""
log_info "Press Ctrl+C to stop all services"

# Monitor processes
while true; do
    if ! kill -0 "$MAIN_PID" 2>/dev/null; then
        log_error "Main server (PID: $MAIN_PID) has stopped unexpectedly"
        wait "$MAIN_PID" 2>/dev/null || EXIT_CODE=$?
        log_error "Main server exit code: ${EXIT_CODE:-unknown}"
        cleanup
        exit 1
    fi
    
    if ! kill -0 "$STREAM_PID" 2>/dev/null; then
        log_error "TCP stream server (PID: $STREAM_PID) has stopped unexpectedly"
        wait "$STREAM_PID" 2>/dev/null || EXIT_CODE=$?
        log_error "TCP stream server exit code: ${EXIT_CODE:-unknown}"
        cleanup
        exit 1
    fi
    
    sleep 5
done
