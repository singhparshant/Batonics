# Starting Batonics Services

## Quick Start

```bash
./start_services.sh
```

This will start both the main orderbook server and the TCP stream server.

## What It Does

The script performs the following:

1. **Validates Environment**
   - Checks for Rust/cargo installation
   - Verifies input DBN file exists
   - Warns if PostgreSQL is not available

2. **Builds Project**
   - Runs `cargo build --release`
   - Reports any compilation errors

3. **Starts Services**
   - **TCP Stream Server** (`stream_tcp`) on port 9090
   - **Main Orderbook Server** (`batonics`) on port 8080
   - Both run with automatic log prefixing

4. **Monitors Health**
   - Continuously checks both processes
   - Automatically restarts on unexpected exit
   - Clean shutdown on Ctrl+C

## Configuration

(Optional) Set environment variables before running:

```bash
# Required
export INPUT_PATH="CLX5_mbo.dbn"              # Path to DBN file

# Optional (defaults shown)
export SERVER_ADDR="127.0.0.1:8080"           # HTTP API address
export TCP_BIND_ADDR="127.0.0.1:9090"         # TCP stream address
export DATABASE_URL="postgres://postgres:postgres@localhost/orderbook_snapshots"
export SNAPSHOT_BATCH_SIZE="5000"             # DB write batch size
export SNAPSHOT_FLUSH_MS="10"                 # DB flush interval
export SNAPSHOT_DEPTH="10"                    # Orderbook depth
export QUEUE_CAPACITY="1000000"               # Snapshot queue size
```

## Accessing Services

Once running:

- **HTTP API**: http://localhost:8080/snapshot
- **Health Check**: http://localhost:8080/healthz
- **TCP Stream**: Connect to localhost:9090

## Log Output

Logs are prefixed by service:

```
[main] storage_writer connected to postgres
[main] server_ready addr=127.0.0.1:8080
[stream_tcp] tcp_streamer listening on 127.0.0.1:9090
[stream_tcp] client_connected id=0 addr=127.0.0.1:54321
[main] storage_writer flushed batch size=5000 total=5000
```

## Error Handling

The script handles errors gracefully:

- **Build Failure**: Shows compilation errors and exits
- **Missing Input File**: Shows clear error with file path
- **Service Crash**: Logs exit code and stops all services
- **Connection Issues**: Storage writer logs detailed DB connection errors

## Stopping Services

Press `Ctrl+C` to gracefully stop all services. The script will:

1. Send SIGTERM to both processes
2. Wait for clean shutdown
3. Report completion

## Troubleshooting

### "cargo not found"
Install Rust: https://rustup.rs/

### "Input file not found"
```bash
# Check file exists
ls -l CLX5_mbo.dbn

# Or specify path
INPUT_PATH="/path/to/file.dbn" ./start_services.sh
```

### "Failed to connect to postgres"
```bash
# Start postgres locally
brew services start postgresql  # macOS
sudo systemctl start postgresql # Linux

# Or use remote database
DATABASE_URL="postgres://host/db" ./start_services.sh
```

### "Port already in use"
```bash
# Use different ports
SERVER_ADDR="127.0.0.1:8081" TCP_BIND_ADDR="127.0.0.1:9091" ./start_services.sh
```

### Service dies immediately
Check the prefixed logs above the error for context:
```
[main] storage_writer failed to connect to postgres: connection refused
[ERROR] Main server (PID: 12345) has stopped unexpectedly
```

## Advanced: Running Services Separately

If you need to run services independently:

```bash
# Build first
cargo build --release

# Run main server
./target/release/batonics

# Run TCP stream server (in another terminal)
./target/release/stream_tcp
```
