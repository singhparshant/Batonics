use std::{
    env, fs,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use crossbeam_channel::Sender;
use dbn::{
    decode::{DecodeRecord, dbn::Decoder},
    record::MboMsg,
};

use batonics::{
    order_book::Market,
    server::{ServerConfig, spawn_http_server},
    snapshot::{
        DEFAULT_TOP_LEVELS, SharedSnapshot, SnapshotRecord, build_full_snapshot_record,
        build_snapshot_record,
    },
    storage::{StorageConfig, spawn_writer},
};

fn main() -> Result<()> {
    let config = AppConfig::from_env()?;
    let (tx, rx) = crossbeam_channel::bounded::<SharedSnapshot>(config.queue_capacity);
    let latest: Arc<ArcSwapOption<SnapshotRecord>> = Arc::new(ArcSwapOption::empty());

    let storage_handle = spawn_writer(
        StorageConfig::new(
            config.db_url.clone(),
            config.batch_size,
            config.flush_interval,
        ),
        rx,
    );

    let server_handle = spawn_http_server(
        latest.clone(),
        ServerConfig {
            addr: config.server_addr,
        },
    );

    run_ingest(&config, tx, latest.clone())?;

    // Wait for persistence to drain
    let storage_result = storage_handle
        .join()
        .expect("storage writer thread panicked");
    storage_result?;

    // Keep serving snapshots until ctrl+c
    let server_result = server_handle.join().expect("server thread panicked");
    server_result?;

    Ok(())
}

fn run_ingest(
    config: &AppConfig,
    tx: Sender<SharedSnapshot>,
    latest: Arc<ArcSwapOption<SnapshotRecord>>,
) -> Result<()> {
    let start = Instant::now();
    let mut decoder = Decoder::from_file(&config.input_path)
        .with_context(|| format!("failed to open DBN file {}", config.input_path))?;

    let mut market = Market::new();
    let mut msg_count: u64 = 0;
    let mut skipped_count: u64 = 0;
    let mut apply_durations_ns: Vec<u64> = Vec::new();
    let mut total_apply_ns: u128 = 0;
    let mut last_ts_ns: i64 = 0;
    let mut last_instrument: u32 = 0;

    loop {
        let rec = match decoder.decode_record::<MboMsg>() {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(e) => {
                eprintln!("decode_error: {} (continuing)", e);
                continue;
            }
        };

        last_ts_ns = rec.hd.ts_event as i64;
        last_instrument = rec.hd.instrument_id;
        let t0 = Instant::now();

        let applied = market.apply(rec.clone());

        // Only generate and persist snapshot if the message was successfully applied
        if applied {
            let snapshot = build_snapshot_record(
                &market,
                rec.hd.instrument_id,
                &config.symbol,
                last_ts_ns,
                config.depth,
            );

            let shared = Arc::new(snapshot);
            latest.store(Some(shared.clone()));
            if tx.send(shared).is_err() {
                eprintln!("snapshot_queue_closed, stopping ingest");
                break;
            }
        } else {
            skipped_count += 1;
        }

        let dt = t0.elapsed().as_nanos() as u64;
        total_apply_ns += dt as u128;
        apply_durations_ns.push(dt);
        msg_count += 1;
    }

    drop(tx);

    emit_metrics(
        start.elapsed(),
        msg_count,
        total_apply_ns,
        apply_durations_ns,
    );
    println!(
        "ingest_complete instrument_id={} last_ts={} processed={} skipped={}",
        last_instrument, last_ts_ns, msg_count, skipped_count
    );

    if msg_count > 0 {
        let final_snapshot =
            build_full_snapshot_record(&market, last_instrument, &config.symbol, last_ts_ns);
        if let Err(e) = write_final_snapshot(&final_snapshot) {
            eprintln!("failed to write final_result.json: {}", e);
        }
    }

    Ok(())
}

fn write_final_snapshot(snapshot: &SnapshotRecord) -> Result<()> {
    let json = serde_json::to_string_pretty(&snapshot.payload)
        .context("failed to serialize final snapshot to json")?;
    fs::write("final_result.json", json).context("failed to write final_result.json")?;
    Ok(())
}

fn emit_metrics(
    elapsed: Duration,
    msg_count: u64,
    total_apply_ns: u128,
    mut apply_durations_ns: Vec<u64>,
) {
    let avg_ns = if msg_count > 0 {
        (total_apply_ns as f64) / (msg_count as f64)
    } else {
        0.0
    };
    let p99_ns = if !apply_durations_ns.is_empty() {
        let n = apply_durations_ns.len();
        let mut idx = (n * 99 + 99) / 100; // ceil(0.99 * n)
        if idx == 0 {
            idx = 1;
        }
        if idx > n {
            idx = n;
        }
        apply_durations_ns.select_nth_unstable(idx - 1);
        apply_durations_ns[idx - 1]
    } else {
        0
    };
    let message_throughput = if elapsed.as_secs_f64() > 0.0 {
        (msg_count as f64) / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let order_processing_rate = if avg_ns > 0.0 { 1e9f64 / avg_ns } else { 0.0 };
    println!(
        "metrics={{\"messagesProcessed\":{},\"averageOrderProcessNs\":{},\"p99OrderProcessNs\":{},\"orderProcessingRate\":{},\"messageThroughput\":{},\"elapsedNs\":{}}}",
        msg_count,
        avg_ns,
        p99_ns,
        order_processing_rate,
        message_throughput,
        elapsed.as_nanos()
    );
}

#[derive(Clone)]
struct AppConfig {
    input_path: String,
    symbol: String,
    queue_capacity: usize,
    batch_size: usize,
    flush_interval: Duration,
    depth: usize,
    db_url: Arc<String>,
    server_addr: SocketAddr,
}

impl AppConfig {
    fn from_env() -> Result<Self> {
        let input_path = env::var("INPUT_PATH").unwrap_or_else(|_| String::from("CLX5_mbo.dbn"));
        let symbol = env::var("SYMBOL").unwrap_or_else(|_| String::from("CLX5"));
        let queue_capacity = env::var("QUEUE_CAPACITY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000);
        let batch_size = env::var("SNAPSHOT_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000);
        let flush_ms = env::var("SNAPSHOT_FLUSH_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_u64);
        let depth = env::var("SNAPSHOT_DEPTH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_TOP_LEVELS);
        let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
            String::from("postgres://postgres:postgres@localhost/orderbook_snapshots")
        });
        if !db_url.contains("orderbook_snapshots") {
            eprintln!("warn: DATABASE_URL does not reference database named orderbook_snapshots");
        }
        let server_addr = env::var("SERVER_ADDR")
            .unwrap_or_else(|_| String::from("127.0.0.1:8080"))
            .parse()
            .context("SERVER_ADDR must be a valid socket address, e.g. 127.0.0.1:8080")?;

        Ok(Self {
            input_path,
            symbol,
            queue_capacity,
            batch_size,
            flush_interval: Duration::from_millis(flush_ms),
            depth: depth.max(1),
            db_url: Arc::new(db_url),
            server_addr,
        })
    }
}
