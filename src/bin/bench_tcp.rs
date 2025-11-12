use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bytes::BytesMut;
use prost::Message;
use tokio::{io::AsyncReadExt, net::TcpStream, time::sleep};

// Include generated protobuf types
mod proto {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use proto::MboBatch;

const DEFAULT_SERVER: &str = "127.0.0.1:9090";

#[derive(Clone, Debug)]
struct BenchConfig {
    server_addr: String,
    duration_secs: u64,
}

impl BenchConfig {
    fn from_env() -> Result<Self> {
        let server_addr = env::var("BENCH_SERVER").unwrap_or_else(|_| DEFAULT_SERVER.to_string());
        let duration_secs = env::var("BENCH_DURATION")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);

        Ok(Self {
            server_addr,
            duration_secs,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = BenchConfig::from_env()?;

    eprintln!("tcp_bench connecting to {}", config.server_addr);
    eprintln!("duration: {}s\n", config.duration_secs);

    let mut stream = TcpStream::connect(&config.server_addr)
        .await
        .with_context(|| format!("failed to connect to {}", config.server_addr))?;

    eprintln!("connected, starting benchmark...\n");

    let msg_counter = Arc::new(AtomicU64::new(0));
    let batch_counter = Arc::new(AtomicU64::new(0));
    let bytes_counter = Arc::new(AtomicU64::new(0));

    let msg_counter_clone = Arc::clone(&msg_counter);
    let batch_counter_clone = Arc::clone(&batch_counter);
    let bytes_counter_clone = Arc::clone(&bytes_counter);

    // Spawn stats reporter
    let stats_handle = tokio::spawn(async move {
        let mut last_report = Instant::now();
        let mut last_msgs = 0u64;
        let mut last_batches = 0u64;
        let mut last_bytes = 0u64;

        loop {
            sleep(Duration::from_secs(1)).await;

            let total_msgs = msg_counter_clone.load(Ordering::Relaxed);
            let total_batches = batch_counter_clone.load(Ordering::Relaxed);
            let total_bytes = bytes_counter_clone.load(Ordering::Relaxed);

            let interval = last_report.elapsed().as_secs_f64();
            let msgs_delta = total_msgs.saturating_sub(last_msgs);
            let batches_delta = total_batches.saturating_sub(last_batches);
            let bytes_delta = total_bytes.saturating_sub(last_bytes);

            let msg_rate = msgs_delta as f64 / interval;
            let batch_rate = batches_delta as f64 / interval;
            let throughput_mbps = (bytes_delta as f64 / interval) / (1024.0 * 1024.0);

            eprintln!(
                "msgs={} batches={} msg_rate={:.0}/s batch_rate={:.0}/s throughput={:.2}MB/s",
                total_msgs, total_batches, msg_rate, batch_rate, throughput_mbps
            );

            last_msgs = total_msgs;
            last_batches = total_batches;
            last_bytes = total_bytes;
            last_report = Instant::now();
        }
    });

    // Read loop
    let bench_start = Instant::now();
    let mut read_buf = BytesMut::with_capacity(1024 * 1024); // 1MB buffer

    while bench_start.elapsed() < Duration::from_secs(config.duration_secs) {
        // Read length prefix (4-byte u32 big-endian)
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf).await {
            eprintln!("read error: {}", e);
            break;
        }

        let frame_len = u32::from_be_bytes(len_buf) as usize;
        bytes_counter.fetch_add(4, Ordering::Relaxed);

        // Read protobuf frame
        read_buf.clear();
        read_buf.resize(frame_len, 0);
        if let Err(e) = stream.read_exact(&mut read_buf).await {
            eprintln!("read frame error: {}", e);
            break;
        }
        bytes_counter.fetch_add(frame_len as u64, Ordering::Relaxed);
        let flag = false;
        // Decode batch
        match MboBatch::decode(&mut read_buf.as_ref()) {
            Ok(batch) => {
                let msg_count = batch.msgs.len() as u64;
                msg_counter.fetch_add(msg_count, Ordering::Relaxed);
                batch_counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("decode error: {}", e);
                break;
            }
        }
    }

    stats_handle.abort();

    // Final stats
    let elapsed = bench_start.elapsed().as_secs_f64();
    let total_msgs = msg_counter.load(Ordering::Relaxed);
    let total_batches = batch_counter.load(Ordering::Relaxed);
    let total_bytes = bytes_counter.load(Ordering::Relaxed);

    eprintln!("\nbench_complete");
    eprintln!("===============");
    eprintln!("Duration:        {:.2}s", elapsed);
    eprintln!("Total Messages:  {}", total_msgs);
    eprintln!("Total Batches:   {}", total_batches);
    eprintln!(
        "Total Bytes:     {} ({:.2}MB)",
        total_bytes,
        total_bytes as f64 / (1024.0 * 1024.0)
    );
    eprintln!();
    eprintln!("Avg Msg Rate:    {:.0} msg/s", total_msgs as f64 / elapsed);
    eprintln!(
        "Avg Batch Rate:  {:.0} batch/s",
        total_batches as f64 / elapsed
    );
    eprintln!(
        "Avg Throughput:  {:.2} MB/s",
        total_bytes as f64 / elapsed / (1024.0 * 1024.0)
    );

    Ok(())
}
