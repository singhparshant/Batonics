use std::{
    env, fs,
    io::{BufWriter, ErrorKind, Write},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use bytes::{BufMut, BytesMut};
use dbn::{
    decode::{DecodeRecord, dbn::Decoder},
    record::MboMsg as DbnMboMsg,
};
use prost::Message;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

// Include generated protobuf types
mod proto {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use proto::{Header, MboBatch, MboMsg};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:9090";
const BATCH_SIZE: usize = 1000; // Messages per protobuf batch
const MAX_BATCH_BYTES: usize = 512 * 1024; // 512KB max batch size

#[derive(Clone, Debug)]
struct StreamConfig {
    bind_addr: String,
    input_path: String,
    encoded_path: String,
    loop_replay: bool,
    batch_size: usize,
    preencode: bool,
}

impl StreamConfig {
    fn from_env() -> Result<Self> {
        let bind_addr = env::var("TCP_BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
        let input_path = env::var("INPUT_PATH").unwrap_or_else(|_| String::from("CLX5_mbo.dbn"));
        let loop_replay = env::var("TCP_LOOP_REPLAY")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let batch_size = env::var("TCP_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(BATCH_SIZE);
        let encoded_path = env::var("ENCODED_PATH").unwrap_or_else(|_| String::from("mbo.frames"));
        let preencode = env::var("PREENCODE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        Ok(Self {
            bind_addr,
            input_path,
            encoded_path,
            loop_replay,
            batch_size,
            preencode,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = StreamConfig::from_env()?;

    eprintln!(
        "tcp_streamer bind={} batch_size={} loop={} input={} encoded={} preencode={}",
        config.bind_addr,
        config.batch_size,
        config.loop_replay,
        config.input_path,
        config.encoded_path,
        config.preencode
    );

    if config.preencode {
        eprintln!(
            "preencoding DBN file {} -> {}",
            config.input_path, config.encoded_path
        );
        let start = Instant::now();
        let stats = preencode_to_file(&config.input_path, &config.encoded_path, config.batch_size)?;
        let elapsed = start.elapsed();
        let total_msgs = stats.batches * config.batch_size;
        eprintln!(
            "preencoded {} batches (~{} msgs, {:.2}MB) in {:.2}s",
            stats.batches,
            total_msgs,
            stats.bytes as f64 / (1024.0 * 1024.0),
            elapsed.as_secs_f64()
        );
    } else {
        let metadata = fs::metadata(&config.encoded_path).with_context(|| {
            format!(
                "encoded file {} not found (set PREENCODE=1 to rebuild)",
                config.encoded_path
            )
        })?;
        eprintln!(
            "using existing encoded file {} ({:.2}MB)",
            config.encoded_path,
            metadata.len() as f64 / (1024.0 * 1024.0)
        );
    }

    // Accept connections
    let listener = TcpListener::bind(&config.bind_addr)
        .await
        .with_context(|| format!("failed to bind TCP listener to {}", config.bind_addr))?;

    eprintln!("tcp_streamer listening on {}", config.bind_addr);

    let mut client_id = 0u64;
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, addr)) => {
                        let cfg = config.clone();
                        let id = client_id;
                        client_id += 1;
                        tokio::spawn(async move {
                            handle_client(id, socket, addr.to_string(), cfg).await;
                        });
                    }
                    Err(e) => {
                        eprintln!("tcp_streamer accept error: {}", e);
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("tcp_streamer shutting down");
                break;
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
struct PreencodeStats {
    batches: usize,
    bytes: u64,
}

fn preencode_to_file(
    input_path: &str,
    encoded_path: &str,
    batch_size: usize,
) -> Result<PreencodeStats> {
    let mut decoder = Decoder::from_file(input_path)
        .with_context(|| format!("failed to open DBN input {}", input_path))?;
    let file = fs::File::create(encoded_path)
        .with_context(|| format!("failed to create encoded output {}", encoded_path))?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    let mut batch_msgs = Vec::with_capacity(batch_size);
    let mut batches_written = 0usize;

    loop {
        match decoder.decode_record::<DbnMboMsg>() {
            Ok(Some(dbn_msg)) => {
                let proto_msg = convert_to_proto(&dbn_msg);
                batch_msgs.push(proto_msg);

                if batch_msgs.len() >= batch_size {
                    let batch = MboBatch {
                        msgs: batch_msgs.clone(),
                    };
                    let encoded = encode_batch(&batch)?;
                    writer.write_all(&encoded)?;
                    batch_msgs.clear();
                    batches_written += 1;
                }
            }
            Ok(None) => {
                if !batch_msgs.is_empty() {
                    let batch = MboBatch {
                        msgs: batch_msgs.clone(),
                    };
                    let encoded = encode_batch(&batch)?;
                    writer.write_all(&encoded)?;
                    batch_msgs.clear();
                    batches_written += 1;
                }
                break;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    writer.flush()?;
    drop(writer);

    let bytes = fs::metadata(encoded_path)
        .with_context(|| format!("failed to stat encoded output {}", encoded_path))?
        .len();

    Ok(PreencodeStats {
        batches: batches_written,
        bytes,
    })
}

// Convert DBN MboMsg to protobuf MboMsg
fn convert_to_proto(dbn_msg: &DbnMboMsg) -> MboMsg {
    let hd = &dbn_msg.hd;

    MboMsg {
        hd: Some(Header {
            ts_event: hd.ts_event,
            rtype: hd.rtype as u32,
            publisher_id: hd.publisher_id as u32,
            instrument_id: hd.instrument_id,
        }),
        action: action_to_string(dbn_msg.action as u8 as char),
        side: side_to_string(dbn_msg.side as u8 as char),
        price: dbn_msg.price as u64,
        size: dbn_msg.size,
        channel_id: dbn_msg.channel_id as u32,
        order_id: dbn_msg.order_id,
        flags: dbn_msg.flags.raw() as u32,
        ts_in_delta: dbn_msg.ts_in_delta as u64,
        sequence: dbn_msg.sequence as u64,
    }
}

#[inline]
fn action_to_string(action: char) -> String {
    match action {
        'A' => "A".to_string(),
        'M' => "M".to_string(),
        'D' => "D".to_string(),
        'C' => "C".to_string(),
        'T' => "T".to_string(),
        _ => action.to_string(),
    }
}

#[inline]
fn side_to_string(side: char) -> String {
    match side {
        'A' => "A".to_string(),
        'B' => "B".to_string(),
        _ => side.to_string(),
    }
}

// Encode a batch with length prefix: [u32 length][protobuf bytes]
fn encode_batch(batch: &MboBatch) -> Result<Vec<u8>> {
    let encoded_len = batch.encoded_len();
    if encoded_len > MAX_BATCH_BYTES {
        bail!(
            "batch encoded length {} exceeds max {} bytes",
            encoded_len,
            MAX_BATCH_BYTES
        );
    }
    let mut buf = BytesMut::with_capacity(encoded_len + 4);

    // Write 4-byte length prefix (big-endian u32)
    buf.put_u32(encoded_len as u32);

    // Write protobuf-encoded batch
    batch.encode(&mut buf)?;

    Ok(buf.to_vec())
}

async fn handle_client(id: u64, mut socket: TcpStream, addr: String, config: StreamConfig) {
    eprintln!("client_connected id={} addr={}", id, addr);
    if let Err(e) = socket.set_nodelay(true) {
        eprintln!("client_{} failed to enable TCP_NODELAY: {}", id, e);
    }

    let start = Instant::now();
    let mut total_msgs_sent = 0u64;
    let mut total_batches_sent = 0u64;
    let mut total_bytes_sent = 0u64;
    let mut last_report = Instant::now();

    'replay: loop {
        let mut file = match tokio::fs::File::open(&config.encoded_path).await {
            Ok(file) => file,
            Err(e) => {
                eprintln!(
                    "client_{} failed to open encoded file {}: {}",
                    id, config.encoded_path, e
                );
                break;
            }
        };

        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = file.read_exact(&mut len_buf).await {
                if e.kind() == ErrorKind::UnexpectedEof {
                    break;
                } else {
                    eprintln!("client_{} read length error: {}", id, e);
                    break 'replay;
                }
            }

            let frame_len = u32::from_be_bytes(len_buf) as usize;
            if frame_len > MAX_BATCH_BYTES {
                eprintln!(
                    "client_{} frame length {} exceeds max {}",
                    id, frame_len, MAX_BATCH_BYTES
                );
                break 'replay;
            }

            let mut frame = vec![0u8; frame_len + 4];
            frame[..4].copy_from_slice(&len_buf);

            if let Err(e) = file.read_exact(&mut frame[4..]).await {
                if e.kind() == ErrorKind::UnexpectedEof {
                    eprintln!("client_{} encountered truncated frame", id);
                } else {
                    eprintln!("client_{} read payload error: {}", id, e);
                }
                break 'replay;
            }

            if let Err(e) = socket.write_all(&frame).await {
                eprintln!(
                    "client_disconnected id={} batches={} msgs={} error={}",
                    id, total_batches_sent, total_msgs_sent, e
                );
                break 'replay;
            }

            total_batches_sent += 1;
            total_msgs_sent += config.batch_size as u64;
            total_bytes_sent += frame.len() as u64;

            if last_report.elapsed() >= Duration::from_secs(1) {
                let elapsed = start.elapsed().as_secs_f64();
                let msg_rate = if elapsed > 0.0 {
                    total_msgs_sent as f64 / elapsed
                } else {
                    0.0
                };
                let batch_rate = if elapsed > 0.0 {
                    total_batches_sent as f64 / elapsed
                } else {
                    0.0
                };
                let throughput_mbps = if elapsed > 0.0 {
                    (total_bytes_sent as f64 / elapsed) / (1024.0 * 1024.0)
                } else {
                    0.0
                };
                eprintln!(
                    "client_{} msgs={} batches={} msg_rate={:.0}/s batch_rate={:.0}/s throughput={:.2}MB/s",
                    id, total_msgs_sent, total_batches_sent, msg_rate, batch_rate, throughput_mbps
                );
                last_report = Instant::now();
            }
        }

        // Finished one complete replay
        if !config.loop_replay {
            eprintln!(
                "client_{} finished batches={} msgs={}",
                id, total_batches_sent, total_msgs_sent
            );
            break 'replay;
        }

        eprintln!("client_{} replaying from start", id);
    }

    let elapsed = start.elapsed().as_secs_f64();
    let msg_rate = if elapsed > 0.0 {
        total_msgs_sent as f64 / elapsed
    } else {
        0.0
    };
    let throughput_mbps = if elapsed > 0.0 {
        (total_bytes_sent as f64 / elapsed) / (1024.0 * 1024.0)
    } else {
        0.0
    };
    eprintln!(
        "client_stats id={} msgs={} batches={} bytes={} duration={:.2}s msg_rate={:.0}/s throughput={:.2}MB/s",
        id,
        total_msgs_sent,
        total_batches_sent,
        total_bytes_sent,
        elapsed,
        msg_rate,
        throughput_mbps
    );
}
