use std::{
    io::Write,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use postgres::error::SqlState;
use postgres::{Client, Config, NoTls};

use crate::snapshot::SharedSnapshot;

const TABLE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    ts_event BIGINT NOT NULL,
    best_bid_price BIGINT NOT NULL,
    best_bid_size INTEGER NOT NULL,
    best_bid_count INTEGER NOT NULL,
    best_ask_price BIGINT NOT NULL,
    best_ask_size INTEGER NOT NULL,
    best_ask_count INTEGER NOT NULL,
    bid_levels INTEGER NOT NULL,
    ask_levels INTEGER NOT NULL,
    total_orders INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_ts
    ON orderbook_snapshots (ts_event);
CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_symbol
    ON orderbook_snapshots (symbol, ts_event DESC);
"#;

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub db_url: Arc<String>,
    pub batch_size: usize,
    pub flush_interval: Duration,
}

impl StorageConfig {
    pub fn new(db_url: Arc<String>, batch_size: usize, flush_interval: Duration) -> Self {
        Self {
            db_url,
            batch_size: batch_size.max(1),
            flush_interval,
        }
    }
}

pub fn spawn_writer(
    config: StorageConfig,
    rx: Receiver<SharedSnapshot>,
) -> thread::JoinHandle<Result<()>> {
    thread::spawn(move || writer_loop(config, rx))
}

pub fn init_database(db_url: &str) -> Result<()> {
    ensure_database(db_url)?;
    let mut client = Client::connect(db_url, NoTls)
        .with_context(|| format!("failed to connect to postgres using {}", db_url))?;
    ensure_schema(&mut client)
}

fn writer_loop(config: StorageConfig, rx: Receiver<SharedSnapshot>) -> Result<()> {
    println!("storage_writer starting db_url={}", config.db_url);

    // Ensure database exists
    if let Err(e) = ensure_database(config.db_url.as_ref()) {
        eprintln!("storage_writer failed to ensure database: {}", e);
        return Err(e);
    }
    println!("storage_writer database ensured");

    // Connect to database
    let mut client = match Client::connect(&config.db_url, NoTls) {
        Ok(c) => {
            println!("storage_writer connected to postgres");
            c
        }
        Err(e) => {
            eprintln!("storage_writer failed to connect to postgres: {}", e);
            return Err(anyhow!(e).context(format!(
                "failed to connect to postgres using {}",
                &config.db_url
            )));
        }
    };

    // Ensure schema
    if let Err(e) = ensure_schema(&mut client) {
        eprintln!("storage_writer failed to ensure schema: {}", e);
        return Err(e);
    }
    println!("storage_writer schema ensured");

    // Drop indexes for bulk load
    println!("storage_writer dropping indexes for bulk load");
    if let Err(e) = drop_indexes(&mut client) {
        eprintln!("storage_writer failed to drop indexes: {}", e);
        return Err(e);
    }
    println!("storage_writer indexes dropped");

    let mut buffer: Vec<SharedSnapshot> = Vec::with_capacity(config.batch_size);
    let mut last_flush = Instant::now();
    let mut total_written = 0usize;
    let mut failed_flushes = 0usize;

    loop {
        let recv_result = if buffer.is_empty() {
            // If buffer empty we block waiting for data.
            rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
        } else {
            rx.recv_timeout(config.flush_interval)
        };

        match recv_result {
            Ok(snapshot) => {
                buffer.push(snapshot);
                if buffer.len() >= config.batch_size {
                    match flush_copy(&mut client, &mut buffer) {
                        Ok(_) => {
                            total_written += buffer.len();
                            println!(
                                "storage_writer flushed batch size={} total={}",
                                buffer.len(),
                                total_written
                            );
                            buffer.clear();
                            last_flush = Instant::now();
                        }
                        Err(e) => {
                            failed_flushes += 1;
                            eprintln!(
                                "storage_writer flush failed attempt={} error={} buffer_size={}",
                                failed_flushes,
                                e,
                                buffer.len()
                            );
                            // Try to reconnect if connection lost
                            if is_connection_error(&e) {
                                println!("storage_writer attempting reconnect...");
                                match Client::connect(&config.db_url, NoTls) {
                                    Ok(new_client) => {
                                        client = new_client;
                                        println!("storage_writer reconnected successfully");
                                        // Retry flush once
                                        if let Err(e2) = flush_copy(&mut client, &mut buffer) {
                                            eprintln!("storage_writer retry flush failed: {}", e2);
                                            return Err(e2);
                                        } else {
                                            total_written += buffer.len();
                                            println!(
                                                "storage_writer retry flush succeeded size={}",
                                                buffer.len()
                                            );
                                            buffer.clear();
                                            last_flush = Instant::now();
                                        }
                                    }
                                    Err(e2) => {
                                        eprintln!("storage_writer reconnect failed: {}", e2);
                                        return Err(
                                            anyhow!(e2).context("failed to reconnect to postgres")
                                        );
                                    }
                                }
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if !buffer.is_empty() {
                    match flush_copy(&mut client, &mut buffer) {
                        Ok(_) => {
                            total_written += buffer.len();
                            println!(
                                "storage_writer flushed timeout batch size={} total={}",
                                buffer.len(),
                                total_written
                            );
                            buffer.clear();
                            last_flush = Instant::now();
                        }
                        Err(e) => {
                            eprintln!("storage_writer timeout flush failed: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                println!(
                    "storage_writer channel disconnected, flushing remaining buffer_size={}",
                    buffer.len()
                );
                if !buffer.is_empty() {
                    match flush_copy(&mut client, &mut buffer) {
                        Ok(_) => {
                            total_written += buffer.len();
                            println!("storage_writer final flush succeeded size={}", buffer.len());
                            buffer.clear();
                        }
                        Err(e) => {
                            eprintln!("storage_writer final flush failed: {}", e);
                            return Err(e);
                        }
                    }
                }
                break;
            }
        }

        if !buffer.is_empty() && last_flush.elapsed() >= config.flush_interval {
            match flush_copy(&mut client, &mut buffer) {
                Ok(_) => {
                    total_written += buffer.len();
                    buffer.clear();
                    last_flush = Instant::now();
                }
                Err(e) => {
                    eprintln!("storage_writer interval flush failed: {}", e);
                    return Err(e);
                }
            }
        }
    }

    println!(
        "storage_writer recreating indexes after {} snapshots (failed_flushes={})",
        total_written, failed_flushes
    );
    if let Err(e) = recreate_indexes(&mut client) {
        eprintln!("storage_writer failed to recreate indexes: {}", e);
        return Err(e);
    }
    println!("storage_writer indexes recreated successfully");

    Ok(())
}

fn is_connection_error(e: &anyhow::Error) -> bool {
    e.to_string().contains("connection")
        || e.to_string().contains("Connection")
        || e.to_string().contains("broken pipe")
        || e.to_string().contains("reset by peer")
}

fn flush_copy(client: &mut Client, buffer: &mut Vec<SharedSnapshot>) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let batch_size = buffer.len();

    let mut txn = client.transaction().with_context(|| {
        format!(
            "failed to start COPY transaction for {} snapshots",
            batch_size
        )
    })?;

    let copy_stmt = "COPY orderbook_snapshots (symbol, ts_event, best_bid_price, best_bid_size, best_bid_count, best_ask_price, best_ask_size, best_ask_count, bid_levels, ask_levels, total_orders) FROM STDIN WITH (FORMAT csv)";
    let mut writer = txn
        .copy_in(copy_stmt)
        .with_context(|| format!("failed to start COPY for {} snapshots", batch_size))?;

    for (idx, snapshot) in buffer.iter().enumerate() {
        let payload = &snapshot.payload;

        // Extract best bid (use 0 if None)
        let (best_bid_price, best_bid_size, best_bid_count) = payload
            .bbo
            .best_bid
            .as_ref()
            .map(|b| (b.price, b.size as i32, b.count as i32))
            .unwrap_or((0, 0, 0));

        // Extract best ask (use 0 if None)
        let (best_ask_price, best_ask_size, best_ask_count) = payload
            .bbo
            .best_ask
            .as_ref()
            .map(|a| (a.price, a.size as i32, a.count as i32))
            .unwrap_or((0, 0, 0));

        let row = format!(
            "{},{},{},{},{},{},{},{},{},{},{}\n",
            escape_csv(&payload.symbol),
            snapshot.ts_event,
            best_bid_price,
            best_bid_size,
            best_bid_count,
            best_ask_price,
            best_ask_size,
            best_ask_count,
            payload.bid_levels,
            payload.ask_levels,
            payload.total_orders
        );

        writer.write_all(row.as_bytes()).with_context(|| {
            format!(
                "failed to write COPY row idx={} instrument_id={} ts={} size={}",
                idx,
                snapshot.instrument_id,
                snapshot.ts_event,
                row.len()
            )
        })?;
    }

    writer
        .finish()
        .with_context(|| format!("failed to finish COPY for {} snapshots", batch_size))?;
    txn.commit()
        .with_context(|| format!("failed to commit COPY batch of {} snapshots", batch_size))?;

    Ok(())
}

fn escape_csv(s: &str) -> String {
    // CSV escape: wrap in quotes and double internal quotes
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn drop_indexes(client: &mut Client) -> Result<()> {
    let drop_sql = r#"
DROP INDEX IF EXISTS idx_orderbook_snapshots_ts;
DROP INDEX IF EXISTS idx_orderbook_snapshots_symbol;
"#;
    client
        .batch_execute(drop_sql)
        .context("failed to drop indexes (ts, symbol)")?;
    println!("storage_writer dropped 2 indexes successfully");
    Ok(())
}

fn recreate_indexes(client: &mut Client) -> Result<()> {
    println!("storage_writer recreating indexes (this may take time)...");
    let start = Instant::now();

    let create_sql = r#"
CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_ts
    ON orderbook_snapshots (ts_event);
CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_symbol
    ON orderbook_snapshots (symbol, ts_event DESC);
"#;
    client
        .batch_execute(create_sql)
        .context("failed to recreate indexes (ts, symbol)")?;

    let elapsed = start.elapsed();
    println!(
        "storage_writer recreated 2 indexes successfully in {:.2}s",
        elapsed.as_secs_f64()
    );
    Ok(())
}

fn ensure_schema(client: &mut Client) -> Result<()> {
    client.batch_execute(TABLE_DDL).context(
        "failed to ensure orderbook_snapshots schema (CREATE TABLE and CREATE INDEX commands)",
    )?;
    println!("storage_writer schema ensured (table and indexes exist)");
    Ok(())
}

fn ensure_database(db_url: &str) -> Result<()> {
    println!("storage_writer ensuring database exists");

    let base_config: Config = db_url
        .parse()
        .with_context(|| format!("failed to parse DATABASE_URL: {}", db_url))?;
    let target_db = base_config
        .get_dbname()
        .map(|s| s.to_owned())
        .unwrap_or_else(|| String::from("postgres"));

    println!("storage_writer target_db={}", target_db);

    match base_config.clone().connect(NoTls) {
        Ok(mut client) => {
            println!(
                "storage_writer database {} already exists, validating...",
                target_db
            );
            client
                .simple_query("SELECT 1")
                .context("failed to validate postgres connectivity with SELECT 1")?;
            println!(
                "storage_writer database {} validated successfully",
                target_db
            );
            return Ok(());
        }
        Err(err) => {
            let missing_db = err
                .as_db_error()
                .map(|db_err| db_err.code() == &SqlState::INVALID_CATALOG_NAME)
                .unwrap_or(false);
            if !missing_db {
                eprintln!(
                    "storage_writer connection error (not missing database): {}",
                    err
                );
                return Err(
                    anyhow!(err).context(format!("failed to connect to postgres using {}", db_url))
                );
            }
            println!(
                "storage_writer database {} does not exist, creating...",
                target_db
            );
        }
    }

    let admin_db = if target_db == "postgres" {
        "template1"
    } else {
        "postgres"
    };
    println!("storage_writer connecting to admin_db={}", admin_db);

    let mut admin_config = base_config.clone();
    admin_config.dbname(admin_db);
    let mut admin_client = admin_config.connect(NoTls).with_context(|| {
        format!(
            "failed to connect to admin database {} (needed to create target db {})",
            admin_db, target_db
        )
    })?;

    println!("storage_writer creating database {}", target_db);
    let create_sql = format!("CREATE DATABASE {}", escape_ident(&target_db));
    match admin_client.simple_query(&create_sql) {
        Ok(_) => {
            println!("storage_writer created database {} successfully", target_db);
        }
        Err(err) => {
            let duplicate = err
                .as_db_error()
                .map(|db_err| db_err.code() == &SqlState::DUPLICATE_DATABASE)
                .unwrap_or(false);
            if !duplicate {
                eprintln!(
                    "storage_writer failed to create database {}: {}",
                    target_db, err
                );
                return Err(
                    anyhow!(err).context(format!("failed to create target database {}", target_db))
                );
            }
            println!(
                "storage_writer database {} already exists (concurrent creation)",
                target_db
            );
        }
    }
    drop(admin_client);

    println!(
        "storage_writer connecting to newly created database {}",
        target_db
    );
    base_config.clone().connect(NoTls).with_context(|| {
        format!(
            "failed to connect to newly created database {} using {}",
            target_db, db_url
        )
    })?;

    println!("storage_writer database {} ready", target_db);
    Ok(())
}

fn escape_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
