use std::sync::Arc;

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::order_book::{Book, Market, PriceLevel};

pub const DEFAULT_TOP_LEVELS: usize = 10;

#[derive(Clone, Debug, Serialize)]
pub struct LevelEntry {
    pub price: i64,
    pub size: u32,
    pub count: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct Bbo {
    pub best_bid: Option<LevelEntry>,
    pub best_ask: Option<LevelEntry>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub bbo: Bbo,
    pub symbol: String,
    pub ts_ns: i64,
    pub bids: Vec<LevelEntry>,
    pub asks: Vec<LevelEntry>,
    pub total_orders: usize,
    pub bid_levels: usize,
    pub ask_levels: usize,
}

#[derive(Clone, Debug)]
pub struct SnapshotRecord {
    pub instrument_id: u32,
    pub ts_event: i64,
    pub payload: Snapshot,
}

pub type SharedSnapshot = Arc<SnapshotRecord>;

impl SnapshotRecord {
    /// Lazily serialize to JSON only when needed (for DB write or HTTP response)
    pub fn to_json(&self) -> Result<Value> {
        Ok(serde_json::to_value(&self.payload)?)
    }

    pub fn to_json_string(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.payload)?)
    }
}

pub fn build_snapshot_record(
    market: &Market,
    instrument_id: u32,
    symbol: &str,
    ts_event: i64,
    depth: usize,
) -> SnapshotRecord {
    build_snapshot_record_internal(market, instrument_id, symbol, ts_event, Some(depth))
}

pub fn build_full_snapshot_record(
    market: &Market,
    instrument_id: u32,
    symbol: &str,
    ts_event: i64,
) -> SnapshotRecord {
    build_snapshot_record_internal(market, instrument_id, symbol, ts_event, None)
}

fn build_snapshot_record_internal(
    market: &Market,
    instrument_id: u32,
    symbol: &str,
    ts_event: i64,
    depth: Option<usize>,
) -> SnapshotRecord {
    let payload = build_snapshot(market, instrument_id, symbol.to_owned(), ts_event, depth);
    SnapshotRecord {
        instrument_id,
        ts_event,
        payload,
    }
}

fn build_snapshot(
    market: &Market,
    instrument_id: u32,
    symbol: String,
    ts_event: i64,
    depth: Option<usize>,
) -> Snapshot {
    let (agg_bid, agg_ask) = market.aggregated_bbo(instrument_id);
    let (book_bids, book_asks, total_orders, bid_levels, ask_levels) = market
        .books_by_pub(instrument_id)
        .and_then(|books| books.first())
        .map(|(_, book)| summarize_book(book, depth))
        .unwrap_or_else(|| (Vec::new(), Vec::new(), 0, 0, 0));

    Snapshot {
        symbol,
        ts_ns: ts_event,
        bbo: Bbo {
            best_bid: agg_bid.as_ref().map(to_level_entry),
            best_ask: agg_ask.as_ref().map(to_level_entry),
        },
        bids: book_bids,
        asks: book_asks,
        total_orders,
        bid_levels,
        ask_levels,
    }
}

fn summarize_book(
    book: &Book,
    depth: Option<usize>,
) -> (Vec<LevelEntry>, Vec<LevelEntry>, usize, usize, usize) {
    let bid_iter = book.iter_bids_desc().map(|lvl| to_level_entry(&lvl));
    let ask_iter = book.iter_asks_asc().map(|lvl| to_level_entry(&lvl));

    let bids = match depth {
        Some(limit) => bid_iter.take(limit).collect::<Vec<_>>(),
        None => bid_iter.collect::<Vec<_>>(),
    };
    let asks = match depth {
        Some(limit) => ask_iter.take(limit).collect::<Vec<_>>(),
        None => ask_iter.collect::<Vec<_>>(),
    };

    (
        bids,
        asks,
        book.total_orders(),
        book.bid_level_count(),
        book.ask_level_count(),
    )
}

fn to_level_entry(level: &PriceLevel) -> LevelEntry {
    LevelEntry {
        price: level.price,
        size: level.size,
        count: level.count,
    }
}

// MBP output format structures
#[derive(Serialize)]
pub struct MbpLevel {
    pub count: u32,
    pub price: String,
    pub size: u32,
}

#[derive(Serialize)]
pub struct MbpBboSide {
    pub count: u32,
    pub price: String,
    pub size: u32,
}

#[derive(Serialize)]
pub struct MbpBbo {
    pub ask: Option<MbpBboSide>,
    pub bid: Option<MbpBboSide>,
}

#[derive(Serialize)]
pub struct MbpLevels {
    pub asks: Vec<MbpLevel>,
    pub bids: Vec<MbpLevel>,
}

#[derive(Serialize)]
pub struct MbpStats {
    pub ask_levels: usize,
    pub bid_levels: usize,
    pub total_orders: usize,
}

#[derive(Serialize)]
pub struct MbpOutput {
    pub bbo: MbpBbo,
    pub levels: MbpLevels,
    pub info: MbpStats,
    pub symbol: String,
    pub timestamp: String,
}

fn level_to_mbp(e: &LevelEntry) -> MbpLevel {
    MbpLevel {
        count: e.count,
        price: e.price.to_string(),
        size: e.size,
    }
}

fn level_to_mbp_bbo(e: &LevelEntry) -> MbpBboSide {
    MbpBboSide {
        count: e.count,
        price: e.price.to_string(),
        size: e.size,
    }
}

pub fn snapshot_to_mbp_output(rec: &SnapshotRecord) -> MbpOutput {
    MbpOutput {
        bbo: MbpBbo {
            ask: rec.payload.bbo.best_ask.as_ref().map(level_to_mbp_bbo),
            bid: rec.payload.bbo.best_bid.as_ref().map(level_to_mbp_bbo),
        },
        levels: MbpLevels {
            asks: rec.payload.asks.iter().map(level_to_mbp).collect(),
            bids: rec.payload.bids.iter().map(level_to_mbp).collect(),
        },
        info: MbpStats {
            ask_levels: rec.payload.ask_levels,
            bid_levels: rec.payload.bid_levels,
            total_orders: rec.payload.total_orders,
        },
        symbol: rec.payload.symbol.clone(),
        timestamp: rec.payload.ts_ns.to_string(),
    }
}
