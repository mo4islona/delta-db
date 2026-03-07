//! Benchmarks for Memory and RocksDB backends.
//!
//! Run with: cargo bench --bench throughput

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use delta_db::db::{Config, DeltaDb};
use delta_db::engine::reducer::ReducerEngine;
use delta_db::schema::parser::parse_schema;
use delta_db::storage::memory::MemoryBackend;
use delta_db::types::{RowMap, Value};

const RAW_ONLY_SCHEMA: &str = r#"
    CREATE TABLE events (
        block_number UInt64,
        tx_hash      String,
        log_index    UInt64,
        from_addr    String,
        to_addr      String,
        value        Float64
    );
"#;

const RAW_WITH_MV_SCHEMA: &str = r#"
    CREATE TABLE events (
        block_number UInt64,
        from_addr    String,
        to_addr      String,
        value        Float64
    );

    CREATE MATERIALIZED VIEW volume_by_sender AS
    SELECT
        from_addr,
        sum(value) AS total_sent,
        count()    AS tx_count
    FROM events
    GROUP BY from_addr;
"#;

const REDUCER_EVENT_RULES_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user         String,
        side         String,
        amount       Float64,
        price        Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity   Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
        WHEN row.side = 'buy' THEN
            SET state.quantity = state.quantity + row.amount
            SET state.cost_basis = state.cost_basis + row.amount * row.price
            EMIT trade_pnl = 0
        WHEN row.side = 'sell' THEN
            LET avg_cost = state.cost_basis / state.quantity
            SET state.quantity = state.quantity - row.amount
            SET state.cost_basis = state.cost_basis - row.amount * avg_cost
            EMIT trade_pnl = row.amount * (row.price - avg_cost)
        ALWAYS EMIT
            state.quantity AS position_size
    END;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl)       AS total_pnl,
        last(position_size)  AS current_position,
        count()              AS trade_count
    FROM pnl
    GROUP BY user;
"#;

const REDUCER_LUA_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user         String,
        side         String,
        amount       Float64,
        price        Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity   Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
    LANGUAGE lua
    PROCESS $$
        if row.side == "buy" then
            state.quantity = state.quantity + row.amount
            state.cost_basis = state.cost_basis + row.amount * row.price
            emit.trade_pnl = 0
        else
            local avg_cost = state.cost_basis / state.quantity
            emit.trade_pnl = row.amount * (row.price - avg_cost)
            state.quantity = state.quantity - row.amount
            state.cost_basis = state.cost_basis - row.amount * avg_cost
        end
        emit.position_size = state.quantity
    $$;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl)       AS total_pnl,
        last(position_size)  AS current_position,
        count()              AS trade_count
    FROM pnl
    GROUP BY user;
"#;

fn make_raw_row(i: usize) -> RowMap {
    HashMap::from([
        ("block_number".to_string(), Value::UInt64(i as u64 / 100)),
        ("tx_hash".to_string(), Value::String(format!("0x{i:064x}"))),
        ("log_index".to_string(), Value::UInt64(i as u64 % 100)),
        ("from_addr".to_string(), Value::String(format!("0xuser{}", i % 1000))),
        ("to_addr".to_string(), Value::String(format!("0xrecv{}", i % 500))),
        ("value".to_string(), Value::Float64(i as f64 * 0.001)),
    ])
}

fn make_raw_row_for_mv(i: usize) -> RowMap {
    HashMap::from([
        ("block_number".to_string(), Value::UInt64(i as u64 / 100)),
        ("from_addr".to_string(), Value::String(format!("0xuser{}", i % 1000))),
        ("to_addr".to_string(), Value::String(format!("0xrecv{}", i % 500))),
        ("value".to_string(), Value::Float64(i as f64 * 0.001)),
    ])
}

fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String(user.to_string())),
        ("side".to_string(), Value::String(side.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
        ("price".to_string(), Value::Float64(price)),
    ])
}

// ─── Backend factories ─────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Backend {
    Memory,
    RocksDb,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::Memory => write!(f, "Memory"),
            Backend::RocksDb => write!(f, "RocksDB"),
        }
    }
}

fn make_config(schema: &str, backend: Backend) -> (Config, Option<tempfile::TempDir>) {
    match backend {
        Backend::Memory => (Config::new(schema), None),
        Backend::RocksDb => {
            let dir = tempfile::tempdir().unwrap();
            let cfg = Config::with_data_dir(schema, dir.path().to_str().unwrap());
            (cfg, Some(dir))
        }
    }
}

// ─── Bench results ─────────────────────────────────────────────────

struct BenchResult {
    name: String,
    #[allow(dead_code)]
    backend: String,
    total_rows: usize,
    elapsed_ms: f64,
    rows_per_sec: f64,
    pass: bool,
    target: String,
}

impl BenchResult {
    fn print(&self) {
        let status = if self.pass { "PASS" } else { "FAIL" };
        println!(
            "  [{status}] {:<45} {:>10.0} rows/s  ({} rows in {:.1}ms)  target: {}",
            self.name, self.rows_per_sec, self.total_rows, self.elapsed_ms, self.target
        );
    }
}

// ─── Benchmarks ────────────────────────────────────────────────────

fn bench_raw_ingestion(backend: Backend) -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 100;
    let (cfg, _dir) = make_config(RAW_ONLY_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows).map(make_raw_row).collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("events", block as u64, chunk.to_vec()).unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Raw ingestion [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 100_000.0,
        target: ">100K rows/sec".to_string(),
    }
}

fn bench_raw_with_mv(backend: Backend) -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 100;
    let (cfg, _dir) = make_config(RAW_WITH_MV_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows).map(make_raw_row_for_mv).collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("events", block as u64, chunk.to_vec()).unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Raw + MV [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 50_000.0,
        target: ">50K rows/sec".to_string(),
    }
}

fn bench_full_pipeline_event_rules(backend: Backend) -> BenchResult {
    let total_rows = 100_000;
    let batch_size = 50;
    let num_users = 100;
    let (cfg, _dir) = make_config(REDUCER_EVENT_RULES_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 { "buy" } else if i % 3 == 0 { "sell" } else { "buy" };
            make_trade(&user, side, 1.0 + (i as f64 * 0.01), 2000.0 + (i as f64 * 0.1))
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("trades", block as u64, chunk.to_vec()).unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Full pipeline — Event Rules [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 50_000.0,
        target: ">50K rows/sec".to_string(),
    }
}

fn bench_full_pipeline_lua(backend: Backend) -> BenchResult {
    let total_rows = 50_000;
    let batch_size = 50;
    let num_users = 100;
    let (cfg, _dir) = make_config(REDUCER_LUA_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 { "buy" } else if i % 3 == 0 { "sell" } else { "buy" };
            make_trade(&user, side, 1.0 + (i as f64 * 0.01), 2000.0 + (i as f64 * 0.1))
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("trades", block as u64, chunk.to_vec()).unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Full pipeline — Lua [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 30_000.0,
        target: ">30K rows/sec".to_string(),
    }
}

fn bench_reducer_event_rules_only() -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 100;
    let num_users = 100;

    let schema = parse_schema(REDUCER_EVENT_RULES_SCHEMA).unwrap();
    let storage = Arc::new(MemoryBackend::new());
    let reducer_def = schema.reducers[0].clone();
    let mut engine = ReducerEngine::new(reducer_def, storage);

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 { "buy" } else if i % 3 == 0 { "sell" } else { "buy" };
            make_trade(&user, side, 1.0 + (i as f64 * 0.01), 2000.0 + (i as f64 * 0.1))
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        engine.process_block(block as u64, chunk).unwrap();
    }
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: "Reducer-only — Event Rules [Memory]".to_string(),
        backend: "Memory".to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 200_000.0,
        target: ">200K rows/sec".to_string(),
    }
}

fn bench_rollback(backend: Backend) -> BenchResult {
    let num_blocks = 75;
    let rows_per_block = 134; // ~10K total rows
    let num_users = 50;
    let (cfg, _dir) = make_config(REDUCER_EVENT_RULES_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let total_rows = num_blocks * rows_per_block;
    for block in 1..=num_blocks as u64 {
        let rows: Vec<RowMap> = (0..rows_per_block)
            .map(|i| {
                let idx = (block as usize - 1) * rows_per_block + i;
                let user = format!("user{}", idx % num_users);
                make_trade(&user, "buy", 1.0, 2000.0)
            })
            .collect();
        db.process_batch("trades", block, rows).unwrap();
    }
    db.flush();

    let start = Instant::now();
    db.rollback(0).unwrap();
    let _batch = db.flush();
    let elapsed = start.elapsed();

    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    BenchResult {
        name: format!("Rollback 75 blocks, {total_rows} rows [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms,
        rows_per_sec: total_rows as f64 / elapsed.as_secs_f64(),
        pass: elapsed_ms < 10.0,
        target: "<10ms".to_string(),
    }
}

fn bench_ingest(backend: Backend) -> BenchResult {
    let total_rows = 100_000;
    let batch_size = 100;
    let (cfg, _dir) = make_config(RAW_WITH_MV_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows).map(make_raw_row_for_mv).collect();

    // Group into per-block batches, each batch becomes one ingest() call
    let blocks: Vec<Vec<RowMap>> = rows.chunks(batch_size).map(|c| c.to_vec()).collect();

    let start = Instant::now();
    for (block_num, block_rows) in blocks.iter().enumerate() {
        let block = block_num as u64;
        let mut data = HashMap::new();
        // Add block_number to each row (ingest requires it)
        let rows_with_bn: Vec<RowMap> = block_rows
            .iter()
            .map(|r| {
                let mut r = r.clone();
                r.insert("block_number".to_string(), Value::UInt64(block));
                r
            })
            .collect();
        data.insert("events".to_string(), rows_with_bn);

        let batch = db
            .ingest(delta_db::db::IngestInput {
                data,
                rollback_chain: vec![delta_db::types::BlockCursor {
                    number: block,
                    hash: format!("0x{block:x}"),
                }],
                finalized_head: delta_db::types::BlockCursor {
                    number: if block > 0 { block - 1 } else { 0 },
                    hash: format!("0x{:x}", if block > 0 { block - 1 } else { 0 }),
                },
            })
            .unwrap();

        if let Some(b) = batch {
            db.ack(b.sequence);
        }
    }
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Ingest (Raw + MV + persist) [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 20_000.0,
        target: ">20K rows/sec".to_string(),
    }
}

fn bench_many_group_keys(backend: Backend) -> BenchResult {
    let num_keys = 100_000;
    let batch_size = 1000;
    let (cfg, _dir) = make_config(REDUCER_EVENT_RULES_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let start = Instant::now();
    for batch_idx in 0..(num_keys / batch_size) {
        let rows: Vec<RowMap> = (0..batch_size)
            .map(|i| {
                let user = format!("user{}", batch_idx * batch_size + i);
                make_trade(&user, "buy", 1.0, 2000.0)
            })
            .collect();
        db.process_batch("trades", batch_idx as u64, rows).unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("{num_keys} unique group keys [{}]", backend),
        backend: backend.to_string(),
        total_rows: num_keys,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec: num_keys as f64 / elapsed.as_secs_f64(),
        pass: true,
        target: "baseline".to_string(),
    }
}

fn main() {
    println!("=== Delta DB Benchmarks ===\n");

    let backends = [Backend::Memory, Backend::RocksDb];

    let mut results: Vec<BenchResult> = Vec::new();

    for &backend in &backends {
        println!("--- {} ---", backend);
        let r = bench_raw_ingestion(backend); r.print(); results.push(r);
        let r = bench_raw_with_mv(backend); r.print(); results.push(r);
        let r = bench_full_pipeline_event_rules(backend); r.print(); results.push(r);
        let r = bench_full_pipeline_lua(backend); r.print(); results.push(r);
        let r = bench_rollback(backend); r.print(); results.push(r);
        let r = bench_ingest(backend); r.print(); results.push(r);
        let r = bench_many_group_keys(backend); r.print(); results.push(r);
        println!();
    }

    // Reducer-only (memory only, no storage)
    println!("--- Isolated ---");
    let r = bench_reducer_event_rules_only(); r.print(); results.push(r);

    println!("\n=== Summary ===\n");

    let all_pass = results.iter().all(|r| r.pass);
    if all_pass {
        println!("All benchmarks PASSED.");
    } else {
        let failed: Vec<_> = results.iter().filter(|r| !r.pass).collect();
        println!("{} benchmark(s) FAILED:", failed.len());
        for r in &failed {
            println!("  - {}: {:.0} rows/s (target: {})", r.name, r.rows_per_sec, r.target);
        }
    }
}
