use std::collections::BTreeMap;
use std::sync::Arc;

use crate::delta::DeltaBuffer;
use crate::engine::dag::DeltaEngine;
use crate::error::{Error, Result};
use crate::schema::parser::parse_schema;
use crate::storage::{StorageBackend, StorageWriteBatch};
use crate::storage::memory::MemoryBackend;
use crate::storage::rocks::RocksDbBackend;
use crate::types::{BlockCursor, BlockNumber, DeltaBatch, RowMap};

/// Configuration for opening a DeltaDb instance.
pub struct Config {
    /// SQL schema definition string.
    pub schema: String,
    /// Maximum number of pending delta records before backpressure.
    pub max_buffer_size: usize,
    /// Path to RocksDB data directory. When set, data is persisted to disk.
    /// When None, uses in-memory storage (data lost on drop).
    pub data_dir: Option<String>,
    /// Explicit storage backend override. Takes precedence over data_dir.
    pub storage: Option<Arc<dyn StorageBackend>>,
}

impl Config {
    /// Create a config with in-memory storage (no persistence).
    /// Suitable for tests and benchmarks.
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            max_buffer_size: 10_000,
            data_dir: None,
            storage: None,
        }
    }

    /// Create a config with RocksDB persistence at the given path.
    pub fn with_data_dir(schema: impl Into<String>, data_dir: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            max_buffer_size: 10_000,
            data_dir: Some(data_dir.into()),
            storage: None,
        }
    }

    pub fn max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    pub fn storage(mut self, storage: Arc<dyn StorageBackend>) -> Self {
        self.storage = Some(storage);
        self
    }
}

/// Input for the atomic `ingest()` method.
pub struct IngestInput {
    /// Table name → rows. Each row must contain `block_number`.
    pub data: std::collections::HashMap<String, Vec<RowMap>>,
    /// Unfinalized blocks with hashes (from ctx.state.rollbackChain).
    pub rollback_chain: Vec<BlockCursor>,
    /// Finalized head cursor (from ctx.head.finalized). Required.
    pub finalized_head: BlockCursor,
}

// Metadata keys for persistence
const META_LATEST_BLOCK: &str = "latest_block";
const META_FINALIZED_BLOCK: &str = "finalized_block";
const META_BLOCK_HASHES: &str = "block_hashes";

/// Top-level Delta DB API.
///
/// Provides a simple interface for ingesting blockchain data,
/// handling rollbacks, and producing delta batches for downstream targets.
pub struct DeltaDb {
    engine: DeltaEngine,
    buffer: DeltaBuffer,
    storage: Arc<dyn StorageBackend>,
}

impl DeltaDb {
    /// Open a DeltaDb instance with the given configuration.
    /// Parses and validates the schema at open time.
    pub fn open(config: Config) -> Result<Self> {
        let schema = parse_schema(&config.schema)?;

        let storage: Arc<dyn StorageBackend> = if let Some(s) = config.storage {
            s
        } else if let Some(ref dir) = config.data_dir {
            Arc::new(RocksDbBackend::open(dir)?)
        } else {
            Arc::new(MemoryBackend::new())
        };

        let mut engine = DeltaEngine::new(&schema, storage.clone());

        // Restore persisted state
        if let Some(bytes) = storage.get_meta(META_LATEST_BLOCK)? {
            let block = u64::from_be_bytes(bytes.try_into().map_err(|_| {
                Error::Storage("corrupt latest_block metadata".into())
            })?);
            engine.set_latest_block(block);
        }
        if let Some(bytes) = storage.get_meta(META_FINALIZED_BLOCK)? {
            let block = u64::from_be_bytes(bytes.try_into().map_err(|_| {
                Error::Storage("corrupt finalized_block metadata".into())
            })?);
            engine.set_finalized_block(block);
        }
        if let Some(bytes) = storage.get_meta(META_BLOCK_HASHES)? {
            let hashes: BTreeMap<BlockNumber, String> =
                serde_json::from_slice(&bytes).map_err(|e| {
                    Error::Storage(format!("corrupt block_hashes metadata: {e}"))
                })?;
            engine.restore_block_hashes(hashes);
        }

        // Replay unfinalized blocks to rebuild reducer/MV in-memory state
        let finalized = engine.finalized_block();
        let latest = engine.latest_block();
        if latest > finalized {
            engine.replay_unfinalized(finalized + 1, latest)?;
        }

        let buffer = DeltaBuffer::new(config.max_buffer_size);

        Ok(Self {
            engine,
            buffer,
            storage,
        })
    }

    /// Process a batch of rows for a raw table at the given block number.
    /// Delta records are buffered internally.
    /// Returns true if backpressure should be applied (buffer is full).
    pub fn process_batch(
        &mut self,
        table: &str,
        block: BlockNumber,
        rows: Vec<RowMap>,
    ) -> Result<bool> {
        let deltas = self.engine.process_batch(table, block, rows)?;

        self.buffer.push(
            deltas,
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
        );

        Ok(self.buffer.is_full())
    }

    /// Roll back all state after fork_point.
    /// Compensating delta records are buffered.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Result<()> {
        let deltas = self.engine.rollback(fork_point)?;

        self.buffer.push(
            deltas,
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
        );

        Ok(())
    }

    /// Finalize all state up to and including the given block.
    /// Finalized data cannot be rolled back.
    /// All finalized state + metadata is committed atomically.
    pub fn finalize(&mut self, block: BlockNumber) -> Result<()> {
        let mut batch = StorageWriteBatch::new();
        self.engine.finalize(block, &mut batch);
        self.append_meta_to_batch(&mut batch)?;
        self.storage.commit(&batch)
    }

    /// Flush all buffered delta records into a DeltaBatch.
    /// Returns None if there are no pending records.
    pub fn flush(&mut self) -> Option<DeltaBatch> {
        self.buffer.flush()
    }

    /// Acknowledge a previously flushed batch by sequence number.
    pub fn ack(&mut self, sequence: u64) {
        self.buffer.ack(sequence);
    }

    /// Number of pending (unflushed) delta records.
    pub fn pending_count(&self) -> usize {
        self.buffer.pending_count()
    }

    /// Whether backpressure should be applied.
    pub fn is_backpressured(&self) -> bool {
        self.buffer.is_full()
    }

    /// Current latest processed block number.
    pub fn latest_block(&self) -> BlockNumber {
        self.engine.latest_block()
    }

    /// Current latest processed block as a cursor (number + hash).
    pub fn latest_cursor(&self) -> Option<BlockCursor> {
        self.engine.latest_cursor()
    }

    /// Current finalized block number.
    pub fn finalized_block(&self) -> BlockNumber {
        self.engine.finalized_block()
    }

    /// Current finalized block as a cursor (number + hash).
    pub fn finalized_cursor(&self) -> Option<BlockCursor> {
        self.engine.finalized_cursor()
    }

    /// Store block hashes from the rollback chain and finalized head.
    pub fn set_rollback_chain(&mut self, chain: &[(BlockNumber, String)]) {
        self.engine.set_rollback_chain(chain);
    }

    /// Find the common ancestor between our state and the Portal's chain.
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: &[(BlockNumber, &str)],
    ) -> Option<BlockCursor> {
        self.engine.resolve_fork_cursor(previous_blocks)
    }

    /// Atomic ingest: process all tables, store rollback chain, finalize, flush.
    ///
    /// Replaces separate `process_batch` + `set_rollback_chain` + `finalize` + `flush`.
    /// Each row must contain a `block_number` field (UInt64).
    pub fn ingest(&mut self, input: IngestInput) -> Result<Option<DeltaBatch>> {
        // 1. For each table, group rows by block_number and process in order
        for (table, rows) in &input.data {
            let mut by_block: BTreeMap<BlockNumber, Vec<RowMap>> = BTreeMap::new();
            for row in rows {
                let block = row
                    .get("block_number")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| {
                        Error::InvalidOperation(format!(
                            "row in table '{table}' missing block_number"
                        ))
                    })?;
                by_block.entry(block).or_default().push(row.clone());
            }

            for (block, block_rows) in by_block {
                let deltas = self.engine.process_batch(table, block, block_rows)?;
                self.buffer.push(
                    deltas,
                    self.engine.finalized_cursor(),
                    self.engine.latest_cursor(),
                );
            }
        }

        // 2. Store rollback chain hashes
        let chain: Vec<(BlockNumber, String)> = input
            .rollback_chain
            .iter()
            .map(|c| (c.number, c.hash.clone()))
            .collect();
        self.engine.set_rollback_chain(&chain);

        // 3. Store finalized head hash and finalize atomically
        self.engine.set_rollback_chain(&[(
            input.finalized_head.number,
            input.finalized_head.hash.clone(),
        )]);
        let mut write_batch = StorageWriteBatch::new();
        self.engine.finalize(input.finalized_head.number, &mut write_batch);
        self.append_meta_to_batch(&mut write_batch)?;
        self.storage.commit(&write_batch)?;

        // 4. Update buffer heads with correct cursors (hashes now stored)
        self.buffer.set_heads(
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
        );

        // 5. Flush
        let batch = self.buffer.flush();

        Ok(batch)
    }

    /// Append engine metadata (latest_block, finalized_block, block_hashes)
    /// to a write batch for atomic commit.
    fn append_meta_to_batch(&self, batch: &mut StorageWriteBatch) -> Result<()> {
        batch.put_meta(
            META_LATEST_BLOCK,
            &self.engine.latest_block().to_be_bytes(),
        );
        batch.put_meta(
            META_FINALIZED_BLOCK,
            &self.engine.finalized_block().to_be_bytes(),
        );
        let hashes_json = serde_json::to_vec(self.engine.block_hashes()).map_err(|e| {
            Error::Storage(format!("failed to serialize block_hashes: {e}"))
        })?;
        batch.put_meta(META_BLOCK_HASHES, &hashes_json);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DeltaOperation, Value};
    use std::collections::HashMap;

    const DEX_SCHEMA: &str = r#"
        CREATE TABLE trades (
            block_number UInt64,
            user String,
            side String,
            amount Float64,
            price Float64
        );

        CREATE REDUCER pnl
        SOURCE trades
        GROUP BY user
        STATE (
            quantity Float64 DEFAULT 0,
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
            sum(trade_pnl) AS total_pnl,
            last(position_size) AS current_position,
            count() AS trade_count
        FROM pnl
        GROUP BY user;
    "#;

    const SIMPLE_SCHEMA: &str = r#"
        CREATE TABLE swaps (
            pool String,
            amount Float64
        );

        CREATE MATERIALIZED VIEW pool_volume AS
        SELECT
            pool,
            sum(amount) AS total_volume,
            count() AS swap_count
        FROM swaps
        GROUP BY pool;
    "#;

    fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
        HashMap::from([
            ("user".to_string(), Value::String(user.to_string())),
            ("side".to_string(), Value::String(side.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
            ("price".to_string(), Value::Float64(price)),
        ])
    }

    fn make_swap(pool: &str, amount: f64) -> RowMap {
        HashMap::from([
            ("pool".to_string(), Value::String(pool.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
        ])
    }

    #[test]
    fn open_with_valid_schema() {
        let db = DeltaDb::open(Config::new(SIMPLE_SCHEMA));
        assert!(db.is_ok());
    }

    #[test]
    fn open_with_invalid_schema() {
        let db = DeltaDb::open(Config::new("INVALID SQL GARBAGE"));
        assert!(db.is_err());
    }

    #[test]
    fn simple_ingest_and_flush() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        db.process_batch("swaps", 1000, vec![
            make_swap("ETH/USDC", 100.0),
            make_swap("ETH/USDC", 200.0),
        ]).unwrap();

        let batch = db.flush().unwrap();
        assert_eq!(batch.sequence, 1);
        assert_eq!(batch.latest_head.as_ref().map(|c| c.number), Some(1000));

        // 2 raw inserts + 1 MV insert = 3 records
        assert_eq!(batch.record_count(), 3);

        let mv_records: Vec<_> = batch.records_for("pool_volume").iter()
            .collect();
        assert_eq!(mv_records.len(), 1);
        assert_eq!(mv_records[0].operation, DeltaOperation::Insert);
        assert_eq!(mv_records[0].values.get("total_volume"), Some(&Value::Float64(300.0)));
    }

    #[test]
    fn multiple_blocks_merge_in_buffer() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        db.process_batch("swaps", 1000, vec![make_swap("ETH/USDC", 100.0)]).unwrap();
        db.process_batch("swaps", 1001, vec![make_swap("ETH/USDC", 200.0)]).unwrap();

        let batch = db.flush().unwrap();

        // MV records should be merged: Insert + Update -> Insert with latest values
        let mv_records: Vec<_> = batch.records_for("pool_volume").iter()
            .collect();
        assert_eq!(mv_records.len(), 1);
        assert_eq!(mv_records[0].operation, DeltaOperation::Insert);
        assert_eq!(mv_records[0].values.get("total_volume"), Some(&Value::Float64(300.0)));
    }

    #[test]
    fn rollback_produces_compensating_deltas() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        db.process_batch("swaps", 1000, vec![make_swap("ETH/USDC", 100.0)]).unwrap();
        db.process_batch("swaps", 1001, vec![make_swap("ETH/USDC", 200.0)]).unwrap();

        // Flush and clear buffer
        db.flush();

        // Rollback block 1001
        db.rollback(1000).unwrap();

        let batch = db.flush().unwrap();

        // Should have MV update (back to 100) and raw delete
        let mv_records: Vec<_> = batch.records_for("pool_volume").iter()
            .collect();
        assert_eq!(mv_records.len(), 1);
        assert_eq!(mv_records[0].operation, DeltaOperation::Update);
        assert_eq!(mv_records[0].values.get("total_volume"), Some(&Value::Float64(100.0)));

        assert_eq!(db.latest_block(), 1000);
    }

    #[test]
    fn finalize_and_rollback() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        db.process_batch("swaps", 1000, vec![make_swap("ETH/USDC", 100.0)]).unwrap();
        db.process_batch("swaps", 1001, vec![make_swap("ETH/USDC", 200.0)]).unwrap();
        db.process_batch("swaps", 1002, vec![make_swap("ETH/USDC", 300.0)]).unwrap();
        db.flush();

        // Finalize up to 1001
        db.finalize(1001).unwrap();
        assert_eq!(db.finalized_block(), 1001);

        // Rollback block 1002
        db.rollback(1001).unwrap();

        let batch = db.flush().unwrap();
        let mv_records: Vec<_> = batch.records_for("pool_volume").iter()
            .collect();
        assert_eq!(mv_records.len(), 1);
        // total should be 100 + 200 = 300
        assert_eq!(mv_records[0].values.get("total_volume"), Some(&Value::Float64(300.0)));
    }

    #[test]
    fn full_pipeline_with_reducer() {
        let mut db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();

        // Block 1000: alice buys 10 @ 2000
        db.process_batch("trades", 1000, vec![
            make_trade("alice", "buy", 10.0, 2000.0),
        ]).unwrap();

        // Block 1001: alice sells 5 @ 2200
        db.process_batch("trades", 1001, vec![
            make_trade("alice", "sell", 5.0, 2200.0),
        ]).unwrap();

        let batch = db.flush().unwrap();

        let mv_records: Vec<_> = batch.records_for("position_summary").iter()
            .collect();
        assert_eq!(mv_records.len(), 1);

        // trade_count should be 2
        assert_eq!(mv_records[0].values.get("trade_count"), Some(&Value::UInt64(2)));
        // current_position = last(position_size) = 5.0
        assert_eq!(mv_records[0].values.get("current_position"), Some(&Value::Float64(5.0)));

        // total_pnl: trade 1 = 0 (buy), trade 2 = 5*(2200-2000) = 1000
        let total_pnl = mv_records[0].values.get("total_pnl").unwrap().as_f64().unwrap();
        assert!((total_pnl - 1000.0).abs() < 0.01);
    }

    #[test]
    fn full_pipeline_rollback_and_reingest() {
        let mut db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();

        db.process_batch("trades", 1000, vec![make_trade("alice", "buy", 10.0, 2000.0)]).unwrap();
        db.process_batch("trades", 1001, vec![make_trade("alice", "buy", 5.0, 2100.0)]).unwrap();
        db.process_batch("trades", 1002, vec![make_trade("alice", "sell", 8.0, 2200.0)]).unwrap();
        db.flush();

        // Rollback block 1002 (the sell)
        db.rollback(1001).unwrap();
        db.flush();

        // Re-ingest with different sell
        db.process_batch("trades", 1002, vec![make_trade("alice", "sell", 3.0, 2300.0)]).unwrap();

        let batch = db.flush().unwrap();
        let mv_records: Vec<_> = batch.records_for("position_summary").iter()
            .collect();
        assert_eq!(mv_records.len(), 1);
        assert_eq!(mv_records[0].values.get("trade_count"), Some(&Value::UInt64(3)));

        // position_size after: 10 + 5 - 3 = 12
        assert_eq!(mv_records[0].values.get("current_position"), Some(&Value::Float64(12.0)));
    }

    #[test]
    fn backpressure_signal() {
        let mut db = DeltaDb::open(
            Config::new(SIMPLE_SCHEMA).max_buffer_size(3),
        ).unwrap();

        // First batch: 2 raw + 1 MV = 3 records → buffer full
        let full = db.process_batch("swaps", 1000, vec![
            make_swap("ETH/USDC", 100.0),
            make_swap("ETH/USDC", 200.0),
        ]).unwrap();

        assert!(full);
        assert!(db.is_backpressured());

        // Flush clears buffer
        db.flush();
        assert!(!db.is_backpressured());
    }

    #[test]
    fn unknown_table_returns_error() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();
        let result = db.process_batch("nonexistent", 1000, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn empty_flush_returns_none() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();
        assert!(db.flush().is_none());
    }

    #[test]
    fn sequence_numbers_increment() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        db.process_batch("swaps", 1000, vec![make_swap("ETH/USDC", 100.0)]).unwrap();
        let b1 = db.flush().unwrap();

        db.process_batch("swaps", 1001, vec![make_swap("ETH/USDC", 200.0)]).unwrap();
        let b2 = db.flush().unwrap();

        assert_eq!(b1.sequence, 1);
        assert_eq!(b2.sequence, 2);
    }

    #[test]
    fn full_rollback_emits_delete_for_mv_group() {
        // Schema: aggregate volume per wallet. A wallet that only appeared in
        // rolled-back blocks should produce a Delete delta for its MV group.
        let schema = r#"
            CREATE TABLE transfers (
                wallet String,
                amount Float64
            );

            CREATE MATERIALIZED VIEW wallet_volume AS
            SELECT
                wallet,
                sum(amount) AS total_volume,
                count() AS tx_count
            FROM transfers
            GROUP BY wallet;
        "#;

        let mut db = DeltaDb::open(Config::new(schema)).unwrap();

        // Block 1000: alice appears for the first time
        db.process_batch("transfers", 1000, vec![
            HashMap::from([
                ("wallet".to_string(), Value::String("alice".to_string())),
                ("amount".to_string(), Value::Float64(500.0)),
            ]),
        ]).unwrap();

        let batch = db.flush().unwrap();

        // Verify Insert was emitted for alice's MV group
        let mv_inserts: Vec<_> = batch.records_for("wallet_volume").iter()
            .filter(|r| r.operation == DeltaOperation::Insert)
            .collect();
        assert_eq!(mv_inserts.len(), 1);
        assert_eq!(mv_inserts[0].values.get("total_volume"), Some(&Value::Float64(500.0)));

        // Rollback block 1000 — alice's only block
        db.rollback(999).unwrap();

        let batch = db.flush().unwrap();

        // The MV group for alice should be deleted since she has no data left
        let mv_deletes: Vec<_> = batch.records_for("wallet_volume").iter()
            .filter(|r| r.operation == DeltaOperation::Delete)
            .collect();
        assert_eq!(mv_deletes.len(), 1, "expected Delete delta for fully rolled-back MV group");
        assert_eq!(
            mv_deletes[0].key.get("wallet"),
            Some(&Value::String("alice".to_string()))
        );
    }

    #[test]
    fn ingest_groups_rows_by_block_number() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        let batch = db.ingest(IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![
                    HashMap::from([
                        ("pool".to_string(), Value::String("ETH/USDC".into())),
                        ("amount".to_string(), Value::Float64(100.0)),
                        ("block_number".to_string(), Value::UInt64(1001)),
                    ]),
                    HashMap::from([
                        ("pool".to_string(), Value::String("ETH/USDC".into())),
                        ("amount".to_string(), Value::Float64(200.0)),
                        ("block_number".to_string(), Value::UInt64(1000)),
                    ]),
                ],
            )]),
            rollback_chain: vec![
                BlockCursor { number: 1000, hash: "0xa".into() },
                BlockCursor { number: 1001, hash: "0xb".into() },
            ],
            finalized_head: BlockCursor { number: 999, hash: "0xf".into() },
        }).unwrap();

        let batch = batch.unwrap();
        assert_eq!(batch.record_count(), 3); // 2 raw inserts + 1 MV insert
        assert_eq!(db.latest_block(), 1001);
        assert_eq!(db.finalized_block(), 999);
    }

    #[test]
    fn ingest_stores_block_hashes_and_cursor() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        db.ingest(IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(100.0)),
                    ("block_number".to_string(), Value::UInt64(1000)),
                ])],
            )]),
            rollback_chain: vec![
                BlockCursor { number: 1000, hash: "0xabc".into() },
            ],
            finalized_head: BlockCursor { number: 999, hash: "0xfin".into() },
        }).unwrap();

        // Cursor should have the latest block's hash
        let cursor = db.latest_cursor().unwrap();
        assert_eq!(cursor.number, 1000);
        assert_eq!(cursor.hash, "0xabc");
    }

    #[test]
    fn ingest_errors_on_missing_block_number() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        let result = db.ingest(IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(100.0)),
                    // no block_number!
                ])],
            )]),
            rollback_chain: vec![],
            finalized_head: BlockCursor { number: 0, hash: "0x0".into() },
        });

        assert!(result.is_err());
    }

    #[test]
    fn ingest_persists_and_restores_state() {
        let dir = tempfile::tempdir().unwrap();
        let schema = SIMPLE_SCHEMA;

        // Ingest some data
        {
            let mut db = DeltaDb::open(
                Config::with_data_dir(schema, dir.path().to_str().unwrap()),
            ).unwrap();

            db.ingest(IngestInput {
                data: std::collections::HashMap::from([(
                    "swaps".to_string(),
                    vec![HashMap::from([
                        ("pool".to_string(), Value::String("ETH/USDC".into())),
                        ("amount".to_string(), Value::Float64(100.0)),
                        ("block_number".to_string(), Value::UInt64(1000)),
                    ])],
                )]),
                rollback_chain: vec![
                    BlockCursor { number: 1000, hash: "0xabc".into() },
                ],
                finalized_head: BlockCursor { number: 999, hash: "0xfin".into() },
            }).unwrap();
        }

        // Reopen and verify state was restored
        {
            let db = DeltaDb::open(
                Config::with_data_dir(schema, dir.path().to_str().unwrap()),
            ).unwrap();

            assert_eq!(db.latest_block(), 1000);
            assert_eq!(db.finalized_block(), 999);

            let cursor = db.latest_cursor().unwrap();
            assert_eq!(cursor.number, 1000);
            assert_eq!(cursor.hash, "0xabc");
        }
    }

    #[test]
    fn resolve_fork_cursor_finds_common_ancestor() {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

        db.ingest(IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![
                    HashMap::from([
                        ("pool".to_string(), Value::String("ETH/USDC".into())),
                        ("amount".to_string(), Value::Float64(100.0)),
                        ("block_number".to_string(), Value::UInt64(100)),
                    ]),
                    HashMap::from([
                        ("pool".to_string(), Value::String("ETH/USDC".into())),
                        ("amount".to_string(), Value::Float64(200.0)),
                        ("block_number".to_string(), Value::UInt64(101)),
                    ]),
                ],
            )]),
            rollback_chain: vec![
                BlockCursor { number: 100, hash: "0xa".into() },
                BlockCursor { number: 101, hash: "0xb".into() },
            ],
            finalized_head: BlockCursor { number: 99, hash: "0xf".into() },
        }).unwrap();

        // Portal says block 101 has different hash, but 100 matches
        let previous_blocks = vec![(101, "0xdifferent"), (100, "0xa")];
        let fork_cursor = db.resolve_fork_cursor(&previous_blocks).unwrap();
        assert_eq!(fork_cursor.number, 100);
        assert_eq!(fork_cursor.hash, "0xa");

        // No match at all
        let previous_blocks = vec![(101, "0xnope"), (100, "0xnope")];
        assert!(db.resolve_fork_cursor(&previous_blocks).is_none());

        // Finalized head acts as fallback anchor
        let previous_blocks = vec![(101, "0xnope"), (99, "0xf")];
        let fork_cursor = db.resolve_fork_cursor(&previous_blocks).unwrap();
        assert_eq!(fork_cursor.number, 99);
    }

    #[test]
    fn partial_rollback_emits_update_not_delete() {
        // When a wallet has data across multiple blocks and only some are
        // rolled back, the MV group should emit Update (not Delete).
        let schema = r#"
            CREATE TABLE transfers (
                wallet String,
                amount Float64
            );

            CREATE MATERIALIZED VIEW wallet_volume AS
            SELECT
                wallet,
                sum(amount) AS total_volume,
                count() AS tx_count
            FROM transfers
            GROUP BY wallet;
        "#;

        let mut db = DeltaDb::open(Config::new(schema)).unwrap();

        db.process_batch("transfers", 1000, vec![
            HashMap::from([
                ("wallet".to_string(), Value::String("alice".to_string())),
                ("amount".to_string(), Value::Float64(100.0)),
            ]),
        ]).unwrap();
        db.process_batch("transfers", 1001, vec![
            HashMap::from([
                ("wallet".to_string(), Value::String("alice".to_string())),
                ("amount".to_string(), Value::Float64(200.0)),
            ]),
        ]).unwrap();
        db.flush();

        // Rollback only block 1001
        db.rollback(1000).unwrap();

        let batch = db.flush().unwrap();

        let mv_records: Vec<_> = batch.records_for("wallet_volume").iter()
            .collect();
        assert_eq!(mv_records.len(), 1);
        assert_eq!(mv_records[0].operation, DeltaOperation::Update);
        assert_eq!(mv_records[0].values.get("total_volume"), Some(&Value::Float64(100.0)));
        assert_eq!(mv_records[0].values.get("tx_count"), Some(&Value::UInt64(1)));
    }

    #[test]
    fn crash_recovery_replays_unfinalized_blocks() {
        // Full pipeline with reducer: ingest blocks, finalize some,
        // reopen (simulating crash), verify reducer/MV state is rebuilt
        // from raw rows and can continue processing correctly.
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: ingest blocks 1000-1002, finalize up to 1000
        {
            let mut db = DeltaDb::open(
                Config::with_data_dir(DEX_SCHEMA, dir.path().to_str().unwrap()),
            ).unwrap();

            db.ingest(IngestInput {
                data: std::collections::HashMap::from([(
                    "trades".to_string(),
                    vec![
                        // Block 1000: alice buys 10 @ 2000
                        {
                            let mut r = make_trade("alice", "buy", 10.0, 2000.0);
                            r.insert("block_number".into(), Value::UInt64(1000));
                            r
                        },
                        // Block 1001: alice buys 5 @ 2100
                        {
                            let mut r = make_trade("alice", "buy", 5.0, 2100.0);
                            r.insert("block_number".into(), Value::UInt64(1001));
                            r
                        },
                        // Block 1002: alice buys 3 @ 2200
                        {
                            let mut r = make_trade("alice", "buy", 3.0, 2200.0);
                            r.insert("block_number".into(), Value::UInt64(1002));
                            r
                        },
                    ],
                )]),
                rollback_chain: vec![
                    BlockCursor { number: 1000, hash: "0xa".into() },
                    BlockCursor { number: 1001, hash: "0xb".into() },
                    BlockCursor { number: 1002, hash: "0xc".into() },
                ],
                finalized_head: BlockCursor { number: 1000, hash: "0xa".into() },
            }).unwrap();

            assert_eq!(db.latest_block(), 1002);
            assert_eq!(db.finalized_block(), 1000);
        }
        // db dropped — simulates crash

        // Phase 2: reopen and verify state was rebuilt
        {
            let mut db = DeltaDb::open(
                Config::with_data_dir(DEX_SCHEMA, dir.path().to_str().unwrap()),
            ).unwrap();

            assert_eq!(db.latest_block(), 1002);
            assert_eq!(db.finalized_block(), 1000);

            // Process block 1003: alice sells 5 @ 2300
            // This requires correct reducer state from blocks 1000-1002:
            //   qty = 10 + 5 + 3 = 18, cost = 20000 + 10500 + 6600 = 37100
            //   avg_cost = 37100/18 ≈ 2061.11
            //   pnl = 5 * (2300 - 2061.11) = 1194.44
            db.process_batch("trades", 1003, vec![
                make_trade("alice", "sell", 5.0, 2300.0),
            ]).unwrap();

            let batch = db.flush().unwrap();

            let mv_records: Vec<_> = batch.records_for("position_summary").iter()
                .collect();
            assert_eq!(mv_records.len(), 1);

            // trade_count: 3 replayed + 1 new = 4
            assert_eq!(mv_records[0].values.get("trade_count"), Some(&Value::UInt64(4)));

            // current_position: 18 - 5 = 13
            assert_eq!(mv_records[0].values.get("current_position"), Some(&Value::Float64(13.0)));

            // total_pnl: 0 + 0 + 0 + 5*(2300 - 37100/18) ≈ 1194.44
            let total_pnl = mv_records[0].values.get("total_pnl").unwrap().as_f64().unwrap();
            assert!((total_pnl - 1194.44).abs() < 1.0);
        }
    }
}
