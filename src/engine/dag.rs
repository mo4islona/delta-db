use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::schema::ast::Schema;
use crate::storage::StorageBackend;
use crate::storage::StorageWriteBatch;
use crate::types::{BlockCursor, BlockNumber, DeltaBatch, DeltaRecord, RowMap};

use super::mv::MVEngine;
use super::raw_table::RawTableEngine;
use super::reducer::ReducerEngine;

/// Processing order node — topologically sorted.
#[derive(Debug)]
enum PipelineNode {
    RawTable(String),
    Reducer(String),
    MV(String),
}

/// Top-level engine that wires the computation DAG:
/// Raw Tables → Reducers → Materialized Views
pub struct DeltaEngine {
    raw_tables: HashMap<String, RawTableEngine>,
    reducers: HashMap<String, ReducerEngine>,
    mvs: HashMap<String, MVEngine>,
    /// Topologically sorted processing order.
    pipeline: Vec<PipelineNode>,
    /// Sequence number for delta batches.
    sequence: u64,
    /// Latest processed block number (for ordering/rollback logic).
    latest_block: BlockNumber,
    /// Finalized block number (for finalization logic).
    finalized_block: BlockNumber,
    /// Block number → hash for all known blocks.
    /// Populated by set_rollback_chain(). Used for fork resolution and cursors.
    block_hashes: BTreeMap<BlockNumber, String>,
}

impl DeltaEngine {
    /// Build the engine from a parsed schema and storage backend.
    pub fn new(schema: &Schema, storage: Arc<dyn StorageBackend>) -> Self {
        let mut raw_tables = HashMap::new();
        let mut reducers = HashMap::new();
        let mut mvs = HashMap::new();

        for table_def in &schema.tables {
            raw_tables.insert(
                table_def.name.clone(),
                RawTableEngine::new(table_def.clone(), storage.clone()),
            );
        }

        for reducer_def in &schema.reducers {
            reducers.insert(
                reducer_def.name.clone(),
                ReducerEngine::new(reducer_def.clone(), storage.clone()),
            );
        }

        for mv_def in &schema.materialized_views {
            mvs.insert(
                mv_def.name.clone(),
                MVEngine::new(mv_def.clone(), storage.clone()),
            );
        }

        let pipeline = build_pipeline(schema);

        Self {
            raw_tables,
            reducers,
            mvs,
            pipeline,
            sequence: 0,
            latest_block: 0,
            finalized_block: 0,
            block_hashes: BTreeMap::new(),
        }
    }

    /// Process a batch of rows for a raw table at the given block.
    /// Cascades through reducers and MVs, returning all delta records.
    pub fn process_batch(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
    ) -> Result<Vec<DeltaRecord>> {
        if !self.raw_tables.contains_key(table) {
            return Err(Error::InvalidOperation(format!("unknown table: {table}")));
        }

        let mut all_deltas = Vec::new();

        // Accumulated output rows per source name, to be consumed by downstream nodes.
        // Uses RowMaps throughout — no Row conversion needed for pipeline processing.
        let mut output_rows: HashMap<String, Vec<RowMap>> = HashMap::new();
        output_rows.insert(table.to_string(), row_maps);

        for node in &self.pipeline {
            match node {
                PipelineNode::RawTable(name) => {
                    if let Some(maps) = output_rows.get(name) {
                        let raw_eng = self.raw_tables.get(name).unwrap();
                        let deltas = raw_eng.ingest(block, maps)?;
                        all_deltas.extend(deltas);
                    }
                }
                PipelineNode::Reducer(name) => {
                    let reducer = self.reducers.get_mut(name).unwrap();
                    let source = reducer.source().to_string();

                    if let Some(source_rows) = output_rows.get(&source) {
                        let enriched = reducer.process_block(block, source_rows)?;
                        if !enriched.is_empty() {
                            output_rows.insert(name.clone(), enriched);
                        }
                    }
                }
                PipelineNode::MV(name) => {
                    let mv = self.mvs.get_mut(name).unwrap();
                    let source = mv.source().to_string();

                    if let Some(source_rows) = output_rows.get(&source) {
                        let deltas = mv.process_block(block, source_rows);
                        all_deltas.extend(deltas);
                    }
                }
            }
        }

        if block > self.latest_block {
            self.latest_block = block;
        }

        Ok(all_deltas)
    }

    /// Replay unfinalized blocks from raw rows in storage.
    /// Used on startup to rebuild reducer/MV in-memory state after a crash.
    /// Reads raw rows for each table in [from_block, to_block] and feeds them
    /// through reducers and MVs (without re-ingesting into raw storage).
    pub fn replay_unfinalized(
        &mut self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Result<()> {
        if from_block > to_block {
            return Ok(());
        }

        // Collect all (table_name, block, row_maps) across all raw tables
        let mut all_blocks: BTreeMap<BlockNumber, HashMap<String, Vec<RowMap>>> = BTreeMap::new();

        for (table_name, raw_eng) in &self.raw_tables {
            let rows_by_block = raw_eng.get_rows(from_block, to_block)?;
            for (block, rows) in rows_by_block {
                let maps: Vec<RowMap> = rows.into_iter().map(|r| r.to_map()).collect();
                all_blocks
                    .entry(block)
                    .or_default()
                    .insert(table_name.clone(), maps);
            }
        }

        // Replay each block in order through reducers and MVs only
        for (block, tables) in all_blocks {
            for (table_name, row_maps) in tables {
                let mut output_rows: HashMap<String, Vec<RowMap>> = HashMap::new();
                output_rows.insert(table_name, row_maps);

                for node in &self.pipeline {
                    match node {
                        PipelineNode::RawTable(_) => {
                            // Skip — rows already in storage
                        }
                        PipelineNode::Reducer(name) => {
                            let reducer = self.reducers.get_mut(name).unwrap();
                            let source = reducer.source().to_string();
                            if let Some(source_rows) = output_rows.get(&source) {
                                let enriched = reducer.process_block(block, source_rows)?;
                                if !enriched.is_empty() {
                                    output_rows.insert(name.clone(), enriched);
                                }
                            }
                        }
                        PipelineNode::MV(name) => {
                            let mv = self.mvs.get_mut(name).unwrap();
                            let source = mv.source().to_string();
                            if let Some(source_rows) = output_rows.get(&source) {
                                mv.process_block(block, source_rows);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Roll back all state after fork_point.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Result<Vec<DeltaRecord>> {
        let mut all_deltas = Vec::new();

        // Roll back in reverse pipeline order
        for node in self.pipeline.iter().rev() {
            match node {
                PipelineNode::MV(name) => {
                    let mv = self.mvs.get_mut(name).unwrap();
                    let deltas = mv.rollback(fork_point);
                    all_deltas.extend(deltas);
                }
                PipelineNode::Reducer(name) => {
                    let reducer = self.reducers.get_mut(name).unwrap();
                    reducer.rollback(fork_point)?;
                }
                PipelineNode::RawTable(name) => {
                    let raw_engine = self.raw_tables.get(name).unwrap();
                    let deltas = raw_engine.rollback(fork_point)?;
                    all_deltas.extend(deltas);
                }
            }
        }

        self.latest_block = fork_point;
        // Remove hashes for rolled-back blocks
        let after = self.block_hashes.split_off(&(fork_point + 1));
        drop(after);

        Ok(all_deltas)
    }

    /// Finalize all state up to and including the given block.
    /// Reducer finalized state writes are collected into the provided batch.
    pub fn finalize(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch) {
        for node in &self.pipeline {
            match node {
                PipelineNode::Reducer(name) => {
                    let reducer = self.reducers.get_mut(name).unwrap();
                    reducer.finalize(block, batch);
                }
                PipelineNode::MV(name) => {
                    let mv = self.mvs.get_mut(name).unwrap();
                    mv.finalize(block, batch);
                }
                PipelineNode::RawTable(_) => {
                    // Raw table finalization = eviction eligibility (not implemented yet)
                }
            }
        }

        self.finalized_block = block;

        // Remove hashes for blocks below finalized (keep finalized itself as anchor)
        let old_hashes = self.block_hashes.split_off(&block);
        self.block_hashes = old_hashes;
    }

    /// Create a DeltaBatch from a set of delta records.
    pub fn make_batch(&mut self, records: Vec<DeltaRecord>) -> DeltaBatch {
        self.sequence += 1;
        DeltaBatch {
            sequence: self.sequence,
            finalized_head: self.finalized_cursor(),
            latest_head: self.latest_cursor(),
            records,
        }
    }

    pub fn latest_block(&self) -> BlockNumber {
        self.latest_block
    }

    pub fn latest_cursor(&self) -> Option<BlockCursor> {
        self.block_hashes.get(&self.latest_block).map(|hash| BlockCursor {
            number: self.latest_block,
            hash: hash.clone(),
        })
    }

    pub fn finalized_block(&self) -> BlockNumber {
        self.finalized_block
    }

    pub fn finalized_cursor(&self) -> Option<BlockCursor> {
        self.block_hashes.get(&self.finalized_block).map(|hash| BlockCursor {
            number: self.finalized_block,
            hash: hash.clone(),
        })
    }

    pub fn set_latest_block(&mut self, block: BlockNumber) {
        self.latest_block = block;
    }

    pub fn set_finalized_block(&mut self, block: BlockNumber) {
        self.finalized_block = block;
    }

    pub fn restore_block_hashes(&mut self, hashes: BTreeMap<BlockNumber, String>) {
        self.block_hashes = hashes;
    }

    pub fn block_hashes(&self) -> &BTreeMap<BlockNumber, String> {
        &self.block_hashes
    }

    /// Store block hashes from the rollback chain (unfinalized blocks)
    /// and the finalized head. Used for fork resolution.
    pub fn set_rollback_chain(&mut self, chain: &[(BlockNumber, String)]) {
        for (number, hash) in chain {
            self.block_hashes.insert(*number, hash.clone());
        }
    }

    /// Find the highest block in `previous_blocks` whose hash matches
    /// our stored hash. Returns the common ancestor as a BlockCursor.
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: &[(BlockNumber, &str)],
    ) -> Option<BlockCursor> {
        for &(number, hash) in previous_blocks {
            if self.block_hashes.get(&number).map(|h| h.as_str()) == Some(hash) {
                return Some(BlockCursor {
                    number,
                    hash: hash.to_string(),
                });
            }
        }
        None
    }
}

/// Build the topologically sorted pipeline from the schema.
fn build_pipeline(schema: &Schema) -> Vec<PipelineNode> {
    let mut pipeline = Vec::new();

    // Build dependency map: name -> sources
    let mut reducer_sources: HashMap<&str, &str> = HashMap::new();
    let mut mv_sources: HashMap<&str, &str> = HashMap::new();
    let table_names: Vec<&str> = schema.tables.iter().map(|t| t.name.as_str()).collect();

    for r in &schema.reducers {
        reducer_sources.insert(&r.name, &r.source);
    }
    for mv in &schema.materialized_views {
        mv_sources.insert(&mv.name, &mv.source);
    }

    // Phase 1: Raw tables (roots)
    for name in &table_names {
        pipeline.push(PipelineNode::RawTable(name.to_string()));
    }

    // Phase 2: Reducers that source from raw tables
    // (currently reducers always source from raw tables per RFC)
    for r in &schema.reducers {
        pipeline.push(PipelineNode::Reducer(r.name.clone()));
    }

    // Phase 3: MVs — sort by dependency
    // MVs sourcing from raw tables come before MVs sourcing from reducers
    let mut mv_from_tables = Vec::new();
    let mut mv_from_reducers = Vec::new();

    for mv in &schema.materialized_views {
        if table_names.contains(&mv.source.as_str()) {
            mv_from_tables.push(PipelineNode::MV(mv.name.clone()));
        } else {
            mv_from_reducers.push(PipelineNode::MV(mv.name.clone()));
        }
    }

    pipeline.extend(mv_from_tables);
    pipeline.extend(mv_from_reducers);

    pipeline
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::*;
    use crate::storage::memory::MemoryBackend;
    use crate::types::{ColumnType, DeltaOperation, RowMap, Value};

    fn dex_schema() -> Schema {
        Schema {
            tables: vec![TableDef {
                name: "trades".to_string(),
                columns: vec![
                    ColumnDef { name: "block_number".to_string(), column_type: ColumnType::UInt64 },
                    ColumnDef { name: "user".to_string(), column_type: ColumnType::String },
                    ColumnDef { name: "side".to_string(), column_type: ColumnType::String },
                    ColumnDef { name: "amount".to_string(), column_type: ColumnType::Float64 },
                    ColumnDef { name: "price".to_string(), column_type: ColumnType::Float64 },
                ],
            }],
            reducers: vec![ReducerDef {
                name: "pnl".to_string(),
                source: "trades".to_string(),
                group_by: vec!["user".to_string()],
                state: vec![
                    StateField {
                        name: "quantity".to_string(),
                        column_type: ColumnType::Float64,
                        default: "0".to_string(),
                    },
                    StateField {
                        name: "cost_basis".to_string(),
                        column_type: ColumnType::Float64,
                        default: "0".to_string(),
                    },
                ],
                body: ReducerBody::EventRules {
                    when_blocks: vec![
                        WhenBlock {
                            condition: Expr::BinaryOp {
                                left: Box::new(Expr::RowRef("side".into())),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Literal("buy".into())),
                            },
                            lets: vec![],
                            sets: vec![
                                ("quantity".into(), Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("quantity".into())),
                                    op: BinaryOp::Add,
                                    right: Box::new(Expr::RowRef("amount".into())),
                                }),
                                ("cost_basis".into(), Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("cost_basis".into())),
                                    op: BinaryOp::Add,
                                    right: Box::new(Expr::BinaryOp {
                                        left: Box::new(Expr::RowRef("amount".into())),
                                        op: BinaryOp::Mul,
                                        right: Box::new(Expr::RowRef("price".into())),
                                    }),
                                }),
                            ],
                            emits: vec![
                                ("trade_pnl".into(), Expr::Int(0)),
                            ],
                        },
                        WhenBlock {
                            condition: Expr::BinaryOp {
                                left: Box::new(Expr::RowRef("side".into())),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Literal("sell".into())),
                            },
                            lets: vec![
                                ("avg_cost".into(), Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("cost_basis".into())),
                                    op: BinaryOp::Div,
                                    right: Box::new(Expr::StateRef("quantity".into())),
                                }),
                            ],
                            sets: vec![
                                ("quantity".into(), Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("quantity".into())),
                                    op: BinaryOp::Sub,
                                    right: Box::new(Expr::RowRef("amount".into())),
                                }),
                                ("cost_basis".into(), Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("cost_basis".into())),
                                    op: BinaryOp::Sub,
                                    right: Box::new(Expr::BinaryOp {
                                        left: Box::new(Expr::RowRef("amount".into())),
                                        op: BinaryOp::Mul,
                                        right: Box::new(Expr::ColumnRef("avg_cost".into())),
                                    }),
                                }),
                            ],
                            emits: vec![
                                ("trade_pnl".into(), Expr::BinaryOp {
                                    left: Box::new(Expr::RowRef("amount".into())),
                                    op: BinaryOp::Mul,
                                    right: Box::new(Expr::BinaryOp {
                                        left: Box::new(Expr::RowRef("price".into())),
                                        op: BinaryOp::Sub,
                                        right: Box::new(Expr::ColumnRef("avg_cost".into())),
                                    }),
                                }),
                            ],
                        },
                    ],
                    always_emit: Some(AlwaysEmit {
                        emits: vec![
                            ("position_size".into(), Expr::StateRef("quantity".into())),
                        ],
                    }),
                },
            }],
            materialized_views: vec![MVDef {
                name: "position_summary".to_string(),
                source: "pnl".to_string(),
                select: vec![
                    SelectItem { expr: SelectExpr::Column("user".into()), alias: None },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Sum, Some("trade_pnl".into())),
                        alias: Some("total_pnl".into()),
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Last, Some("position_size".into())),
                        alias: Some("current_position".into()),
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Count, None),
                        alias: Some("trade_count".into()),
                    },
                ],
                group_by: vec!["user".into()],
            }],
        }
    }

    fn simple_mv_only_schema() -> Schema {
        Schema {
            tables: vec![TableDef {
                name: "swaps".to_string(),
                columns: vec![
                    ColumnDef { name: "pool".to_string(), column_type: ColumnType::String },
                    ColumnDef { name: "amount".to_string(), column_type: ColumnType::Float64 },
                ],
            }],
            reducers: vec![],
            materialized_views: vec![MVDef {
                name: "pool_volume".to_string(),
                source: "swaps".to_string(),
                select: vec![
                    SelectItem { expr: SelectExpr::Column("pool".into()), alias: None },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Sum, Some("amount".into())),
                        alias: Some("total".into()),
                    },
                ],
                group_by: vec!["pool".into()],
            }],
        }
    }

    fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
        HashMap::from([
            ("user".to_string(), Value::String(user.to_string())),
            ("side".to_string(), Value::String(side.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
            ("price".to_string(), Value::Float64(price)),
        ])
    }

    #[test]
    fn raw_table_to_mv_direct() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

        let rows = vec![
            HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(100.0)),
            ]),
            HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(200.0)),
            ]),
        ];

        let deltas = engine.process_batch("swaps", 1000, rows).unwrap();

        // Should have: 2 raw inserts + 1 MV insert
        let raw_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "swaps").collect();
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_volume").collect();

        assert_eq!(raw_deltas.len(), 2);
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Insert);
        assert_eq!(mv_deltas[0].values.get("total"), Some(&Value::Float64(300.0)));
    }

    #[test]
    fn full_pipeline_raw_reducer_mv() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&dex_schema(), storage);

        // Block 1000: alice buys 10 @ 2000
        let deltas = engine
            .process_batch("trades", 1000, vec![make_trade("alice", "buy", 10.0, 2000.0)])
            .unwrap();

        // Raw insert + MV insert (position_summary)
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "position_summary").collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Insert);
        assert_eq!(mv_deltas[0].values.get("trade_count"), Some(&Value::UInt64(1)));
    }

    #[test]
    fn pipeline_rollback() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

        // Block 1000
        engine.process_batch("swaps", 1000, vec![
            HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(100.0)),
            ]),
        ]).unwrap();

        // Block 1001
        engine.process_batch("swaps", 1001, vec![
            HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(200.0)),
            ]),
        ]).unwrap();

        // Rollback block 1001
        let deltas = engine.rollback(1000).unwrap();

        // MV should update back to 100
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_volume").collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Update);
        assert_eq!(mv_deltas[0].values.get("total"), Some(&Value::Float64(100.0)));

        // Raw table should emit delete delta
        let raw_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "swaps").collect();
        assert_eq!(raw_deltas.len(), 1);
        assert_eq!(raw_deltas[0].operation, DeltaOperation::Delete);
    }

    #[test]
    fn pipeline_finalize() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

        engine.process_batch("swaps", 1000, vec![
            HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(100.0)),
            ]),
        ]).unwrap();

        engine.process_batch("swaps", 1001, vec![
            HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(200.0)),
            ]),
        ]).unwrap();

        let mut batch = StorageWriteBatch::new();
        engine.finalize(1000, &mut batch);
        assert_eq!(engine.finalized_block(), 1000);

        // Rollback to 1000 should only remove block 1001
        let deltas = engine.rollback(1000).unwrap();
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_volume").collect();
        assert_eq!(mv_deltas.len(), 1);
        // After finalize(1000) + rollback(1001→1000): total should be 100
        assert_eq!(mv_deltas[0].values.get("total"), Some(&Value::Float64(100.0)));
    }

    #[test]
    fn full_pipeline_rollback_and_reingest() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&dex_schema(), storage);

        // Block 1000: alice buys 10 @ 2000
        engine.process_batch("trades", 1000, vec![make_trade("alice", "buy", 10.0, 2000.0)]).unwrap();

        // Block 1001: alice buys 5 @ 2100
        engine.process_batch("trades", 1001, vec![make_trade("alice", "buy", 5.0, 2100.0)]).unwrap();

        // Block 1002: alice sells 8 @ 2200 (will be rolled back)
        engine.process_batch("trades", 1002, vec![make_trade("alice", "sell", 8.0, 2200.0)]).unwrap();

        // Rollback block 1002
        engine.rollback(1001).unwrap();

        // Re-ingest block 1002 with different trade
        let deltas = engine.process_batch("trades", 1002, vec![make_trade("alice", "sell", 3.0, 2300.0)]).unwrap();

        // MV should get updated with new trade data
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "position_summary").collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Update);
        assert_eq!(mv_deltas[0].values.get("trade_count"), Some(&Value::UInt64(3)));
    }

    #[test]
    fn make_batch_increments_sequence() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

        let batch1 = engine.make_batch(vec![]);
        assert_eq!(batch1.sequence, 1);

        let batch2 = engine.make_batch(vec![]);
        assert_eq!(batch2.sequence, 2);
    }

    #[test]
    fn unknown_table_returns_error() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

        let result = engine.process_batch("nonexistent", 1000, vec![]);
        assert!(result.is_err());
    }
}
