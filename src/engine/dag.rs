use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use rayon::prelude::*;

use crate::error::{Error, Result};
use crate::schema::ast::Schema;
use crate::storage::StorageBackend;
use crate::storage::StorageWriteBatch;
use crate::types::{BlockCursor, BlockNumber, DeltaBatch, DeltaRecord, Row, RowMap};

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

/// An independent reducer→MV chain that can be processed in parallel.
/// Engines are stored inline to avoid HashMap extraction/reinsertion per batch.
struct PipelineBranch {
    reducer_name: String,
    reducer: ReducerEngine,
    mv_entries: Vec<(String, MVEngine)>,
}

/// Top-level engine that wires the computation DAG:
/// Raw Tables → Reducers → Materialized Views
pub struct DeltaEngine {
    raw_tables: HashMap<String, RawTableEngine>,
    reducers: HashMap<String, ReducerEngine>,
    mvs: HashMap<String, MVEngine>,
    /// Tables marked as VIRTUAL — stored but no deltas emitted.
    virtual_tables: HashSet<String>,
    /// Topologically sorted processing order.
    pipeline: Vec<PipelineNode>,
    /// MVs that source directly from raw tables (processed before branches).
    direct_mvs: Vec<String>,
    /// Independent reducer→MV branches. When len() >= 2, processed in parallel.
    branches: Vec<PipelineBranch>,
    /// Sequence number for delta batches.
    sequence: u64,
    /// Latest processed block number (for ordering/rollback logic).
    latest_block: Option<BlockNumber>,
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
            let source_registry = raw_tables
                .get(&reducer_def.source)
                .expect("reducer source table must exist")
                .registry();
            reducers.insert(
                reducer_def.name.clone(),
                ReducerEngine::new(reducer_def.clone(), storage.clone(), source_registry),
            );
        }

        for mv_def in &schema.materialized_views {
            mvs.insert(
                mv_def.name.clone(),
                MVEngine::new(mv_def.clone(), storage.clone()),
            );
        }

        let virtual_tables: HashSet<String> = schema
            .tables
            .iter()
            .filter(|t| t.virtual_table)
            .map(|t| t.name.clone())
            .collect();

        let pipeline = build_pipeline(schema);
        let (direct_mvs, branch_specs) = compute_branches(&pipeline, &reducers, &mvs);

        // Move branch engines out of HashMaps into PipelineBranch structs
        // to avoid per-batch HashMap extraction/reinsertion in parallel path.
        let branches: Vec<PipelineBranch> = branch_specs
            .into_iter()
            .map(|(reducer_name, mv_names)| {
                let reducer = reducers.remove(&reducer_name).unwrap();
                let mv_entries: Vec<_> = mv_names
                    .into_iter()
                    .map(|name| {
                        let mv = mvs.remove(&name).unwrap();
                        (name, mv)
                    })
                    .collect();
                PipelineBranch {
                    reducer_name,
                    reducer,
                    mv_entries,
                }
            })
            .collect();

        Self {
            raw_tables,
            reducers,
            mvs,
            virtual_tables,
            pipeline,
            direct_mvs,
            branches,
            sequence: 0,
            latest_block: None,
            finalized_block: 0,
            block_hashes: BTreeMap::new(),
        }
    }

    /// Process a batch of rows for a raw table at the given block.
    /// Cascades through reducers and MVs, returning all delta records.
    ///
    /// When multiple independent reducer branches exist (e.g. two reducers
    /// both sourcing from the same raw table), they are executed in parallel
    /// using rayon's thread pool.
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

        // Phase 1: Raw table ingest (uses original RowMaps for storage + DeltaRecord creation)
        let raw_eng = self.raw_tables.get(table).unwrap();
        if self.virtual_tables.contains(table) {
            raw_eng.ingest_no_deltas(block, &row_maps)?;
        } else {
            let deltas = raw_eng.ingest(block, &row_maps)?;
            all_deltas.extend(deltas);
        }

        // Output rows for downstream consumption (reducers + MVs)
        let mut output_rows: HashMap<String, Vec<RowMap>> = HashMap::new();
        output_rows.insert(table.to_string(), row_maps);

        // Check if parallel branch execution is possible:
        // - 2+ branches, and all reducers source from raw tables (not from each other)
        let can_parallelize = self.branches.len() >= 2
            && self
                .branches
                .iter()
                .all(|b| self.raw_tables.contains_key(b.reducer.source()));

        if can_parallelize {
            // Phase 2a: Process MVs that source directly from raw tables
            for mv_name in &self.direct_mvs {
                let mv = self.mvs.get_mut(mv_name).unwrap();
                let source = mv.source().to_string();
                if let Some(source_rows) = output_rows.get(&source) {
                    let deltas = mv.process_block(block, source_rows);
                    all_deltas.extend(deltas);
                }
            }

            // Phase 2b: Process reducer branches in parallel.
            if self.branches.len() == 2 {
                let (first, second) = self.branches.split_at_mut(1);
                let branch_0 = &mut first[0];
                let branch_1 = &mut second[0];

                let (result_0, result_1) = rayon::join(
                    || -> Result<Vec<DeltaRecord>> {
                        let source = branch_0.reducer.source();
                        let mut deltas = Vec::new();
                        if let Some(rows) = output_rows.get(source) {
                            let enriched = branch_0.reducer.process_block_maps(block, rows)?;
                            if !enriched.is_empty() {
                                for (_, mv) in branch_0.mv_entries.iter_mut() {
                                    deltas.extend(mv.process_block(block, &enriched));
                                }
                            }
                        }
                        Ok(deltas)
                    },
                    || -> Result<Vec<DeltaRecord>> {
                        let source = branch_1.reducer.source();
                        let mut deltas = Vec::new();
                        if let Some(rows) = output_rows.get(source) {
                            let enriched = branch_1.reducer.process_block_maps(block, rows)?;
                            if !enriched.is_empty() {
                                for (_, mv) in branch_1.mv_entries.iter_mut() {
                                    deltas.extend(mv.process_block(block, &enriched));
                                }
                            }
                        }
                        Ok(deltas)
                    },
                );

                all_deltas.extend(result_0?);
                all_deltas.extend(result_1?);
            } else {
                // General N-branch parallel using par_iter_mut
                let results: Vec<Result<Vec<DeltaRecord>>> = self
                    .branches
                    .par_iter_mut()
                    .map(|branch| {
                        let source = branch.reducer.source();
                        let mut deltas = Vec::new();
                        if let Some(rows) = output_rows.get(source) {
                            let enriched = branch.reducer.process_block_maps(block, rows)?;
                            if !enriched.is_empty() {
                                for (_, mv) in branch.mv_entries.iter_mut() {
                                    deltas.extend(mv.process_block(block, &enriched));
                                }
                            }
                        }
                        Ok(deltas)
                    })
                    .collect();

                for result in results {
                    all_deltas.extend(result?);
                }
            }
        } else {
            // Sequential execution: process branches + remaining engines
            for node in &self.pipeline {
                match node {
                    PipelineNode::RawTable(_) => {} // Already processed in Phase 1
                    PipelineNode::Reducer(name) => {
                        let enriched = if let Some(branch) =
                            self.branches.iter_mut().find(|b| b.reducer_name == *name)
                        {
                            let source = branch.reducer.source().to_string();
                            if let Some(source_rows) = output_rows.get(&source) {
                                branch.reducer.process_block_maps(block, source_rows)?
                            } else {
                                Vec::new()
                            }
                        } else {
                            let reducer = self.reducers.get_mut(name).unwrap();
                            let source = reducer.source().to_string();
                            if let Some(source_rows) = output_rows.get(&source) {
                                reducer.process_block_maps(block, source_rows)?
                            } else {
                                Vec::new()
                            }
                        };
                        if !enriched.is_empty() {
                            output_rows.insert(name.clone(), enriched);
                        }
                    }
                    PipelineNode::MV(name) => {
                        // MVs consume RowMaps from output_rows
                        let found_in_branch = self.branches.iter_mut().any(|branch| {
                            if let Some((_, mv)) =
                                branch.mv_entries.iter_mut().find(|(n, _)| n == name)
                            {
                                let source = mv.source().to_string();
                                if let Some(source_rows) = output_rows.get(&source) {
                                    let deltas = mv.process_block(block, source_rows);
                                    all_deltas.extend(deltas);
                                }
                                true
                            } else {
                                false
                            }
                        });
                        if !found_in_branch {
                            let mv = self.mvs.get_mut(name).unwrap();
                            let source = mv.source().to_string();
                            if let Some(source_rows) = output_rows.get(&source) {
                                let deltas = mv.process_block(block, source_rows);
                                all_deltas.extend(deltas);
                            }
                        }
                    }
                }
            }
        }

        if self.latest_block.is_none_or(|b| block > b) {
            self.latest_block = Some(block);
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

        // Collect all (table_name, block, rows) across all raw tables.
        // Rows are stored as Vec<Row> — no to_map() conversion needed!
        let mut all_blocks: BTreeMap<BlockNumber, HashMap<String, Vec<Row>>> = BTreeMap::new();

        for (table_name, raw_eng) in &self.raw_tables {
            let rows_by_block = raw_eng.get_rows(from_block, to_block)?;
            for (block, rows) in rows_by_block {
                all_blocks
                    .entry(block)
                    .or_default()
                    .insert(table_name.clone(), rows);
            }
        }

        // Replay each block in order through reducers and MVs only
        for (block, tables) in all_blocks {
            for (table_name, rows) in tables {
                // Row cache for reducer input (indexed access)
                let mut row_cache: HashMap<String, Vec<Row>> = HashMap::new();
                row_cache.insert(table_name, rows);

                // Output from reducers (RowMaps for MV consumption)
                let mut output_rows: HashMap<String, Vec<RowMap>> = HashMap::new();

                for node in &self.pipeline {
                    match node {
                        PipelineNode::RawTable(_) => {
                            // Skip — rows already in storage
                        }
                        PipelineNode::Reducer(name) => {
                            let enriched = if let Some(branch) =
                                self.branches.iter_mut().find(|b| b.reducer_name == *name)
                            {
                                let source = branch.reducer.source().to_string();
                                if let Some(source_rows) = row_cache.get(&source) {
                                    branch.reducer.process_block(block, source_rows)?
                                } else {
                                    Vec::new()
                                }
                            } else {
                                let reducer = self.reducers.get_mut(name).unwrap();
                                let source = reducer.source().to_string();
                                if let Some(source_rows) = row_cache.get(&source) {
                                    reducer.process_block(block, source_rows)?
                                } else {
                                    Vec::new()
                                }
                            };
                            if !enriched.is_empty() {
                                output_rows.insert(name.clone(), enriched);
                            }
                        }
                        PipelineNode::MV(name) => {
                            let mut found = false;
                            for branch in &mut self.branches {
                                if let Some((_, mv)) =
                                    branch.mv_entries.iter_mut().find(|(n, _)| n == name)
                                {
                                    let source = mv.source().to_string();
                                    if let Some(source_rows) = output_rows.get(&source) {
                                        mv.process_block(block, source_rows);
                                    }
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
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
                    // Check branch MVs first, then HashMap
                    let mut found = false;
                    for branch in &mut self.branches {
                        if let Some((_, mv)) = branch.mv_entries.iter_mut().find(|(n, _)| n == name)
                        {
                            all_deltas.extend(mv.rollback(fork_point));
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        let mv = self.mvs.get_mut(name).unwrap();
                        all_deltas.extend(mv.rollback(fork_point));
                    }
                }
                PipelineNode::Reducer(name) => {
                    // Check branches first, then HashMap
                    if let Some(branch) = self.branches.iter_mut().find(|b| b.reducer_name == *name)
                    {
                        branch.reducer.rollback(fork_point)?;
                    } else {
                        let reducer = self.reducers.get_mut(name).unwrap();
                        reducer.rollback(fork_point)?;
                    }
                }
                PipelineNode::RawTable(name) => {
                    let raw_engine = self.raw_tables.get(name).unwrap();
                    let deltas = raw_engine.rollback(fork_point)?;
                    if !self.virtual_tables.contains(name) {
                        all_deltas.extend(deltas);
                    }
                }
            }
        }

        self.latest_block = Some(fork_point);
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
                    if let Some(branch) = self.branches.iter_mut().find(|b| b.reducer_name == *name)
                    {
                        branch.reducer.finalize(block, batch);
                    } else {
                        let reducer = self.reducers.get_mut(name).unwrap();
                        reducer.finalize(block, batch);
                    }
                }
                PipelineNode::MV(name) => {
                    let mut found = false;
                    for branch in &mut self.branches {
                        if let Some((_, mv)) = branch.mv_entries.iter_mut().find(|(n, _)| n == name)
                        {
                            mv.finalize(block, batch);
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        let mv = self.mvs.get_mut(name).unwrap();
                        mv.finalize(block, batch);
                    }
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
        // Group records by table name
        let mut tables: HashMap<String, Vec<DeltaRecord>> = HashMap::new();
        for record in records {
            tables.entry(record.table.clone()).or_default().push(record);
        }
        DeltaBatch {
            sequence: self.sequence,
            finalized_head: self.finalized_cursor(),
            latest_head: self.latest_cursor(),
            tables,
        }
    }

    pub fn latest_block(&self) -> BlockNumber {
        self.latest_block.unwrap_or(0)
    }

    pub fn latest_cursor(&self) -> Option<BlockCursor> {
        let block = self.latest_block?;
        let hash = self
            .block_hashes
            .get(&block)
            .cloned()
            .unwrap_or_default();
        Some(BlockCursor {
            number: block,
            hash,
        })
    }

    pub fn finalized_block(&self) -> BlockNumber {
        self.finalized_block
    }

    pub fn finalized_cursor(&self) -> Option<BlockCursor> {
        self.block_hashes
            .get(&self.finalized_block)
            .map(|hash| BlockCursor {
                number: self.finalized_block,
                hash: hash.clone(),
            })
    }

    pub fn set_latest_block(&mut self, block: BlockNumber) {
        self.latest_block = Some(block);
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

/// Identify independent branches and direct MVs from the pipeline.
///
/// A branch is a reducer + its downstream MVs. Branches that all source
/// from raw tables (not from each other) can be executed in parallel.
/// Direct MVs source from raw tables and are processed before branches.
///
/// Returns (direct_mv_names, Vec<(reducer_name, mv_names)>).
fn compute_branches(
    pipeline: &[PipelineNode],
    reducers: &HashMap<String, ReducerEngine>,
    mvs: &HashMap<String, MVEngine>,
) -> (Vec<String>, Vec<(String, Vec<String>)>) {
    let reducer_names: HashSet<&str> = reducers.keys().map(|s| s.as_str()).collect();

    let mut branches = Vec::new();
    let mut direct_mvs = Vec::new();

    // Build branches: each reducer + its downstream MVs
    for node in pipeline {
        if let PipelineNode::Reducer(name) = node {
            let downstream: Vec<String> = pipeline
                .iter()
                .filter_map(|n| {
                    if let PipelineNode::MV(mv_name) = n {
                        let mv = mvs.get(mv_name).unwrap();
                        if mv.source() == name.as_str() {
                            return Some(mv_name.clone());
                        }
                    }
                    None
                })
                .collect();

            branches.push((name.clone(), downstream));
        }
    }

    // Find MVs sourcing from raw tables (not from reducers)
    for node in pipeline {
        if let PipelineNode::MV(name) = node {
            let mv = mvs.get(name).unwrap();
            if !reducer_names.contains(mv.source()) {
                direct_mvs.push(name.clone());
            }
        }
    }

    (direct_mvs, branches)
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
                    ColumnDef {
                        name: "block_number".to_string(),
                        column_type: ColumnType::UInt64,
                    },
                    ColumnDef {
                        name: "user".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "side".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        column_type: ColumnType::Float64,
                    },
                    ColumnDef {
                        name: "price".to_string(),
                        column_type: ColumnType::Float64,
                    },
                ],
                virtual_table: false,
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
                                (
                                    "quantity".into(),
                                    Expr::BinaryOp {
                                        left: Box::new(Expr::StateRef("quantity".into())),
                                        op: BinaryOp::Add,
                                        right: Box::new(Expr::RowRef("amount".into())),
                                    },
                                ),
                                (
                                    "cost_basis".into(),
                                    Expr::BinaryOp {
                                        left: Box::new(Expr::StateRef("cost_basis".into())),
                                        op: BinaryOp::Add,
                                        right: Box::new(Expr::BinaryOp {
                                            left: Box::new(Expr::RowRef("amount".into())),
                                            op: BinaryOp::Mul,
                                            right: Box::new(Expr::RowRef("price".into())),
                                        }),
                                    },
                                ),
                            ],
                            emits: vec![("trade_pnl".into(), Expr::Int(0))],
                        },
                        WhenBlock {
                            condition: Expr::BinaryOp {
                                left: Box::new(Expr::RowRef("side".into())),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Literal("sell".into())),
                            },
                            lets: vec![(
                                "avg_cost".into(),
                                Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("cost_basis".into())),
                                    op: BinaryOp::Div,
                                    right: Box::new(Expr::StateRef("quantity".into())),
                                },
                            )],
                            sets: vec![
                                (
                                    "quantity".into(),
                                    Expr::BinaryOp {
                                        left: Box::new(Expr::StateRef("quantity".into())),
                                        op: BinaryOp::Sub,
                                        right: Box::new(Expr::RowRef("amount".into())),
                                    },
                                ),
                                (
                                    "cost_basis".into(),
                                    Expr::BinaryOp {
                                        left: Box::new(Expr::StateRef("cost_basis".into())),
                                        op: BinaryOp::Sub,
                                        right: Box::new(Expr::BinaryOp {
                                            left: Box::new(Expr::RowRef("amount".into())),
                                            op: BinaryOp::Mul,
                                            right: Box::new(Expr::ColumnRef("avg_cost".into())),
                                        }),
                                    },
                                ),
                            ],
                            emits: vec![(
                                "trade_pnl".into(),
                                Expr::BinaryOp {
                                    left: Box::new(Expr::RowRef("amount".into())),
                                    op: BinaryOp::Mul,
                                    right: Box::new(Expr::BinaryOp {
                                        left: Box::new(Expr::RowRef("price".into())),
                                        op: BinaryOp::Sub,
                                        right: Box::new(Expr::ColumnRef("avg_cost".into())),
                                    }),
                                },
                            )],
                        },
                    ],
                    always_emit: Some(AlwaysEmit {
                        emits: vec![("position_size".into(), Expr::StateRef("quantity".into()))],
                    }),
                },
            }],
            materialized_views: vec![MVDef {
                name: "position_summary".to_string(),
                source: "pnl".to_string(),
                select: vec![
                    SelectItem {
                        expr: SelectExpr::Column("user".into()),
                        alias: None,
                    },
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
                    ColumnDef {
                        name: "pool".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        column_type: ColumnType::Float64,
                    },
                ],
                virtual_table: false,
            }],
            reducers: vec![],
            materialized_views: vec![MVDef {
                name: "pool_volume".to_string(),
                source: "swaps".to_string(),
                select: vec![
                    SelectItem {
                        expr: SelectExpr::Column("pool".into()),
                        alias: None,
                    },
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
        assert_eq!(
            mv_deltas[0].values.get("total"),
            Some(&Value::Float64(300.0))
        );
    }

    #[test]
    fn full_pipeline_raw_reducer_mv() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&dex_schema(), storage);

        // Block 1000: alice buys 10 @ 2000
        let deltas = engine
            .process_batch(
                "trades",
                1000,
                vec![make_trade("alice", "buy", 10.0, 2000.0)],
            )
            .unwrap();

        // Raw insert + MV insert (position_summary)
        let mv_deltas: Vec<_> = deltas
            .iter()
            .filter(|d| d.table == "position_summary")
            .collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Insert);
        assert_eq!(
            mv_deltas[0].values.get("trade_count"),
            Some(&Value::UInt64(1))
        );
    }

    #[test]
    fn pipeline_rollback() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

        // Block 1000
        engine
            .process_batch(
                "swaps",
                1000,
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(100.0)),
                ])],
            )
            .unwrap();

        // Block 1001
        engine
            .process_batch(
                "swaps",
                1001,
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(200.0)),
                ])],
            )
            .unwrap();

        // Rollback block 1001
        let deltas = engine.rollback(1000).unwrap();

        // MV should update back to 100
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_volume").collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Update);
        assert_eq!(
            mv_deltas[0].values.get("total"),
            Some(&Value::Float64(100.0))
        );

        // Raw table should emit delete delta
        let raw_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "swaps").collect();
        assert_eq!(raw_deltas.len(), 1);
        assert_eq!(raw_deltas[0].operation, DeltaOperation::Delete);
    }

    #[test]
    fn pipeline_finalize() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

        engine
            .process_batch(
                "swaps",
                1000,
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(100.0)),
                ])],
            )
            .unwrap();

        engine
            .process_batch(
                "swaps",
                1001,
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(200.0)),
                ])],
            )
            .unwrap();

        let mut batch = StorageWriteBatch::new();
        engine.finalize(1000, &mut batch);
        assert_eq!(engine.finalized_block(), 1000);

        // Rollback to 1000 should only remove block 1001
        let deltas = engine.rollback(1000).unwrap();
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_volume").collect();
        assert_eq!(mv_deltas.len(), 1);
        // After finalize(1000) + rollback(1001→1000): total should be 100
        assert_eq!(
            mv_deltas[0].values.get("total"),
            Some(&Value::Float64(100.0))
        );
    }

    #[test]
    fn full_pipeline_rollback_and_reingest() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&dex_schema(), storage);

        // Block 1000: alice buys 10 @ 2000
        engine
            .process_batch(
                "trades",
                1000,
                vec![make_trade("alice", "buy", 10.0, 2000.0)],
            )
            .unwrap();

        // Block 1001: alice buys 5 @ 2100
        engine
            .process_batch(
                "trades",
                1001,
                vec![make_trade("alice", "buy", 5.0, 2100.0)],
            )
            .unwrap();

        // Block 1002: alice sells 8 @ 2200 (will be rolled back)
        engine
            .process_batch(
                "trades",
                1002,
                vec![make_trade("alice", "sell", 8.0, 2200.0)],
            )
            .unwrap();

        // Rollback block 1002
        engine.rollback(1001).unwrap();

        // Re-ingest block 1002 with different trade
        let deltas = engine
            .process_batch(
                "trades",
                1002,
                vec![make_trade("alice", "sell", 3.0, 2300.0)],
            )
            .unwrap();

        // MV should get updated with new trade data
        let mv_deltas: Vec<_> = deltas
            .iter()
            .filter(|d| d.table == "position_summary")
            .collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Update);
        assert_eq!(
            mv_deltas[0].values.get("trade_count"),
            Some(&Value::UInt64(3))
        );
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
