use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::schema::ast::{AggFunc, MVDef, SelectExpr, SelectItem, SlidingWindowDef};
use crate::storage::{self, StorageBackend, StorageWriteBatch};
use crate::types::{BlockNumber, DeltaOperation, DeltaRecord, GroupKey, RowMap, Value};

use super::aggregation::{create_agg, restore_agg, to_start_of_interval, AggregationFunc};

/// Describes one output column of the MV.
#[derive(Debug)]
enum OutputColumn {
    /// A pass-through GROUP BY column (value comes from the group key).
    GroupBy { source_col: String, output_name: String },
    /// A time-window GROUP BY column.
    Window { source_col: String, interval_seconds: u64, output_name: String },
    /// An aggregation column.
    Agg { source_col: Option<String>, agg_index: usize, output_name: String },
}

/// Pre-computed info for feeding a single aggregation from a source row.
struct AggFeedInfo {
    source_col: Option<String>,
    agg_index: usize,
}

/// Pre-computed group key extraction — avoids per-row pattern matching on OutputColumn.
enum GroupKeyExtractor {
    Column(String),
    Window(String, u64),
}

/// Manages a single materialized view: GROUP BY routing, aggregation, rollback, deltas.
pub struct MVEngine {
    def: MVDef,
    /// The output column descriptors (in SELECT order).
    output_columns: Vec<OutputColumn>,
    /// The AggFunc types in SELECT order (for deserialization).
    agg_funcs: Vec<AggFunc>,
    /// Number of aggregation functions per group.
    agg_count: usize,
    /// Pre-computed list of agg columns for fast row feeding (avoids per-row pattern matching).
    agg_feeds: Vec<AggFeedInfo>,
    /// Pre-computed group key extractors (avoids per-row pattern matching on OutputColumn).
    group_key_extractors: Vec<GroupKeyExtractor>,
    /// group_key -> aggregation state (one AggregationFunc per agg column).
    groups: FxHashMap<GroupKey, Vec<Box<dyn AggregationFunc>>>,
    /// Tracks which blocks have been ingested (for rollback).
    /// block -> set of group keys touched. BTreeMap for O(log N) range queries.
    block_groups: BTreeMap<BlockNumber, FxHashSet<GroupKey>>,
    /// Snapshot of previous output values per group key, for delta computation.
    prev_output: FxHashMap<GroupKey, HashMap<String, Value>>,
    /// Storage backend for persisting finalized MV state.
    storage: Arc<dyn StorageBackend>,
    /// Sliding window configuration (None for tumbling/non-windowed MVs).
    sliding_window: Option<SlidingWindowDef>,
    /// Block number → max timestamp (ms) seen in that block.
    /// Only populated when sliding_window is Some.
    block_times: BTreeMap<BlockNumber, i64>,
    /// The maximum timestamp seen across all blocks (the "watermark").
    current_watermark: i64,
    /// Group keys removed since last finalize (for storage cleanup).
    removed_groups: Vec<GroupKey>,
}

impl MVEngine {
    pub fn new(def: MVDef, storage: Arc<dyn StorageBackend>) -> Self {
        let mut output_columns = Vec::new();
        let mut agg_funcs = Vec::new();
        let mut agg_index = 0usize;

        for item in &def.select {
            let output_name = resolve_output_name(item);
            match &item.expr {
                SelectExpr::Column(col) => {
                    output_columns.push(OutputColumn::GroupBy {
                        source_col: col.clone(),
                        output_name,
                    });
                }
                SelectExpr::WindowFunc { column, interval_seconds } => {
                    output_columns.push(OutputColumn::Window {
                        source_col: column.clone(),
                        interval_seconds: *interval_seconds,
                        output_name,
                    });
                }
                SelectExpr::Agg(func, source_col) => {
                    output_columns.push(OutputColumn::Agg {
                        source_col: source_col.clone(),
                        agg_index,
                        output_name,
                    });
                    agg_funcs.push(func.clone());
                    agg_index += 1;
                }
            }
        }

        let agg_count = agg_index;

        // Pre-compute agg feed info for fast row processing
        let agg_feeds: Vec<AggFeedInfo> = output_columns
            .iter()
            .filter_map(|col| {
                if let OutputColumn::Agg {
                    source_col,
                    agg_index,
                    ..
                } = col
                {
                    Some(AggFeedInfo {
                        source_col: source_col.clone(),
                        agg_index: *agg_index,
                    })
                } else {
                    None
                }
            })
            .collect();

        // Pre-compute group key extractors for fast group key computation
        let group_key_extractors: Vec<GroupKeyExtractor> = output_columns
            .iter()
            .filter_map(|col| match col {
                OutputColumn::GroupBy { source_col, .. } => {
                    Some(GroupKeyExtractor::Column(source_col.clone()))
                }
                OutputColumn::Window { source_col, interval_seconds, .. } => {
                    Some(GroupKeyExtractor::Window(source_col.clone(), *interval_seconds))
                }
                OutputColumn::Agg { .. } => None,
            })
            .collect();

        // Restore finalized MV state from storage
        let mut groups: FxHashMap<GroupKey, Vec<Box<dyn AggregationFunc>>> = FxHashMap::default();
        let mut prev_output: FxHashMap<GroupKey, HashMap<String, Value>> = FxHashMap::default();

        if let Ok(group_keys) = storage.list_mv_group_keys(&def.name) {
            for gk_bytes in group_keys {
                if let Ok(Some(state_bytes)) = storage.get_mv_state(&def.name, &gk_bytes) {
                    if let Some((aggs, prev)) =
                        deserialize_mv_group(&state_bytes, &agg_funcs)
                    {
                        let group_key = storage::decode_group_key(&gk_bytes);
                        if let Some(prev) = prev {
                            prev_output.insert(group_key.clone(), prev);
                        }
                        groups.insert(group_key, aggs);
                    }
                }
            }
        }

        let sliding_window = def.sliding_window.clone();
        let mut block_times: BTreeMap<BlockNumber, i64> = BTreeMap::new();
        let mut block_groups: BTreeMap<BlockNumber, FxHashSet<GroupKey>> = BTreeMap::new();

        // Restore sliding window metadata from storage
        if sliding_window.is_some() {
            let meta_key = format!("mv_block_times:{}", def.name);
            if let Ok(Some(bt_bytes)) = storage.get_meta(&meta_key) {
                if let Ok(bt) = rmp_serde::from_slice::<BTreeMap<BlockNumber, i64>>(&bt_bytes) {
                    block_times = bt;
                }
            }

            // Rebuild block_groups from restored agg state (union of all aggs'
            // block numbers, since different agg types may track different blocks)
            for (group_key, aggs) in &groups {
                for agg in aggs {
                    for block in agg.block_numbers() {
                        block_groups
                            .entry(block)
                            .or_default()
                            .insert(group_key.clone());
                    }
                }
            }
        }

        let current_watermark = block_times.values().copied().max().unwrap_or(0);

        MVEngine {
            def,
            output_columns,
            agg_funcs,
            agg_count,
            agg_feeds,
            group_key_extractors,
            groups,
            block_groups,
            prev_output,
            storage,
            sliding_window,
            block_times,
            current_watermark,
            removed_groups: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn source(&self) -> &str {
        &self.def.source
    }

    /// Process a batch of rows from a single block.
    /// Returns delta records for new/updated groups.
    pub fn process_block(&mut self, block: BlockNumber, rows: &[RowMap]) -> Vec<DeltaRecord> {
        // Sliding window replay protection: skip blocks already in restored state
        if self.sliding_window.is_some() && self.block_times.contains_key(&block) {
            return Vec::new();
        }

        // Track block timestamp for sliding windows
        if let Some(ref sw) = self.sliding_window {
            let block_max_ts = rows
                .iter()
                .filter_map(|r| r.get(&sw.time_column).and_then(|v| v.as_i64()))
                .max()
                // Fallback: if no rows have a valid timestamp, use current watermark
                // so the block still participates in expiry rather than leaking.
                .unwrap_or(self.current_watermark);
            self.block_times.insert(block, block_max_ts);
            if block_max_ts > self.current_watermark {
                self.current_watermark = block_max_ts;
            }
        }

        // Snapshot current output for touched groups before mutation
        let mut touched_keys: FxHashSet<GroupKey> = FxHashSet::default();

        for row in rows {
            let group_key = self.compute_group_key(row);

            // Snapshot prev output before first mutation of this group in this call
            if !touched_keys.contains(&group_key) {
                let prev = self.compute_output(&group_key);
                if let Some(prev) = prev {
                    self.prev_output.insert(group_key.clone(), prev);
                }
                touched_keys.insert(group_key.clone());
            }

            // Ensure group exists
            if !self.groups.contains_key(&group_key) {
                self.groups.insert(group_key.clone(), self.create_agg_vec());
            }
            let aggs = self.groups.get_mut(&group_key).unwrap();

            // Feed values to each aggregation using pre-computed agg info
            for feed in &self.agg_feeds {
                let value = match feed.source_col.as_deref() {
                    Some(col) => row.get(col).cloned().unwrap_or(Value::Null),
                    None => Value::UInt64(1),
                };
                aggs[feed.agg_index].add_block(block, std::slice::from_ref(&value));
            }

            // Track block -> group key mapping for rollback
            self.block_groups
                .entry(block)
                .or_default()
                .insert(group_key);
        }

        // Expire old blocks for sliding windows
        if self.sliding_window.is_some() {
            let expired_keys = self.expire_old_blocks();
            touched_keys.extend(expired_keys);
        }

        // Emit deltas for all touched groups
        self.emit_deltas(&touched_keys)
    }

    /// Roll back all blocks after fork_point.
    /// Returns compensating delta records.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Vec<DeltaRecord> {
        // Use BTreeMap range to efficiently find blocks > fork_point
        let rolled_back = self.block_groups.split_off(&(fork_point + 1));

        if rolled_back.is_empty() {
            return Vec::new();
        }

        // Clean up sliding window state for rolled-back blocks
        if self.sliding_window.is_some() {
            drop(self.block_times.split_off(&(fork_point + 1)));
            self.current_watermark = self.block_times.values().copied().max().unwrap_or(0);
        }

        // Collect all group keys affected by rolled-back blocks (consume by value)
        let mut touched_keys: FxHashSet<GroupKey> = FxHashSet::default();
        for (_block, keys) in rolled_back {
            for key in keys {
                touched_keys.insert(key);
            }
        }

        // Snapshot prev output before mutation
        for key in &touched_keys {
            let prev = self.compute_output(key);
            if let Some(prev) = prev {
                self.prev_output.insert(key.clone(), prev);
            }
        }

        // Batch-remove blocks from aggregations: one split_off per group key
        for key in &touched_keys {
            if let Some(aggs) = self.groups.get_mut(key) {
                for agg in aggs.iter_mut() {
                    agg.remove_blocks_after(fork_point);
                }
            }
        }

        // Emit deltas (updates or deletes)
        self.emit_deltas(&touched_keys)
    }

    /// Finalize all blocks up to and including the given block.
    /// Persists finalized aggregation state to the batch for atomic commit.
    pub fn finalize(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch) {
        let is_sliding = self.sliding_window.is_some();

        if !is_sliding {
            // Standard path: merge per-block data into finalized state
            for aggs in self.groups.values_mut() {
                for agg in aggs.iter_mut() {
                    agg.finalize_up_to(block);
                }
            }
        }
        // For sliding windows: do NOT call finalize_up_to — keep per-block data

        // Persist state for all groups
        for (group_key, aggs) in &self.groups {
            let gk_bytes = storage::encode_group_key(group_key);
            let prev = self.prev_output.get(group_key);
            let state_bytes = if is_sliding {
                serialize_mv_group_full(aggs, prev)
            } else {
                serialize_mv_group(aggs, prev)
            };
            batch.put_mv_state(&self.def.name, &gk_bytes, &state_bytes);
        }

        // Persist block_times for sliding windows
        if is_sliding {
            let bt_bytes = rmp_serde::to_vec(&self.block_times)
                .expect("block_times serialization should not fail");
            batch.put_meta(&format!("mv_block_times:{}", self.def.name), &bt_bytes);
        }

        // Delete stale groups from storage (expired/removed since last finalize)
        for key in self.removed_groups.drain(..) {
            let gk_bytes = storage::encode_group_key(&key);
            batch.delete_mv_state(&self.def.name, &gk_bytes);
        }

        if !is_sliding {
            // Remove finalized blocks from tracking using split_off
            let remaining = self.block_groups.split_off(&(block + 1));
            self.block_groups = remaining;
        }
        // For sliding windows: block_groups pruning is handled by expire_old_blocks
    }

    /// Remove blocks whose timestamps have fallen outside the sliding window.
    /// Returns the set of group keys affected by expiry.
    fn expire_old_blocks(&mut self) -> FxHashSet<GroupKey> {
        let sw = self.sliding_window.as_ref().unwrap();
        let window_ms = (sw.interval_seconds as i64).saturating_mul(1000);
        let cutoff = self.current_watermark - window_ms;

        // Find all blocks with timestamp < cutoff (strict less-than: boundary is inclusive).
        // Scan is bounded by window size since expired blocks are removed each round.
        let expired_blocks: Vec<BlockNumber> = self
            .block_times
            .iter()
            .filter(|(_, ts)| **ts < cutoff)
            .map(|(block, _)| *block)
            .collect();

        if expired_blocks.is_empty() {
            return FxHashSet::default();
        }

        let mut expired_keys: FxHashSet<GroupKey> = FxHashSet::default();

        for &block in &expired_blocks {
            if let Some(keys) = self.block_groups.remove(&block) {
                for key in keys {
                    // Snapshot prev output before first mutation of this group by expiry.
                    // If already in prev_output (touched by row processing), skip.
                    if !self.prev_output.contains_key(&key) {
                        if let Some(prev) = self.compute_output(&key) {
                            self.prev_output.insert(key.clone(), prev);
                        }
                    }

                    // Remove block's contribution from all aggs for this group
                    if let Some(aggs) = self.groups.get_mut(&key) {
                        for agg in aggs.iter_mut() {
                            agg.remove_block(block);
                        }
                    }

                    expired_keys.insert(key);
                }
            }

            self.block_times.remove(&block);
        }

        expired_keys
    }

    fn compute_group_key(&self, row: &RowMap) -> GroupKey {
        let mut key = GroupKey::new();
        for ext in &self.group_key_extractors {
            match ext {
                GroupKeyExtractor::Column(source_col) => {
                    let v = row.get(source_col.as_str()).cloned().unwrap_or(Value::Null);
                    key.push(v);
                }
                GroupKeyExtractor::Window(source_col, interval_seconds) => {
                    let ts = row
                        .get(source_col.as_str())
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let window_start = to_start_of_interval(ts, *interval_seconds);
                    key.push(Value::DateTime(window_start));
                }
            }
        }
        key
    }

    fn compute_output(&self, group_key: &GroupKey) -> Option<HashMap<String, Value>> {
        let aggs = self.groups.get(group_key)?;
        let mut output = HashMap::with_capacity(self.output_columns.len());

        let mut key_idx = 0;
        for col in &self.output_columns {
            match col {
                OutputColumn::GroupBy { output_name, .. }
                | OutputColumn::Window { output_name, .. } => {
                    output.insert(output_name.clone(), group_key[key_idx].clone());
                    key_idx += 1;
                }
                OutputColumn::Agg { agg_index, output_name, .. } => {
                    output.insert(output_name.clone(), aggs[*agg_index].current_value());
                }
            }
        }

        Some(output)
    }

    fn emit_deltas(&mut self, touched_keys: &FxHashSet<GroupKey>) -> Vec<DeltaRecord> {
        let mut deltas = Vec::new();

        for key in touched_keys {
            let prev = self.prev_output.remove(key);
            let current = self.compute_output(key);

            // Check if group is now empty (all aggs have no data)
            let is_empty = self
                .groups
                .get(key)
                .map(|aggs| aggs.iter().all(|a| !a.has_data()))
                .unwrap_or(true);

            let delta_key = self.build_delta_key(key);

            match (prev, is_empty) {
                (None, false) => {
                    // New group -> Insert
                    if let Some(values) = current {
                        deltas.push(DeltaRecord {
                            table: self.def.name.clone(),
                            operation: DeltaOperation::Insert,
                            key: delta_key,
                            values,
                            prev_values: None,
                        });
                    }
                }
                (Some(prev_vals), false) => {
                    // Existing group updated -> Update
                    if let Some(values) = current {
                        if values != prev_vals {
                            deltas.push(DeltaRecord {
                                table: self.def.name.clone(),
                                operation: DeltaOperation::Update,
                                key: delta_key,
                                values,
                                prev_values: Some(prev_vals),
                            });
                        }
                    }
                }
                (Some(prev_vals), true) => {
                    // Group became empty after rollback/expiry -> Delete
                    deltas.push(DeltaRecord {
                        table: self.def.name.clone(),
                        operation: DeltaOperation::Delete,
                        key: delta_key,
                        values: prev_vals.clone(),
                        prev_values: Some(prev_vals),
                    });
                    // Clean up empty group and track for storage deletion
                    self.groups.remove(key);
                    self.removed_groups.push(key.clone());
                }
                (None, true) => {
                    // Was never emitted and is empty — no delta needed
                }
            }
        }

        deltas
    }

    fn build_delta_key(&self, group_key: &GroupKey) -> HashMap<String, Value> {
        let mut delta_key = HashMap::new();
        let mut key_idx = 0;
        for col in &self.output_columns {
            match col {
                OutputColumn::GroupBy { output_name, .. }
                | OutputColumn::Window { output_name, .. } => {
                    delta_key.insert(output_name.clone(), group_key[key_idx].clone());
                    key_idx += 1;
                }
                OutputColumn::Agg { .. } => {}
            }
        }
        delta_key
    }

    fn create_agg_vec(&self) -> Vec<Box<dyn AggregationFunc>> {
        let mut aggs = Vec::with_capacity(self.agg_count);
        for col in &self.output_columns {
            if let OutputColumn::Agg { .. } = col {
                let agg_func = &self.def.select.iter().find_map(|item| {
                    if let SelectExpr::Agg(func, _) = &item.expr {
                        let name = resolve_output_name(item);
                        let matching = self.output_columns.iter().any(|c| {
                            matches!(c, OutputColumn::Agg { output_name, agg_index, .. }
                                     if *output_name == name && *agg_index == aggs.len())
                        });
                        if matching {
                            return Some(func.clone());
                        }
                    }
                    None
                }).expect("agg func must exist");
                aggs.push(create_agg(agg_func));
            }
        }
        aggs
    }
}

fn resolve_output_name(item: &SelectItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expr {
        SelectExpr::Column(col) => col.clone(),
        SelectExpr::Agg(func, col) => {
            let func_name = match func {
                crate::schema::ast::AggFunc::Sum => "sum",
                crate::schema::ast::AggFunc::Count => "count",
                crate::schema::ast::AggFunc::Min => "min",
                crate::schema::ast::AggFunc::Max => "max",
                crate::schema::ast::AggFunc::Avg => "avg",
                crate::schema::ast::AggFunc::First => "first",
                crate::schema::ast::AggFunc::Last => "last",
            };
            match col {
                Some(c) => format!("{func_name}_{c}"),
                None => func_name.to_string(),
            }
        }
        SelectExpr::WindowFunc { column, .. } => column.clone(),
    }
}

/// Serialize an MV group's full state (finalized + per-block) for sliding window persistence.
fn serialize_mv_group_full(
    aggs: &[Box<dyn AggregationFunc>],
    prev_output: Option<&HashMap<String, Value>>,
) -> Vec<u8> {
    let agg_bytes: Vec<Vec<u8>> = aggs.iter().map(|a| a.to_bytes()).collect();
    rmp_serde::to_vec(&(agg_bytes, prev_output))
        .expect("MV group state serialization should not fail")
}

/// Serialize an MV group's aggregation state + prev_output for persistence.
/// Format: MessagePack-encoded (Vec<Vec<u8>>, Option<HashMap<String, Value>>)
fn serialize_mv_group(
    aggs: &[Box<dyn AggregationFunc>],
    prev_output: Option<&HashMap<String, Value>>,
) -> Vec<u8> {
    let agg_bytes: Vec<Vec<u8>> = aggs.iter().map(|a| a.to_finalized_bytes()).collect();
    rmp_serde::to_vec(&(agg_bytes, prev_output))
        .expect("MV group state serialization should not fail")
}

/// Deserialize an MV group's state from bytes.
fn deserialize_mv_group(
    bytes: &[u8],
    agg_funcs: &[AggFunc],
) -> Option<(Vec<Box<dyn AggregationFunc>>, Option<HashMap<String, Value>>)> {
    let (agg_bytes, prev_output): (Vec<Vec<u8>>, Option<HashMap<String, Value>>) =
        rmp_serde::from_slice(bytes).ok()?;
    if agg_bytes.len() != agg_funcs.len() {
        return None;
    }
    let aggs: Vec<Box<dyn AggregationFunc>> = agg_funcs
        .iter()
        .zip(agg_bytes.iter())
        .map(|(func, bytes)| restore_agg(func, bytes))
        .collect();
    Some((aggs, prev_output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::{AggFunc, MVDef, SelectExpr, SelectItem};
    use crate::storage::memory::MemoryBackend;

    fn test_storage() -> Arc<dyn StorageBackend> {
        Arc::new(MemoryBackend::new())
    }

    fn make_row(pairs: &[(&str, Value)]) -> RowMap {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn ohlcv_mv_def() -> MVDef {
        MVDef {
            name: "candles_5m".to_string(),
            source: "trades".to_string(),
            select: vec![
                SelectItem { expr: SelectExpr::Column("pair".into()), alias: None },
                SelectItem {
                    expr: SelectExpr::WindowFunc {
                        column: "block_time".into(),
                        interval_seconds: 300,
                    },
                    alias: Some("window_start".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::First, Some("price".into())),
                    alias: Some("open".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Max, Some("price".into())),
                    alias: Some("high".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Min, Some("price".into())),
                    alias: Some("low".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Last, Some("price".into())),
                    alias: Some("close".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Sum, Some("amount".into())),
                    alias: Some("volume".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Count, None),
                    alias: Some("trade_count".into()),
                },
            ],
            group_by: vec!["pair".into(), "window_start".into()],
            sliding_window: None,
        }
    }

    fn simple_sum_mv_def() -> MVDef {
        MVDef {
            name: "volume_by_pool".to_string(),
            source: "swaps".to_string(),
            select: vec![
                SelectItem { expr: SelectExpr::Column("pool".into()), alias: None },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Sum, Some("amount".into())),
                    alias: Some("total_volume".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Count, None),
                    alias: Some("swap_count".into()),
                },
            ],
            group_by: vec!["pool".into()],
            sliding_window: None,
        }
    }

    #[test]
    fn simple_mv_insert_deltas() {
        let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage());

        let rows = vec![
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(10.0))]),
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(20.0))]),
            make_row(&[("pool", Value::String("BTC/USDC".into())), ("amount", Value::Float64(5.0))]),
        ];

        let deltas = mv.process_block(1000, &rows);

        // Two new groups -> two Insert deltas
        assert_eq!(deltas.len(), 2);
        assert!(deltas.iter().all(|d| d.operation == DeltaOperation::Insert));

        let eth = deltas.iter().find(|d| d.key.get("pool") == Some(&Value::String("ETH/USDC".into()))).unwrap();
        assert_eq!(eth.values.get("total_volume"), Some(&Value::Float64(30.0)));
        assert_eq!(eth.values.get("swap_count"), Some(&Value::UInt64(2)));

        let btc = deltas.iter().find(|d| d.key.get("pool") == Some(&Value::String("BTC/USDC".into()))).unwrap();
        assert_eq!(btc.values.get("total_volume"), Some(&Value::Float64(5.0)));
        assert_eq!(btc.values.get("swap_count"), Some(&Value::UInt64(1)));
    }

    #[test]
    fn mv_update_deltas_on_second_block() {
        let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage());

        let rows1 = vec![
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(10.0))]),
        ];
        let deltas1 = mv.process_block(1000, &rows1);
        assert_eq!(deltas1.len(), 1);
        assert_eq!(deltas1[0].operation, DeltaOperation::Insert);

        let rows2 = vec![
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(20.0))]),
        ];
        let deltas2 = mv.process_block(1001, &rows2);
        assert_eq!(deltas2.len(), 1);
        assert_eq!(deltas2[0].operation, DeltaOperation::Update);
        assert_eq!(deltas2[0].values.get("total_volume"), Some(&Value::Float64(30.0)));
        assert_eq!(
            deltas2[0].prev_values.as_ref().unwrap().get("total_volume"),
            Some(&Value::Float64(10.0))
        );
    }

    #[test]
    fn mv_rollback_produces_update_delta() {
        let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage());

        mv.process_block(1000, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(10.0))]),
        ]);
        mv.process_block(1001, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(20.0))]),
        ]);

        let deltas = mv.rollback(1000);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, DeltaOperation::Update);
        assert_eq!(deltas[0].values.get("total_volume"), Some(&Value::Float64(10.0)));
        assert_eq!(
            deltas[0].prev_values.as_ref().unwrap().get("total_volume"),
            Some(&Value::Float64(30.0))
        );
    }

    #[test]
    fn mv_rollback_produces_delete_when_empty() {
        let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage());

        mv.process_block(1000, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(10.0))]),
        ]);

        let deltas = mv.rollback(999); // rollback everything
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, DeltaOperation::Delete);
    }

    #[test]
    fn mv_rollback_noop_when_nothing_to_rollback() {
        let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage());

        mv.process_block(1000, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(10.0))]),
        ]);

        let deltas = mv.rollback(1000);
        assert!(deltas.is_empty());
    }

    #[test]
    fn mv_finalize_then_rollback() {
        let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage());

        mv.process_block(1000, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(10.0))]),
        ]);
        mv.process_block(1001, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(20.0))]),
        ]);
        mv.process_block(1002, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(30.0))]),
        ]);

        let mut batch = StorageWriteBatch::new();
        mv.finalize(1001, &mut batch);

        // Rollback block 1002
        let deltas = mv.rollback(1001);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, DeltaOperation::Update);
        // Finalized sum: 10+20=30, block 1002 removed
        assert_eq!(deltas[0].values.get("total_volume"), Some(&Value::Float64(30.0)));
    }

    #[test]
    fn ohlcv_candle_end_to_end() {
        let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage());

        // All trades in same 5-min window (block_time within same 300s interval)
        let window_base = 1_700_000_000_000i64; // some ms timestamp

        // Block 1000: ETH/USDC price=100, amount=1
        mv.process_block(1000, &[make_row(&[
            ("pair", Value::String("ETH/USDC".into())),
            ("block_time", Value::DateTime(window_base + 10_000)),
            ("price", Value::Float64(100.0)),
            ("amount", Value::Float64(1.0)),
        ])]);

        // Block 1001: price=110, amount=2
        mv.process_block(1001, &[make_row(&[
            ("pair", Value::String("ETH/USDC".into())),
            ("block_time", Value::DateTime(window_base + 20_000)),
            ("price", Value::Float64(110.0)),
            ("amount", Value::Float64(2.0)),
        ])]);

        // Block 1002: price=90, amount=3
        mv.process_block(1002, &[make_row(&[
            ("pair", Value::String("ETH/USDC".into())),
            ("block_time", Value::DateTime(window_base + 30_000)),
            ("price", Value::Float64(90.0)),
            ("amount", Value::Float64(3.0)),
        ])]);

        // Block 1003: price=200, amount=10 (will be rolled back)
        mv.process_block(1003, &[make_row(&[
            ("pair", Value::String("ETH/USDC".into())),
            ("block_time", Value::DateTime(window_base + 40_000)),
            ("price", Value::Float64(200.0)),
            ("amount", Value::Float64(10.0)),
        ])]);

        // Rollback block 1003
        let deltas = mv.rollback(1002);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, DeltaOperation::Update);

        let vals = &deltas[0].values;
        assert_eq!(vals.get("open"), Some(&Value::Float64(100.0)));
        assert_eq!(vals.get("high"), Some(&Value::Float64(110.0))); // was 200, now 110
        assert_eq!(vals.get("low"), Some(&Value::Float64(90.0)));
        assert_eq!(vals.get("close"), Some(&Value::Float64(90.0))); // was 200, now 90
        assert_eq!(vals.get("volume"), Some(&Value::Float64(6.0))); // was 16, now 6
        assert_eq!(vals.get("trade_count"), Some(&Value::UInt64(3))); // was 4, now 3
    }

    #[test]
    fn ohlcv_multiple_pairs_isolated() {
        let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage());
        let ts = 1_700_000_000_000i64;

        mv.process_block(1000, &[
            make_row(&[
                ("pair", Value::String("ETH/USDC".into())),
                ("block_time", Value::DateTime(ts)),
                ("price", Value::Float64(100.0)),
                ("amount", Value::Float64(1.0)),
            ]),
            make_row(&[
                ("pair", Value::String("BTC/USDC".into())),
                ("block_time", Value::DateTime(ts)),
                ("price", Value::Float64(50000.0)),
                ("amount", Value::Float64(0.1)),
            ]),
        ]);

        // Rollback block 1000 — both groups should be deleted
        let deltas = mv.rollback(999);
        assert_eq!(deltas.len(), 2);
        assert!(deltas.iter().all(|d| d.operation == DeltaOperation::Delete));
    }

    #[test]
    fn mv_different_time_windows() {
        let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage());

        // Two trades in different 5-min windows
        let window1 = 1_700_000_000_000i64;
        let window2 = window1 + 300_000; // +5 minutes

        mv.process_block(1000, &[
            make_row(&[
                ("pair", Value::String("ETH/USDC".into())),
                ("block_time", Value::DateTime(window1 + 1000)),
                ("price", Value::Float64(100.0)),
                ("amount", Value::Float64(1.0)),
            ]),
            make_row(&[
                ("pair", Value::String("ETH/USDC".into())),
                ("block_time", Value::DateTime(window2 + 1000)),
                ("price", Value::Float64(200.0)),
                ("amount", Value::Float64(2.0)),
            ]),
        ]);

        // Should produce 2 Insert deltas (different windows = different groups)
        // Already consumed by process_block, let's check via rollback
        let deltas = mv.rollback(999);
        assert_eq!(deltas.len(), 2);
        assert!(deltas.iter().all(|d| d.operation == DeltaOperation::Delete));
    }

    #[test]
    fn full_cycle_ingest_rollback_reingest() {
        let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage());

        // Block 1000
        mv.process_block(1000, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(10.0))]),
        ]);
        // Block 1001
        mv.process_block(1001, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(20.0))]),
        ]);
        // Block 1002 (will be rolled back)
        mv.process_block(1002, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(100.0))]),
        ]);

        // Rollback block 1002
        let rollback_deltas = mv.rollback(1001);
        assert_eq!(rollback_deltas.len(), 1);
        assert_eq!(rollback_deltas[0].values.get("total_volume"), Some(&Value::Float64(30.0)));

        // Re-ingest block 1002 with different data (reorg)
        let new_deltas = mv.process_block(1002, &[
            make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(5.0))]),
        ]);
        assert_eq!(new_deltas.len(), 1);
        assert_eq!(new_deltas[0].operation, DeltaOperation::Update);
        assert_eq!(new_deltas[0].values.get("total_volume"), Some(&Value::Float64(35.0)));
    }

    #[test]
    fn non_sliding_mv_finalize_deletes_empty_group_from_storage() {
        let storage = test_storage();

        // Phase 1: Two groups persisted — ETH/USDC and BTC/USDC
        {
            let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone());
            mv.process_block(1, &[
                make_row(&[("pool", Value::String("ETH/USDC".into())), ("amount", Value::Float64(10.0))]),
            ]);
            mv.process_block(2, &[
                make_row(&[("pool", Value::String("BTC/USDC".into())), ("amount", Value::Float64(20.0))]),
            ]);
            let mut batch = StorageWriteBatch::new();
            mv.finalize(1, &mut batch);
            storage.commit(&batch).unwrap();
        }

        // Phase 2: Rollback block 2 (above finalized=1) — BTC/USDC group empties
        {
            let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone());
            // Replay unfinalized block 2
            mv.process_block(2, &[
                make_row(&[("pool", Value::String("BTC/USDC".into())), ("amount", Value::Float64(20.0))]),
            ]);
            // Rollback to block 1, removing block 2
            let deltas = mv.rollback(1);
            // BTC/USDC should get a Delete delta (it only had unfinalized data)
            assert!(deltas.iter().any(|d| d.operation == DeltaOperation::Delete));

            let mut batch = StorageWriteBatch::new();
            mv.finalize(1, &mut batch);
            storage.commit(&batch).unwrap();
        }

        // Phase 3: Restore — BTC/USDC should not be resurrected
        {
            let mv = MVEngine::new(simple_sum_mv_def(), storage.clone());
            assert_eq!(mv.groups.len(), 1, "only ETH/USDC should survive");
        }
    }

    // -----------------------------------------------------------------------
    // Sliding window tests
    // -----------------------------------------------------------------------

    /// Helper: create a sliding window MV def with SUM(volume), COUNT().
    /// Window = `window_secs` seconds, grouped by `pair`, time column = `ts`.
    fn sliding_sum_mv_def(window_secs: u64) -> MVDef {
        MVDef {
            name: "volume_sliding".to_string(),
            source: "trades".to_string(),
            select: vec![
                SelectItem { expr: SelectExpr::Column("pair".into()), alias: None },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Sum, Some("volume".into())),
                    alias: Some("total_volume".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Count, None),
                    alias: Some("trade_count".into()),
                },
            ],
            group_by: vec!["pair".into()],
            sliding_window: Some(SlidingWindowDef {
                interval_seconds: window_secs,
                time_column: "ts".into(),
            }),
        }
    }

    /// Helper: create a sliding window MV def with all 7 aggregation types.
    fn sliding_all_aggs_mv_def(window_secs: u64) -> MVDef {
        MVDef {
            name: "all_aggs_sliding".to_string(),
            source: "data".to_string(),
            select: vec![
                SelectItem { expr: SelectExpr::Column("grp".into()), alias: None },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Sum, Some("val".into())),
                    alias: Some("s".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Count, None),
                    alias: Some("c".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Min, Some("val".into())),
                    alias: Some("mn".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Max, Some("val".into())),
                    alias: Some("mx".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Avg, Some("val".into())),
                    alias: Some("av".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::First, Some("val".into())),
                    alias: Some("fi".into()),
                },
                SelectItem {
                    expr: SelectExpr::Agg(AggFunc::Last, Some("val".into())),
                    alias: Some("la".into()),
                },
            ],
            group_by: vec!["grp".into()],
            sliding_window: Some(SlidingWindowDef {
                interval_seconds: window_secs,
                time_column: "ts".into(),
            }),
        }
    }

    fn make_ts_row(pairs: &[(&str, Value)], ts_ms: i64) -> RowMap {
        let mut row = make_row(pairs);
        row.insert("ts".to_string(), Value::DateTime(ts_ms));
        row
    }

    #[test]
    fn sliding_window_no_expiry_within_window() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(3600), test_storage());

        // Three blocks all within 1 hour
        let d1 = mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(100.0))], 0),
        ]);
        assert_eq!(d1.len(), 1);
        assert_eq!(d1[0].operation, DeltaOperation::Insert);
        assert_eq!(d1[0].values.get("total_volume"), Some(&Value::Float64(100.0)));

        let d2 = mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(200.0))], 1_800_000),
        ]);
        assert_eq!(d2.len(), 1);
        assert_eq!(d2[0].operation, DeltaOperation::Update);
        assert_eq!(d2[0].values.get("total_volume"), Some(&Value::Float64(300.0)));

        let d3 = mv.process_block(3, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(50.0))], 3_500_000),
        ]);
        assert_eq!(d3.len(), 1);
        assert_eq!(d3[0].values.get("total_volume"), Some(&Value::Float64(350.0)));
        assert_eq!(d3[0].values.get("trade_count"), Some(&Value::UInt64(3)));
    }

    #[test]
    fn sliding_window_basic_expiry() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(3600), test_storage()); // 1 hour

        // Block 1: ts=0, volume=100
        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(100.0))], 0),
        ]);

        // Block 2: ts=30min, volume=200
        mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(200.0))], 1_800_000),
        ]);

        // Block 3: ts=1hr+1s → block 1 (ts=0) should expire
        // cutoff = 3_601_000 - 3_600_000 = 1_000. Block 1 ts=0 < 1_000 → expired
        let d3 = mv.process_block(3, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(300.0))], 3_601_000),
        ]);
        assert_eq!(d3.len(), 1);
        assert_eq!(d3[0].operation, DeltaOperation::Update);
        // After expiry: 200 + 300 = 500 (block 1's 100 expired)
        assert_eq!(d3[0].values.get("total_volume"), Some(&Value::Float64(500.0)));
        assert_eq!(d3[0].values.get("trade_count"), Some(&Value::UInt64(2)));
    }

    #[test]
    fn sliding_window_full_expiry_delete() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(3600), test_storage());

        // Block 1: group A at ts=0
        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("A".into())), ("volume", Value::Float64(10.0))], 0),
        ]);

        // Block 2: group B at ts=1hr+1s → group A fully expires
        let d2 = mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("B".into())), ("volume", Value::Float64(20.0))], 3_601_000),
        ]);

        // Should have Insert for B and Delete for A
        assert_eq!(d2.len(), 2);
        let insert = d2.iter().find(|d| d.operation == DeltaOperation::Insert).unwrap();
        let delete = d2.iter().find(|d| d.operation == DeltaOperation::Delete).unwrap();
        assert_eq!(insert.key.get("pair"), Some(&Value::String("B".into())));
        assert_eq!(delete.key.get("pair"), Some(&Value::String("A".into())));
    }

    #[test]
    fn sliding_window_sum_correctness_across_expiry() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage()); // 10 second window

        // 5 blocks, each 3 seconds apart
        for i in 0..5u64 {
            mv.process_block(i + 1, &[
                make_ts_row(
                    &[("pair", Value::String("X".into())), ("volume", Value::Float64((i + 1) as f64 * 10.0))],
                    (i * 3_000) as i64,
                ),
            ]);
        }

        // At block 5: ts=12_000, window=10_000, cutoff=2_000
        // Block 1 (ts=0) expired. Blocks 2-5 remain.
        // Sum = 20 + 30 + 40 + 50 = 140, count = 4
        // (Note: we need to check current state, which is reflected in the last delta)
        // Actually, let me trace: after block 5, the last emit_deltas for "X" captures all changes
        // including expiry. Let me just check the accumulated state.
        // Re-check: block 5 ts=12000, cutoff = 12000-10000 = 2000
        // Block 1 ts=0 < 2000 → expired. Blocks 2(ts=3000),3(ts=6000),4(ts=9000),5(ts=12000) remain
        // Sum = 20+30+40+50 = 140

        // Process one more block that doesn't expire anything, to get current state
        let d = mv.process_block(6, &[
            make_ts_row(
                &[("pair", Value::String("X".into())), ("volume", Value::Float64(1.0))],
                12_500, // still within window of block 2
            ),
        ]);
        assert_eq!(d.len(), 1);
        // Sum: 20+30+40+50+1 = 141
        assert_eq!(d[0].values.get("total_volume"), Some(&Value::Float64(141.0)));
    }

    #[test]
    fn sliding_window_all_agg_types() {
        let mut mv = MVEngine::new(sliding_all_aggs_mv_def(3600), test_storage());

        // Block 1: val=10 at ts=0
        mv.process_block(1, &[
            make_ts_row(&[("grp", Value::String("G".into())), ("val", Value::Float64(10.0))], 0),
        ]);

        // Block 2: val=20 at ts=30min
        mv.process_block(2, &[
            make_ts_row(&[("grp", Value::String("G".into())), ("val", Value::Float64(20.0))], 1_800_000),
        ]);

        // Block 3: val=15 at ts=1hr+1s → block 1 expires
        let d3 = mv.process_block(3, &[
            make_ts_row(&[("grp", Value::String("G".into())), ("val", Value::Float64(15.0))], 3_601_000),
        ]);
        assert_eq!(d3.len(), 1);
        let v = &d3[0].values;
        // After expiry of block 1 (val=10):
        // Remaining: block 2 (val=20), block 3 (val=15)
        assert_eq!(v.get("s"), Some(&Value::Float64(35.0)));     // sum: 20+15
        assert_eq!(v.get("c"), Some(&Value::UInt64(2)));          // count: 2
        assert_eq!(v.get("mn"), Some(&Value::Float64(15.0)));     // min: 15
        assert_eq!(v.get("mx"), Some(&Value::Float64(20.0)));     // max: 20
        assert_eq!(v.get("av"), Some(&Value::Float64(17.5)));     // avg: 35/2
        assert_eq!(v.get("fi"), Some(&Value::Float64(20.0)));     // first: earliest remaining = block 2
        assert_eq!(v.get("la"), Some(&Value::Float64(15.0)));     // last: latest = block 3
    }

    #[test]
    fn sliding_window_rollback() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(3600), test_storage());

        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(100.0))], 0),
        ]);
        mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(200.0))], 1_000_000),
        ]);
        mv.process_block(3, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(300.0))], 2_000_000),
        ]);

        // Rollback to block 1
        let rollback_deltas = mv.rollback(1);
        assert_eq!(rollback_deltas.len(), 1);
        assert_eq!(rollback_deltas[0].operation, DeltaOperation::Update);
        assert_eq!(rollback_deltas[0].values.get("total_volume"), Some(&Value::Float64(100.0)));

        // Re-ingest block 2 with different data
        let d = mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(50.0))], 1_500_000),
        ]);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].values.get("total_volume"), Some(&Value::Float64(150.0)));
    }

    #[test]
    fn sliding_window_rollback_plus_expiry() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(3600), test_storage());

        // Block 1: ts=0
        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(100.0))], 0),
        ]);
        // Block 2: ts=1800s
        mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(200.0))], 1_800_000),
        ]);
        // Block 3: ts=3601s → block 1 expired
        mv.process_block(3, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(300.0))], 3_601_000),
        ]);

        // Now rollback to block 2. Block 3 is removed. Block 1 was already expired.
        let rb = mv.rollback(2);
        assert_eq!(rb.len(), 1);
        // After rollback: only block 2 remains (block 1 expired, block 3 rolled back)
        // Watermark recalculated to 1_800_000
        assert_eq!(rb[0].values.get("total_volume"), Some(&Value::Float64(200.0)));
    }

    #[test]
    fn sliding_window_rapid_expiry() {
        // 1-second window: every new block expires the previous one
        let mut mv = MVEngine::new(sliding_sum_mv_def(1), test_storage());

        let d1 = mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(10.0))], 0),
        ]);
        assert_eq!(d1[0].values.get("total_volume"), Some(&Value::Float64(10.0)));

        let d2 = mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(20.0))], 2_000),
        ]);
        // Block 1 (ts=0) expired (cutoff = 2000 - 1000 = 1000, 0 < 1000)
        assert_eq!(d2[0].values.get("total_volume"), Some(&Value::Float64(20.0)));
        assert_eq!(d2[0].values.get("trade_count"), Some(&Value::UInt64(1)));

        let d3 = mv.process_block(3, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(30.0))], 4_000),
        ]);
        // Block 2 (ts=2000) expired (cutoff = 4000 - 1000 = 3000, 2000 < 3000)
        assert_eq!(d3[0].values.get("total_volume"), Some(&Value::Float64(30.0)));
    }

    #[test]
    fn sliding_window_multiple_groups_independent_expiry() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(3600), test_storage());

        // Group A at ts=0
        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("A".into())), ("volume", Value::Float64(100.0))], 0),
        ]);
        // Group B at ts=3000s (within window)
        mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("B".into())), ("volume", Value::Float64(200.0))], 3_000_000),
        ]);
        // Group A new data at ts=3601s → block 1 (group A) expires
        let d3 = mv.process_block(3, &[
            make_ts_row(&[("pair", Value::String("A".into())), ("volume", Value::Float64(50.0))], 3_601_000),
        ]);

        // Group A: block 1 expired, block 3 added → volume=50
        // Group B: block 2 (ts=3000s) still within window (cutoff=3_601_000-3_600_000=1_000)
        let a_delta = d3.iter().find(|d| d.key.get("pair") == Some(&Value::String("A".into()))).unwrap();
        assert_eq!(a_delta.values.get("total_volume"), Some(&Value::Float64(50.0)));

        // Group B should NOT appear in deltas (not touched and not expired)
        assert!(d3.iter().all(|d| d.key.get("pair") != Some(&Value::String("B".into()))));
    }

    #[test]
    fn sliding_window_persistence_and_restore() {
        let storage = test_storage();

        // Create MV, process blocks, finalize
        {
            let mut mv = MVEngine::new(sliding_sum_mv_def(3600), storage.clone());

            mv.process_block(1, &[
                make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(100.0))], 0),
            ]);
            mv.process_block(2, &[
                make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(200.0))], 1_000_000),
            ]);

            let mut batch = StorageWriteBatch::new();
            mv.finalize(2, &mut batch);
            storage.commit(&batch).unwrap();
        }

        // Restore from storage
        {
            let mv = MVEngine::new(sliding_sum_mv_def(3600), storage.clone());

            // block_times should be restored
            assert_eq!(mv.block_times.len(), 2);
            assert!(mv.block_times.contains_key(&1));
            assert!(mv.block_times.contains_key(&2));

            // block_groups should be rebuilt
            assert!(mv.block_groups.contains_key(&1));
            assert!(mv.block_groups.contains_key(&2));

            // Aggregation state should be restored with per-block data
            assert_eq!(mv.groups.len(), 1);
        }
    }

    #[test]
    fn sliding_window_replay_skip() {
        let storage = test_storage();

        {
            let mut mv = MVEngine::new(sliding_sum_mv_def(3600), storage.clone());
            mv.process_block(1, &[
                make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(100.0))], 0),
            ]);
            let mut batch = StorageWriteBatch::new();
            mv.finalize(1, &mut batch);
            storage.commit(&batch).unwrap();
        }

        // Simulate restart + replay
        let mut mv = MVEngine::new(sliding_sum_mv_def(3600), storage.clone());

        // Replay block 1 — should be skipped (already in block_times)
        let d = mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(100.0))], 0),
        ]);
        assert!(d.is_empty(), "replay of persisted block should be skipped");

        // New block 2 should work normally
        let d2 = mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("ETH".into())), ("volume", Value::Float64(50.0))], 500_000),
        ]);
        assert_eq!(d2.len(), 1);
        assert_eq!(d2[0].values.get("total_volume"), Some(&Value::Float64(150.0)));
    }

    #[test]
    fn sliding_window_out_of_order_timestamps() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage()); // 10 second window

        // Block 1 at ts=5000
        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(10.0))], 5_000),
        ]);
        // Block 2 at ts=2000 (earlier than block 1!)
        mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(20.0))], 2_000),
        ]);
        // Block 3 at ts=13000 → watermark=13000, cutoff=3000
        // Block 2 (ts=2000) < 3000 → expired. Block 1 (ts=5000) stays.
        let d3 = mv.process_block(3, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(30.0))], 13_000),
        ]);
        assert_eq!(d3.len(), 1);
        // Remaining: block 1 (10) + block 3 (30) = 40
        assert_eq!(d3[0].values.get("total_volume"), Some(&Value::Float64(40.0)));
    }

    #[test]
    fn sliding_window_boundary_inclusive() {
        // Test that the window boundary is inclusive (data AT cutoff is NOT expired)
        let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage()); // 10 second window

        // Block 1 at ts=0
        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(10.0))], 0),
        ]);

        // Block 2 at ts=10000 → cutoff = 10000 - 10000 = 0
        // Block 1 ts=0, cutoff=0. Since we use strict less-than (ts < cutoff),
        // ts=0 is NOT less than 0, so block 1 is NOT expired.
        let d2 = mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(20.0))], 10_000),
        ]);
        assert_eq!(d2.len(), 1);
        // Both blocks remain: 10 + 20 = 30
        assert_eq!(d2[0].values.get("total_volume"), Some(&Value::Float64(30.0)));
    }

    #[test]
    fn sliding_window_empty_group_cleanup_on_finalize() {
        let storage = test_storage();
        let mut mv = MVEngine::new(sliding_sum_mv_def(1), storage.clone()); // 1 second window

        // Block 1 at ts=0
        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(10.0))], 0),
        ]);
        // Block 2 at ts=2000 → block 1 expired, group X is now empty
        mv.process_block(2, &[
            make_ts_row(&[("pair", Value::String("Y".into())), ("volume", Value::Float64(20.0))], 2_000),
        ]);

        // Group X should be gone from self.groups (Delete delta cleans it up)
        assert!(!mv.groups.keys().any(|k| k.iter().any(|v| v == &Value::String("X".into()))));

        // Finalize
        let mut batch = StorageWriteBatch::new();
        mv.finalize(2, &mut batch);
        storage.commit(&batch).unwrap();

        // Restore — group X should not exist
        let mv2 = MVEngine::new(sliding_sum_mv_def(1), storage.clone());
        assert_eq!(mv2.groups.len(), 1); // only Y
    }

    #[test]
    fn sliding_window_missing_timestamp_uses_watermark() {
        let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage()); // 10s window

        // Block 1: has timestamp
        mv.process_block(1, &[
            make_ts_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(10.0))], 5_000),
        ]);

        // Block 2: missing "ts" column — should still get a block_times entry
        mv.process_block(2, &[
            make_row(&[("pair", Value::String("X".into())), ("volume", Value::Float64(20.0))]),
        ]);

        // block_times should have entries for both blocks
        assert!(mv.block_times.contains_key(&1));
        assert!(mv.block_times.contains_key(&2));
        // Block 2 should use watermark (5000) as fallback
        assert_eq!(mv.block_times[&2], 5_000);
    }
}
