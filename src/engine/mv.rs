use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::schema::ast::{AggFunc, MVDef, SelectExpr, SelectItem};
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

/// Manages a single materialized view: GROUP BY routing, aggregation, rollback, deltas.
pub struct MVEngine {
    def: MVDef,
    /// The output column descriptors (in SELECT order).
    output_columns: Vec<OutputColumn>,
    /// The AggFunc types in SELECT order (for deserialization).
    agg_funcs: Vec<AggFunc>,
    /// Number of aggregation functions per group.
    agg_count: usize,
    /// group_key -> aggregation state (one AggregationFunc per agg column).
    groups: HashMap<GroupKey, Vec<Box<dyn AggregationFunc>>>,
    /// Tracks which blocks have been ingested (for rollback).
    /// block -> set of group keys touched. BTreeMap for O(log N) range queries.
    block_groups: BTreeMap<BlockNumber, HashSet<GroupKey>>,
    /// Snapshot of previous output values per group key, for delta computation.
    prev_output: HashMap<GroupKey, HashMap<String, Value>>,
    /// Storage backend for persisting finalized MV state.
    storage: Arc<dyn StorageBackend>,
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

        // Restore finalized MV state from storage
        let mut groups: HashMap<GroupKey, Vec<Box<dyn AggregationFunc>>> = HashMap::new();
        let mut prev_output: HashMap<GroupKey, HashMap<String, Value>> = HashMap::new();

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

        MVEngine {
            def,
            output_columns,
            agg_funcs,
            agg_count,
            groups,
            block_groups: BTreeMap::new(),
            prev_output,
            storage,
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
        // Snapshot current output for touched groups before mutation
        let mut touched_keys: HashSet<GroupKey> = HashSet::new();

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

            // Feed values to each aggregation
            for col in &self.output_columns {
                if let OutputColumn::Agg { source_col, agg_index, .. } = col {
                    let values = extract_agg_values(row, source_col.as_deref());
                    aggs[*agg_index].add_block(block, &values);
                }
            }

            // Track block -> group key mapping for rollback
            self.block_groups
                .entry(block)
                .or_default()
                .insert(group_key);
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

        // Collect all group keys affected by rolled-back blocks (consume by value)
        let mut touched_keys: HashSet<GroupKey> = HashSet::new();
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
        for aggs in self.groups.values_mut() {
            for agg in aggs.iter_mut() {
                agg.finalize_up_to(block);
            }
        }

        // Persist finalized state for all groups
        for (group_key, aggs) in &self.groups {
            let gk_bytes = storage::encode_group_key(group_key);
            let prev = self.prev_output.get(group_key);
            let state_bytes = serialize_mv_group(aggs, prev);
            batch.put_mv_state(&self.def.name, &gk_bytes, &state_bytes);
        }

        // Remove finalized blocks from tracking using split_off
        // split_off(block+1) leaves entries <= block in the original (which we discard)
        let remaining = self.block_groups.split_off(&(block + 1));
        self.block_groups = remaining;
    }

    fn compute_group_key(&self, row: &RowMap) -> GroupKey {
        let mut key = Vec::new();
        for col in &self.output_columns {
            match col {
                OutputColumn::GroupBy { source_col, .. } => {
                    let v = row.get(source_col).cloned().unwrap_or(Value::Null);
                    key.push(v);
                }
                OutputColumn::Window { source_col, interval_seconds, .. } => {
                    let ts = row
                        .get(source_col)
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let window_start = to_start_of_interval(ts, *interval_seconds);
                    key.push(Value::DateTime(window_start));
                }
                OutputColumn::Agg { .. } => {
                    // Agg columns are not part of the group key
                }
            }
        }
        key
    }

    fn compute_output(&self, group_key: &GroupKey) -> Option<HashMap<String, Value>> {
        let aggs = self.groups.get(group_key)?;
        let mut output = HashMap::new();

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

    fn emit_deltas(&mut self, touched_keys: &HashSet<GroupKey>) -> Vec<DeltaRecord> {
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
                    // Group became empty after rollback -> Delete
                    deltas.push(DeltaRecord {
                        table: self.def.name.clone(),
                        operation: DeltaOperation::Delete,
                        key: delta_key,
                        values: prev_vals.clone(),
                        prev_values: Some(prev_vals),
                    });
                    // Clean up empty group
                    self.groups.remove(key);
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

fn extract_agg_values(row: &RowMap, source_col: Option<&str>) -> Vec<Value> {
    match source_col {
        Some(col) => {
            vec![row.get(col).cloned().unwrap_or(Value::Null)]
        }
        None => {
            // count() with no column — count the row itself
            vec![Value::UInt64(1)]
        }
    }
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
}
