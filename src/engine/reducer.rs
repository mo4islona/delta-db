use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::error::Result;
use crate::reducer_runtime::event_rules::EventRulesRuntime;
use crate::reducer_runtime::lua::LuaRuntime;
use crate::reducer_runtime::ReducerRuntime;
use crate::schema::ast::{ReducerBody, ReducerDef};
use crate::storage::{self, StorageBackend, StorageWriteBatch};
use crate::types::{BlockNumber, RowMap, Value};

type State = HashMap<String, Value>;

/// Orchestrates a single reducer: state management, snapshots, rollback, and output.
///
/// State is kept entirely in memory during normal processing. Storage is only
/// used for persisting finalized state. Block-level snapshots are held in an
/// in-memory `BTreeMap` so that rollback can restore to any unfinalized block
/// without serialization overhead.
pub struct ReducerEngine {
    def: ReducerDef,
    runtime: Box<dyn ReducerRuntime>,
    storage: Arc<dyn StorageBackend>,
    /// Current hot state per group key.
    state_cache: HashMap<Vec<u8>, State>,
    /// In-memory state snapshots: group_key -> (block -> state).
    /// Only contains unfinalized blocks. Used for rollback.
    block_snapshots: HashMap<Vec<u8>, BTreeMap<BlockNumber, State>>,
    /// Tracks which blocks have been processed and which group keys were touched.
    /// BTreeMap for O(log N) range queries during rollback/finalize.
    block_groups: BTreeMap<BlockNumber, HashSet<Vec<u8>>>,
}

impl ReducerEngine {
    pub fn new(def: ReducerDef, storage: Arc<dyn StorageBackend>) -> Self {
        let runtime: Box<dyn ReducerRuntime> = match &def.body {
            ReducerBody::EventRules { .. } => Box::new(EventRulesRuntime::new(&def.body)),
            ReducerBody::Lua { script } => Box::new(LuaRuntime::new(script)),
        };

        Self {
            def,
            runtime,
            storage,
            state_cache: HashMap::new(),
            block_snapshots: HashMap::new(),
            block_groups: BTreeMap::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn source(&self) -> &str {
        &self.def.source
    }

    /// Process a batch of rows for a given block.
    /// Returns enriched output rows (one per input row that produced emit output).
    ///
    /// State is updated in memory only. A snapshot of each touched group key's
    /// state is saved in `block_snapshots` at the end of the block — no storage
    /// I/O or serialization happens here.
    pub fn process_block(
        &mut self,
        block: BlockNumber,
        rows: &[RowMap],
    ) -> Result<Vec<RowMap>> {
        let mut output_maps: Vec<RowMap> = Vec::new();
        let mut touched_keys: HashSet<Vec<u8>> = HashSet::new();

        for row in rows {
            let group_key_bytes = self.compute_group_key_bytes(row);

            // Load state if not cached
            if !self.state_cache.contains_key(&group_key_bytes) {
                let state = self.load_state(&group_key_bytes)?;
                self.state_cache.insert(group_key_bytes.clone(), state);
            }

            let state = self.state_cache.get_mut(&group_key_bytes).unwrap();

            // Call the runtime
            let emit = self.runtime.process(state, row);

            // Track touched key for deferred snapshot
            touched_keys.insert(group_key_bytes);

            if let Some(mut emit_row) = emit {
                // Add group-by columns to the output row for downstream MVs
                for col in &self.def.group_by {
                    if let Some(v) = row.get(col.as_str()) {
                        emit_row.entry(col.clone()).or_insert_with(|| v.clone());
                    }
                }
                output_maps.push(emit_row);
            }
        }

        // Save in-memory snapshot for each touched group key (one clone per key per block)
        let block_keys = self.block_groups.entry(block).or_default();
        for group_key_bytes in touched_keys {
            let state = self.state_cache.get(&group_key_bytes).unwrap();
            self.block_snapshots
                .entry(group_key_bytes.clone())
                .or_default()
                .insert(block, state.clone());
            block_keys.insert(group_key_bytes);
        }

        Ok(output_maps)
    }

    /// Roll back all blocks after fork_point.
    /// Restores state from in-memory block snapshots.
    /// Returns the number of groups affected.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Result<usize> {
        // Use BTreeMap split_off for O(log N) range extraction
        let rolled_back = self.block_groups.split_off(&(fork_point + 1));

        if rolled_back.is_empty() {
            return Ok(0);
        }

        // Collect all affected group keys (consume by value to avoid cloning)
        let mut affected_keys: HashSet<Vec<u8>> = HashSet::new();
        for (_block, keys) in rolled_back {
            for key in keys {
                affected_keys.insert(key);
            }
        }

        // Restore state for each affected group key
        for group_key_bytes in &affected_keys {
            // Remove snapshots after fork_point from the in-memory map.
            // split_off(fork_point+1) leaves entries <= fork_point in the original
            // and returns entries > fork_point (which we discard).
            if let Some(snapshots) = self.block_snapshots.get_mut(group_key_bytes) {
                let _discarded = snapshots.split_off(&(fork_point + 1));
            }

            // Find the state at fork_point (or the most recent before it)
            let state = self.find_state_at_or_before(group_key_bytes, fork_point)?;
            self.state_cache.insert(group_key_bytes.clone(), state);

            // Clean up empty snapshot maps
            if let Some(snapshots) = self.block_snapshots.get(group_key_bytes) {
                if snapshots.is_empty() {
                    self.block_snapshots.remove(group_key_bytes);
                }
            }
        }

        Ok(affected_keys.len())
    }

    /// Finalize state up to the given block.
    /// Collects finalized state writes into the provided batch for atomic commit.
    /// Drops in-memory snapshots for finalized blocks.
    pub fn finalize(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch) {
        // Split off blocks > block, keeping blocks <= block for finalization
        let remaining = self.block_groups.split_off(&(block + 1));
        let finalized_block_groups = std::mem::replace(&mut self.block_groups, remaining);

        let mut finalized_keys: HashSet<Vec<u8>> = HashSet::new();
        for keys in finalized_block_groups.values() {
            finalized_keys.extend(keys.iter().cloned());
        }

        // For each group key, add finalized state to batch and drop old snapshots
        for group_key_bytes in &finalized_keys {
            // Find the most recent state at or before the finalization block
            if let Some(state) = self.find_snapshot_at_or_before(group_key_bytes, block) {
                let state_bytes = storage::encode_state(&state);
                batch.set_reducer_finalized(
                    &self.def.name,
                    group_key_bytes,
                    &state_bytes,
                );
            }

            // Remove in-memory snapshots for blocks <= finalization point
            if let Some(snapshots) = self.block_snapshots.get_mut(group_key_bytes) {
                let remaining = snapshots.split_off(&(block + 1));
                *snapshots = remaining;
                if snapshots.is_empty() {
                    self.block_snapshots.remove(group_key_bytes);
                }
            }
        }
    }

    /// Find state at or before the given block from in-memory snapshots,
    /// falling back to storage (finalized state) or defaults.
    fn find_state_at_or_before(
        &self,
        group_key_bytes: &[u8],
        block: BlockNumber,
    ) -> Result<State> {
        if let Some(state) = self.find_snapshot_at_or_before(group_key_bytes, block) {
            return Ok(state);
        }
        // Fall back to finalized state in storage
        self.load_state(group_key_bytes)
    }

    /// Look up the most recent in-memory snapshot at or before the given block.
    fn find_snapshot_at_or_before(
        &self,
        group_key_bytes: &[u8],
        block: BlockNumber,
    ) -> Option<State> {
        self.block_snapshots
            .get(group_key_bytes)
            .and_then(|snapshots| {
                snapshots
                    .range(..=block)
                    .next_back()
                    .map(|(_, state)| state.clone())
            })
    }

    fn compute_group_key_bytes(&self, row: &RowMap) -> Vec<u8> {
        if self.def.group_by.is_empty() {
            return Vec::new();
        }
        // Fast path: single string group key — use raw bytes instead of MessagePack
        if self.def.group_by.len() == 1 {
            if let Some(Value::String(s)) = row.get(self.def.group_by[0].as_str()) {
                return s.as_bytes().to_vec();
            }
        }
        // General path: MessagePack-encoded composite key
        let key: Vec<Value> = self
            .def
            .group_by
            .iter()
            .map(|col| row.get(col.as_str()).cloned().unwrap_or(Value::Null))
            .collect();
        storage::encode_group_key(&key)
    }

    fn load_state(&self, group_key_bytes: &[u8]) -> Result<State> {
        // Try finalized state from storage
        if let Some(bytes) = self.storage.get_reducer_finalized(&self.def.name, group_key_bytes)? {
            return Ok(storage::decode_state(&bytes));
        }
        Ok(self.default_state())
    }

    fn default_state(&self) -> State {
        let mut state = HashMap::new();
        for field in &self.def.state {
            let default_val = parse_default(&field.default, &field.column_type);
            state.insert(field.name.clone(), default_val);
        }
        state
    }
}

fn parse_default(default_str: &str, column_type: &crate::types::ColumnType) -> Value {
    use crate::types::ColumnType;
    match column_type {
        ColumnType::Float64 => {
            Value::Float64(default_str.parse::<f64>().unwrap_or(0.0))
        }
        ColumnType::UInt64 => {
            Value::UInt64(default_str.parse::<u64>().unwrap_or(0))
        }
        ColumnType::Int64 => {
            Value::Int64(default_str.parse::<i64>().unwrap_or(0))
        }
        ColumnType::String => {
            // Strip surrounding quotes
            let s = default_str.trim_matches('\'').trim_matches('"');
            Value::String(s.to_string())
        }
        ColumnType::Boolean => {
            Value::Boolean(default_str == "true" || default_str == "1")
        }
        _ => column_type.default_value(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::*;
    use crate::storage::memory::MemoryBackend;

    fn pnl_reducer_def() -> ReducerDef {
        ReducerDef {
            name: "pnl_tracker".to_string(),
            source: "trades".to_string(),
            group_by: vec!["user".to_string()],
            state: vec![
                StateField {
                    name: "quantity".to_string(),
                    column_type: crate::types::ColumnType::Float64,
                    default: "0".to_string(),
                },
                StateField {
                    name: "cost_basis".to_string(),
                    column_type: crate::types::ColumnType::Float64,
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
    fn reducer_processes_rows_and_emits_output() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage);

        let rows = vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("alice", "buy", 5.0, 2100.0),
        ];

        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(output.len(), 2);

        // Both emits should have trade_pnl = 0 (buys)
        assert_eq!(output[0].get("trade_pnl"), Some(&Value::UInt64(0)));
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(10.0)));
        // user group-by column should be forwarded
        assert_eq!(output[0].get("user"), Some(&Value::String("alice".into())));

        assert_eq!(output[1].get("position_size"), Some(&Value::Float64(15.0)));
    }

    #[test]
    fn reducer_state_persists_across_blocks() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage);

        // Block 1: buy
        engine.process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)]).unwrap();

        // Block 2: sell
        let output = engine.process_block(1001, &[make_trade("alice", "sell", 5.0, 2200.0)]).unwrap();
        assert_eq!(output.len(), 1);
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // 5 * (2200 - 2000) = 1000
        assert!((pnl - 1000.0).abs() < 0.01);
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(5.0)));
    }

    #[test]
    fn reducer_rollback_restores_state() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage);

        // Block 1: buy 10 @ 2000
        engine.process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)]).unwrap();

        // Block 2: buy 5 @ 2100 (will be rolled back)
        engine.process_block(1001, &[make_trade("alice", "buy", 5.0, 2100.0)]).unwrap();

        // Rollback block 2
        let affected = engine.rollback(1000).unwrap();
        assert_eq!(affected, 1);

        // Process block 2 again with different data
        let output = engine.process_block(1001, &[make_trade("alice", "sell", 3.0, 2200.0)]).unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // After rollback, state is: qty=10, cost=20000, avg=2000
        // sell 3 @ 2200: pnl = 3 * (2200 - 2000) = 600
        assert!((pnl - 600.0).abs() < 0.01);
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(7.0)));
    }

    #[test]
    fn reducer_multiple_groups() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage);

        let rows = vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("bob", "buy", 5.0, 3000.0),
        ];

        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(output.len(), 2);

        // Alice: position 10
        let alice_out = output.iter().find(|r| r.get("user") == Some(&Value::String("alice".into()))).unwrap();
        assert_eq!(alice_out.get("position_size"), Some(&Value::Float64(10.0)));

        // Bob: position 5
        let bob_out = output.iter().find(|r| r.get("user") == Some(&Value::String("bob".into()))).unwrap();
        assert_eq!(bob_out.get("position_size"), Some(&Value::Float64(5.0)));
    }

    #[test]
    fn reducer_finalize_then_rollback() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage.clone());

        // Block 1000: buy 10
        engine.process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)]).unwrap();
        // Block 1001: buy 5
        engine.process_block(1001, &[make_trade("alice", "buy", 5.0, 2100.0)]).unwrap();

        // Finalize up to 1000
        let mut batch = StorageWriteBatch::new();
        engine.finalize(1000, &mut batch);
        storage.commit(&batch).unwrap();

        // Block 1002: buy 3 (will be rolled back)
        engine.process_block(1002, &[make_trade("alice", "buy", 3.0, 2200.0)]).unwrap();

        // Rollback to 1001
        engine.rollback(1001).unwrap();

        // After rollback: state should be at block 1001 (qty=15, cost=30500)
        let output = engine.process_block(1002, &[make_trade("alice", "sell", 15.0, 2100.0)]).unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // avg cost = 30500/15 = 2033.33, sell 15 @ 2100: pnl = 15 * (2100 - 2033.33) = 1000
        assert!((pnl - 1000.0).abs() < 0.01);
    }

    #[test]
    fn reducer_lua_runtime() {
        let def = ReducerDef {
            name: "counter".to_string(),
            source: "events".to_string(),
            group_by: vec![],
            state: vec![
                StateField {
                    name: "count".to_string(),
                    column_type: crate::types::ColumnType::Float64,
                    default: "0".to_string(),
                },
            ],
            body: ReducerBody::Lua {
                script: r#"
                    state.count = state.count + row.value
                    emit.total = state.count
                "#.to_string(),
            },
        };

        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(def, storage);

        let rows: Vec<RowMap> = vec![
            HashMap::from([("value".to_string(), Value::Float64(10.0))]),
            HashMap::from([("value".to_string(), Value::Float64(20.0))]),
        ];
        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].get("total"), Some(&Value::Float64(10.0)));
        assert_eq!(output[1].get("total"), Some(&Value::Float64(30.0)));
    }
}
