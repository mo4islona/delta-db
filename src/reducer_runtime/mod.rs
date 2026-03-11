pub mod event_rules;
pub mod lua;

use std::collections::HashMap;

use crate::types::{Row, RowMap, Value};

/// Trait for reducer process logic runtimes.
///
/// A runtime evaluates the reducer's process function for a single row,
/// reading and mutating state, and producing output (emit) rows.
///
/// Returns Vec<RowMap> since Lua reducers can emit multiple rows per input.
pub trait ReducerRuntime: Send + Sync {
    /// Process one input row against the current state.
    ///
    /// - `state`: mutable reducer state (read + write)
    /// - `row`: the input row from the source table (indexed access via ColumnRegistry)
    ///
    /// Returns zero or more output rows (the emitted columns).
    fn process(&self, state: &mut HashMap<String, Value>, row: &Row) -> Vec<RowMap>;
}
