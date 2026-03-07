pub mod event_rules;
pub mod lua;

use std::collections::HashMap;

use crate::types::{RowMap, Value};

/// Trait for reducer process logic runtimes.
///
/// A runtime evaluates the reducer's process function for a single row,
/// reading and mutating state, and producing an output (emit) row.
///
/// Returns a RowMap (HashMap) since the output columns may be dynamic
/// (especially for Lua reducers).
pub trait ReducerRuntime: Send + Sync {
    /// Process one input row against the current state.
    ///
    /// - `state`: mutable reducer state (read + write)
    /// - `row`: the input row from the source table
    ///
    /// Returns an output row (the emitted columns), or None if the row should be dropped.
    fn process(&self, state: &mut HashMap<String, Value>, row: &RowMap) -> Option<RowMap>;
}
