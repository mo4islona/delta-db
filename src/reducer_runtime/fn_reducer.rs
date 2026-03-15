use std::collections::HashMap;

use crate::types::{Row, RowMap, Value};

use super::ReducerRuntime;

type State = HashMap<String, Value>;

/// A reducer runtime backed by a Rust closure.
///
/// Used for benchmarks and tests to measure the overhead of the
/// `ReducerRuntime` trait dispatch without Lua VM or host FFI costs.
pub struct FnReducerRuntime {
    process_fn: Box<dyn Fn(&mut State, &Row) -> Vec<RowMap> + Send + Sync>,
}

impl FnReducerRuntime {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&mut State, &Row) -> Vec<RowMap> + Send + Sync + 'static,
    {
        Self {
            process_fn: Box::new(f),
        }
    }
}

impl ReducerRuntime for FnReducerRuntime {
    fn process(&self, state: &mut State, row: &Row) -> Vec<RowMap> {
        (self.process_fn)(state, row)
    }
}
