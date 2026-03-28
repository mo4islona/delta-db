# Known Issues

Findings from code review.  
Reviewed by: Claude Sonnet 4.6 · GPT-5.4 · Claude Opus 4.6

---

## 🔴 Critical

### 1. `Delete→Insert` merge silently destroys `prev_values`
**File:** `src/delta.rs:161`

In `merge_in_place`, the `(Delete, Insert)` arm does:
```rust
existing.prev_values = Some(std::mem::take(&mut existing.values));
```
For a `Delete` record, `values` is always `{}`. `take` pulls that empty map and stores it as `prev_values`, overwriting the real pre-delete field values. Every downstream consumer of the merged `Update` sees `prev_values = Some({})` instead of the actual deleted row's data. The test for this case never checks `prev_values`.

**Fix:** Keep `existing.prev_values` unchanged; only update `operation` and `values`:
```rust
(DeltaOperation::Delete, DeltaOperation::Insert) => {
    existing.operation = DeltaOperation::Update;
    existing.values = incoming.values;
    true
}
```

---

### 2. `Insert→Delete→Insert` produces `Update` on a non-existent row
**File:** `src/delta.rs:84–89`, `src/delta.rs:159–165`

When an Insert+Delete cancels (`merge_in_place` returns `false`), the slot becomes a sentinel `{Delete, prev_values: None, values: {}}`, but its entry in `index` is **not removed**. A subsequent Insert for the same key finds the old index entry, enters the `(Delete, Insert)` arm, and produces `{Update, prev_values: Some({})}` — but the row never existed in the DB before this batch. Downstream storage will attempt to UPDATE a non-existent row.

**Fix:** When cancelling a slot, remove its key from `index` so the next Insert is treated as a fresh insert. Alternatively, in the `(Delete, Insert)` arm, check `existing.prev_values.is_none()` to distinguish a cancelled sentinel from a genuine prior Delete.

---

### 3. `unsafe impl Sync` on `LuaRuntime` is unsound
**File:** `src/reducer_runtime/lua.rs:44`

The safety comment claims exclusion is provided by `&mut self`, but `Sync` permits taking simultaneous `&LuaRuntime` references from multiple threads. If `LuaRuntime` is placed in an `Arc` — directly or via a trait object — concurrent `process()` calls on the non-thread-safe Lua C state are possible, causing undefined behaviour.

**Fix:** Remove the `unsafe impl Sync` and add `PhantomData<*mut ()>` to make `LuaRuntime` explicitly `!Sync`, or use a `Mutex` to protect the Lua state.

---

### 4. User Lua script errors panic the process
**File:** `src/reducer_runtime/lua.rs:238`, `src/reducer_runtime/lua.rs:257`

```rust
let (...) = call.call(()).expect("Lua script execution failed");
```
Any runtime error in a user-supplied Lua script (type error, stack overflow, explicit `error()` call) aborts the entire process. `mlua::Function::call` already returns `mlua::Result`.

**Fix:** Propagate the error as a `Result` instead of calling `.expect`.

---

### 5. `process_batch` crash orphans raw rows, silently diverging computed state
**File:** `src/db.rs:157–172` · `src/engine/raw_table.rs:45`

`process_batch()` writes raw rows to RocksDB immediately via `storage.put_raw_rows()`, but `latest_block` metadata is only persisted at the next `finalize()`. A crash between these two leaves orphaned raw rows in storage. On recovery:

1. `open()` reads the stale pre-crash `latest_block` from metadata.
2. `replay_unfinalized(finalized+1, latest)` replays only up to that stale value — the orphaned rows are **never replayed** into reducer/MV state.
3. Raw storage and computed state silently diverge.
4. If the same block numbers are re-ingested later, the old raw rows are silently overwritten with no error.

The `ingest()` path avoids this by deferring all writes into a single atomic batch. `process_batch()` has no equivalent protection (a doc-comment warning was added, but the underlying issue remains).

**Fix:** Make `process_batch` use deferred writes with a trailing metadata commit, matching the `ingest()` pattern.

---

## 🟠 High

### 6. `u64::MAX` overflow in `memory.rs` rollback/delete helpers
**File:** `src/storage/memory.rs:79`, `src/storage/memory.rs:97`, `src/storage/memory.rs:159`

All three methods do `after_block + 1` without a guard:
```rust
.range((table.to_string(), after_block + 1)..)
```
When `after_block == u64::MAX`, this overflows to `0` in release mode, causing the range to cover the entire map and silently deleting **all** entries instead of none. The RocksDB counterparts all guard against this; the memory backend does not.

**Fix:** Add the same guard used in the RocksDB backend:
```rust
if after_block == BlockNumber::MAX { return Ok(()); }
```

---

### 7. `u64::MAX` overflow on rollback wipes all history
**File:** `src/engine/reducer.rs:378`, `src/engine/mv.rs:287`

```rust
let rolled_back = self.block_groups.split_off(&(fork_point + 1));
```
When `fork_point == u64::MAX`, `fork_point + 1` wraps to `0` and `split_off(&0)` returns the **entire** map, rolling back all state to empty. `raw_table.rs:159` already has the guard for this; it is missing in the reducer and MV paths.

**Fix:**
```rust
if fork_point == BlockNumber::MAX { return Ok(0); }
```

---

### 8. `parse_or_expr` never parses `OR` — OR conditions silently fail
**File:** `src/schema/parser.rs:988–1001`

```rust
fn parse_or_expr(input: &str) -> Result<Expr, Error> {
    let parts = split_binary_op(input, &[" AND ", " and "]);  // AND only
```
Any schema condition containing `OR` (e.g. `WHEN row.side = 'buy' OR row.side = 'sell'`) fails at parse time. `BinaryOp::Or` is defined in the AST and handled in `event_rules.rs` but is unreachable.

**Fix:** Add an OR-splitting level above the AND level:
```
parse_or_expr  → split on OR  → parse_and_expr
parse_and_expr → split on AND → parse_comparison
```

---

### 9. `u64 > i64::MAX` silently demoted to lossy `f64` or `Null` in JSON conversion
**File:** `src/json_conv.rs:14–19`

```rust
if let Some(i) = n.as_i64() {        // fails for values > 9.2e18
    ...
} else if let Some(f) = n.as_f64() { // lossy for 2^53 < v <= u64::MAX
    Value::Float64(f)
} else {
    Value::Null                        // data silently dropped
}
```

**Fix:** Insert a `u64` branch before the `f64` fallback:
```rust
} else if let Some(u) = n.as_u64() {
    Value::UInt64(u)
}
```

---

### 10. Stale Lua row fields leak across rows
**File:** `src/reducer_runtime/lua.rs:194–220`

The persistent Lua `row` table is updated field-by-field via `row.iter()`, which skips `Null` values (see `types.rs:121–128`). A column that is non-null in row N but null in row N+1 retains its row-N value in the Lua table. The `emit` table is cleared per row but `row` is not. This affects even different group keys, since all rows share the same Lua VM.

**Fix:** Clear the `row` table before populating it each call (e.g. `for k in pairs(row) do row[k] = nil end` in the compiled wrapper), or explicitly set all absent columns to `nil`.

---

### 11. `take_raw_rows_after` in RocksDB backend is non-atomic
**File:** `src/storage/rocks.rs:219–240`

The RocksDB implementation reads keys into a `Vec` (lines 222–229) and then issues a separate `WriteBatch` delete (lines 232–238). A crash between these two steps leaves rows in storage while the caller has already consumed them. On restart, `replay_unfinalized` reprocesses these rows and double-counts their contributions into reducer/MV state.

**Fix:** Use `delete_range_cf` within the same `WriteBatch` as part of an atomic read+delete, or always use the batch rollback path.

---

### 12. `replay_unfinalized` silently skips direct MV nodes
**File:** `src/engine/dag.rs:449–514`

During crash recovery, raw table rows are placed into `row_cache` (a `Vec<Row>`). Reducers correctly check `row_cache` first during replay (lines 468/478), but the `PipelineNode::MV` handler (lines 490–510) only checks `output_rows` — which is empty for raw table sources. Any MV that sources directly from a raw table (not through a reducer) receives no data during replay and ends up with stale/incorrect aggregation state after restart.

**Fix:** In `replay_unfinalized`, also insert raw table rows into `output_rows` as `Vec<RowMap>`, or have the MV handler fall back to `row_cache` with a `Row→RowMap` conversion.

---

### 13. All numeric aggregations silently corrupt blockchain-scale integers
**File:** `src/engine/aggregation.rs:71`, `217`, `305`, `395`

`SUM`, `MIN`, `MAX`, and `AVG` all normalize inputs through `Value::as_f64()`, which casts `UInt64`/`Int64` with `as f64`. Any value above 2⁵³ (~9×10¹⁵) silently loses precision. Token amounts in base units (EVM wei, Solana lamports) routinely exceed this.

**Fix:** Implement separate integer-space accumulation paths for `UInt64`/`Int64` inputs instead of routing everything through `f64`.

---

### 14. `ingest()` accepts invalid block numbers via lossy coercion
**File:** `src/db.rs:276`

`block_number` is extracted with `v.as_u64()`, which casts `Int64(-1)` → `u64::MAX` and `Float64(1.9)` → `1` without error. A negative block number silently triggers a full-history rollback (see issue #7).

**Fix:** Require `Value::UInt64` explicitly for `block_number` and reject all other variants with an error.

---

## 🟡 Medium

### 15. `SumAgg::has_data` uses float equality — valid zero-sum groups deleted
**File:** `src/engine/aggregation.rs:100–102`

```rust
fn has_data(&self) -> bool {
    self.finalized != 0.0 || !self.blocks.is_empty()
}
```
A group whose finalized blocks sum to exactly `0.0` (e.g. +10 then −10, then finalized) returns `false`. In `emit_deltas` (`mv.rs:476–480`), this is the sole gate on group deletion — the group is incorrectly emitted as a `Delete` and removed from storage. Masked when a `CountAgg` is also present, but manifests in sum-only MVs.

**Fix:** Track a separate `finalized_count: u64` flag set whenever any block is finalized, regardless of the resulting sum.

---

### 16. `UInt64`/`Int64` lose precision when passed into Lua
**File:** `src/reducer_runtime/lua.rs:422–423`

```rust
Value::UInt64(v) => Ok(LuaValue::Number(*v as f64)),
Value::Int64(v)  => Ok(LuaValue::Number(*v as f64)),
```
Lua 5.4 supports 64-bit integers via `LuaValue::Integer`. All integers > 2⁵³ are silently rounded.

**Fix:** Use `LuaValue::Integer(*v as i64)` for values that fit in `i64`; encode larger `u64` values as strings or return an error.

---

### 17. Lua sandbox does not remove `require` / `package`
**File:** `src/reducer_runtime/lua.rs:379–387`

`sandbox()` removes `os`, `io`, `debug`, etc., but not `require` or `package`. A script can call `require("os")` to recover the cached `os` table from `package.loaded`, or load a native C extension from `LUA_CPATH`, fully escaping the sandbox.

**Fix:** Add `"require"` and `"package"` to the nil-out list in `sandbox()`.

---

### 18. Large JSON integers silently become `0.0` in Lua conversion
**File:** `src/reducer_runtime/lua.rs:542`

```rust
serde_json::Value::Number(n) => Ok(LuaValue::Number(n.as_f64().unwrap_or(0.0))),
```
`as_f64()` can return `None` for integers that overflow `f64`, producing a silent `0.0`.

**Fix:** Attempt integer conversion first (`as_i64()` / `as_u64()`), fall back to `f64` only if needed.

---

### 19. Hash-only delta deduplication can silently drop unrelated records on collision
**File:** `src/delta.rs:75–107`

A 64-bit hash of `(table, key)` is the sole identity check for detecting duplicates. A collision between two different records causes `merge_in_place` to be called on them. If one is `Insert` and the other `Delete`, both records are permanently dropped from the batch (marked cancelled) with no error. The `combined` hash uses `wrapping_add`, which is commutative and has no equality confirmation step after the hash match.

**Fix:** After a hash match, confirm key equality before merging. Use a `HashMap<(table, key_bytes), usize>` or check `existing.key == incoming.key`.

---

### 20. `wrapping_add` key hash has poor collision resistance for structured keys
**File:** `src/delta.rs:186–194`

Per-field hashes are combined with `wrapping_add`, which is commutative (`{a, b}` and `{b, a}` collide when hashes are equal). For monotonically increasing integer keys, field hashes cluster near zero, further elevating collision probability beyond the naive `N²/2⁶⁵` bound.

**Fix:** Use XOR + `rotate_left` mixing, or feed fields sequentially into a single `Hasher`.

---

### 21. Corrupt stored data panics instead of returning `Err`
**File:** `src/storage/mod.rs:315–330`, `src/storage/rocks.rs:170`, `227`, `291`

Byte-level decoding uses `try_into().unwrap()`, unchecked index arithmetic, and `_ => panic!("unknown value type tag: {tag}")`. Truncated or corrupt bytes in RocksDB abort the process rather than returning a recoverable error.

**Fix:** Use checked slice access (`bytes.get(pos..pos+4).ok_or(...)`) and propagate errors through `Result` in all decode paths.

---

### 22. `Row::from_values` length check stripped in release builds
**File:** `src/types.rs:68`

```rust
debug_assert_eq!(values.len(), registry.len());
```
In release builds, a `values` vec shorter than the registry causes a panic on the first out-of-bounds `Row::get` / `Row::set` access. This is a public constructor.

**Fix:** Use `assert!` or return a `Result`.

---

### 23. `split_as_alias` uses char-count index for byte-offset slicing
**File:** `src/schema/parser.rs:961–975`

```rust
for (i, c) in s.chars().enumerate() {  // i is char count, not byte offset
    ...
    return Some((s[..i].trim(), s[i + 4..].trim()));  // byte-indexed by char count
}
```
Any non-ASCII character before the `AS` keyword causes a panic on a non-UTF-8 boundary.

**Fix:** Use `s.char_indices()` to get the byte offset.

---

### 24. `read_token` does not handle SQL doubled-quote escaping
**File:** `src/schema/parser.rs:1266–1280`

```rust
while p < bytes.len() && bytes[p] != quote { p += 1; }
```
The SQL string `'it''s'` terminates at the first `'`, silently truncating the default value to `'it'`.

**Fix:** Handle `''` escape sequences inside quoted strings.

---

### 25. `set_rollback_chain` called twice with unrelated data
**File:** `src/db.rs:305–311`

The function is called once for all unfinalized hashes and again for just the finalized head. This works only because the underlying `BTreeMap::insert` merges entries. If the contract ever changes to "replace", all rollback-chain hashes are silently discarded.

**Fix:** Introduce a separate `set_finalized_head(block, hash)` call instead of reusing `set_rollback_chain`.

---

## 🔵 Performance

### 26. `Row::PartialEq` allocates two `HashMap`s per comparison
**File:** `src/types.rs:131–136`

```rust
fn eq(&self, other: &Self) -> bool {
    self.to_map() == other.to_map()
}
```
`to_map()` clones every non-null field into a fresh `HashMap`. `Row` equality appears in hot paths (group key deduplication).

**Fix:** Compare the underlying `values` arrays directly with null-aware field-by-field comparison.

---

### 27. `Value::JSON` hashing allocates a `String` on every hash call
**File:** `src/types.rs:340`

```rust
Value::JSON(v) => v.to_string().hash(state),
```
Every group-by or hash-map operation involving a JSON column heap-allocates the serialized representation.

**Fix:** Hash the raw internal bytes/representation without serializing to a `String`.

---

### 28. `create_agg_vec` is O(n²) per new group key
**File:** `src/engine/mv.rs:549–569`

Nested `find_map` + `any` loops over `output_columns` and `select` items on every new group key creation. `self.agg_funcs` is already precomputed at construction time in exactly the correct order.

**Fix:**
```rust
fn create_agg_vec(&self) -> Vec<Box<dyn AggregationFunc>> {
    self.agg_funcs.iter().map(create_agg).collect()
}
```

---

### 29. Triple HashMap lookup in per-row hot path
**File:** `src/engine/reducer.rs:222–226`

```rust
if !self.state_cache.contains_key(&group_key_bytes) {  // lookup #1
    let state = self.load_state(&group_key_bytes)?;
    self.state_cache.insert(group_key_bytes.clone(), state); // lookup #2
}
let state = self.state_cache.get_mut(&group_key_bytes).unwrap(); // lookup #3
```

**Fix:** Use the entry API to reduce to a single lookup:
```rust
let state = match self.state_cache.entry(group_key_bytes.clone()) {
    Entry::Occupied(e) => e.into_mut(),
    Entry::Vacant(e) => e.insert(self.load_state(&group_key_bytes)?),
};
```

---

### 30. O(P×B) linear branch scan per pipeline node
**File:** `src/engine/dag.rs:363–410`, `547–583`, `599–626`

In all three paths (process, rollback, finalize), each `PipelineNode` calls `self.branches.iter_mut().find(|b| b.reducer_name == *name)` — O(P×B) total per batch.

**Fix:** Build a `HashMap<String, usize>` (reducer name → branch index) once at construction for O(1) lookups.

---

### 31. `AvgAgg::add_block` allocates `Vec<f64>` per row per AVG column
**File:** `src/engine/aggregation.rs:395`

`add_block` collects a `Vec<f64>` just to compute sum/count, but the caller always passes a single-element slice via `std::slice::from_ref(&value)`.

**Fix:** Accumulate sum and count directly in the iterator loop without collecting.

---

### 32. Unnecessary `GroupKey` clones in hot row loop
**File:** `src/engine/mv.rs:243–270`

Each new group key is cloned twice (`prev_output.insert` and `touched_keys.insert`) before being moved into `block_groups`. `GroupKey` is `Vec<Value>`, making these non-trivial allocations in the per-row path.

**Fix:** Defer the `prev_output` insert until after the loop, or restructure to move the key and re-borrow as needed.
