use std::collections::HashMap;

use crate::types::{BlockCursor, DeltaBatch, DeltaOperation, DeltaRecord, Value};

/// Buffers delta batches while downstream hasn't acknowledged.
/// Records are appended on push; merging is deferred to flush time.
pub struct DeltaBuffer {
    /// Pending (unacked) records, appended in order.
    pending: Vec<DeltaRecord>,
    /// Next sequence number.
    next_sequence: u64,
    /// Finalized cursor as of last push.
    finalized_head: Option<BlockCursor>,
    /// Latest cursor as of last push.
    latest_head: Option<BlockCursor>,
    /// Max pending records before backpressure is applied.
    max_buffer_size: usize,
}

impl DeltaBuffer {
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            pending: Vec::new(),
            next_sequence: 1,
            finalized_head: None,
            latest_head: None,
            max_buffer_size,
        }
    }

    /// Returns true if backpressure should be applied (buffer is full).
    pub fn is_full(&self) -> bool {
        self.pending.len() >= self.max_buffer_size
    }

    /// Number of pending records in the buffer.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Update the finalized and latest head cursors without pushing records.
    pub fn set_heads(
        &mut self,
        finalized_head: Option<BlockCursor>,
        latest_head: Option<BlockCursor>,
    ) {
        self.finalized_head = finalized_head;
        self.latest_head = latest_head;
    }

    /// Push new delta records into the buffer (append-only, no merge on push).
    /// Merging is deferred to flush() time.
    pub fn push(
        &mut self,
        records: Vec<DeltaRecord>,
        finalized_head: Option<BlockCursor>,
        latest_head: Option<BlockCursor>,
    ) {
        self.finalized_head = finalized_head;
        self.latest_head = latest_head;
        self.pending.extend(records);
    }

    /// Flush: merge and drain all pending records into a DeltaBatch.
    /// Returns None if there are no pending records (or all cancel out).
    pub fn flush(&mut self) -> Option<DeltaBatch> {
        if self.pending.is_empty() {
            return None;
        }

        // Merge records by hash(table, key) in a single pass.
        // Uses hash-only index (no table string clone). Collision probability
        // for N records with 64-bit hash is ~N²/2^65, negligible in practice.
        let mut index: HashMap<u64, usize> =
            HashMap::with_capacity(self.pending.len());
        let mut merged: Vec<DeltaRecord> = Vec::with_capacity(self.pending.len());

        for record in self.pending.drain(..) {
            let key_hash = hash_delta_key(&record.table, &record.key);

            if let Some(&idx) = index.get(&key_hash) {
                // In-place merge: mutate existing record, move fields from incoming.
                // Avoids cloning table, key, values, and prev_values HashMaps.
                if !merge_in_place(&mut merged[idx], record) {
                    // Records cancel out — mark as cancelled
                    merged[idx].operation = DeltaOperation::Delete;
                    merged[idx].prev_values = None;
                    merged[idx].values.clear();
                }
            } else {
                let idx = merged.len();
                index.insert(key_hash, idx);
                merged.push(record);
            }
        }

        // Filter out cancelled records and group by table
        let mut tables: HashMap<String, Vec<DeltaRecord>> = HashMap::new();
        for record in merged.into_iter().filter(|r| !is_cancelled(r)) {
            if let Some(vec) = tables.get_mut(&record.table) {
                vec.push(record);
            } else {
                let table = record.table.clone();
                tables.insert(table, vec![record]);
            }
        }

        if tables.is_empty() {
            return None;
        }

        let seq = self.next_sequence;
        self.next_sequence += 1;

        Some(DeltaBatch {
            sequence: seq,
            finalized_head: self.finalized_head.clone(),
            latest_head: self.latest_head.clone(),
            tables,
        })
    }

    /// Acknowledge a batch (currently a no-op; future: track acked sequences).
    pub fn ack(&mut self, _sequence: u64) {
        // In a full implementation, this would track which sequences have been
        // acknowledged by the downstream target to enable retry/resume.
    }
}

/// Merge `incoming` into `existing` in place, moving fields instead of cloning.
/// Returns `false` if the records cancel out (insert + delete = no-op).
fn merge_in_place(existing: &mut DeltaRecord, incoming: DeltaRecord) -> bool {
    match (&existing.operation, &incoming.operation) {
        // Insert then Update: net Insert with latest values
        (DeltaOperation::Insert, DeltaOperation::Update) => {
            existing.values = incoming.values;
            // operation stays Insert, prev_values stays None
            true
        }

        // Insert then Delete: cancel out
        (DeltaOperation::Insert, DeltaOperation::Delete) => false,

        // Update then Update: keep original prev_values, latest values
        (DeltaOperation::Update, DeltaOperation::Update) => {
            existing.values = incoming.values;
            // operation stays Update, prev_values stays from first update
            true
        }

        // Update then Delete: net Delete with original prev_values
        (DeltaOperation::Update, DeltaOperation::Delete) => {
            existing.operation = DeltaOperation::Delete;
            existing.values = incoming.values;
            // prev_values stays from first update
            true
        }

        // Delete then Insert: net Update (prev = old delete values)
        (DeltaOperation::Delete, DeltaOperation::Insert) => {
            existing.prev_values = Some(std::mem::take(&mut existing.values));
            existing.operation = DeltaOperation::Update;
            existing.values = incoming.values;
            true
        }

        // Same operation following same: just replace in place
        _ => {
            *existing = incoming;
            true
        }
    }
}

/// Check if a record has been cancelled (insert + delete = no net effect).
fn is_cancelled(record: &DeltaRecord) -> bool {
    record.operation == DeltaOperation::Delete
        && record.prev_values.is_none()
        && record.values.is_empty()
}

fn hash_delta_key(table: &str, key: &HashMap<String, Value>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table.hash(&mut hasher);
    // Commutative hash: wrapping_add of per-field hashes (order-independent, no allocation)
    let mut combined: u64 = 0;
    for (k, v) in key {
        let mut field_hasher = std::collections::hash_map::DefaultHasher::new();
        k.hash(&mut field_hasher);
        v.hash(&mut field_hasher);
        combined = combined.wrapping_add(field_hasher.finish());
    }
    hasher.write_u64(combined);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DeltaOperation;

    fn make_insert(table: &str, key_val: &str, data: &str) -> DeltaRecord {
        DeltaRecord {
            table: table.to_string(),
            operation: DeltaOperation::Insert,
            key: HashMap::from([("id".to_string(), Value::String(key_val.to_string()))]),
            values: HashMap::from([("data".to_string(), Value::String(data.to_string()))]),
            prev_values: None,
        }
    }

    fn make_update(table: &str, key_val: &str, data: &str, prev: &str) -> DeltaRecord {
        DeltaRecord {
            table: table.to_string(),
            operation: DeltaOperation::Update,
            key: HashMap::from([("id".to_string(), Value::String(key_val.to_string()))]),
            values: HashMap::from([("data".to_string(), Value::String(data.to_string()))]),
            prev_values: Some(HashMap::from([("data".to_string(), Value::String(prev.to_string()))])),
        }
    }

    fn make_delete(table: &str, key_val: &str) -> DeltaRecord {
        DeltaRecord {
            table: table.to_string(),
            operation: DeltaOperation::Delete,
            key: HashMap::from([("id".to_string(), Value::String(key_val.to_string()))]),
            values: HashMap::new(),
            prev_values: Some(HashMap::from([("data".to_string(), Value::String("old".to_string()))])),
        }
    }

    #[test]
    fn empty_buffer_flush_returns_none() {
        let mut buffer = DeltaBuffer::new(100);
        assert!(buffer.flush().is_none());
    }

    fn cursor(n: u64) -> Option<BlockCursor> {
        if n == 0 {
            None
        } else {
            Some(BlockCursor { number: n, hash: format!("0x{n:x}") })
        }
    }

    #[test]
    fn flush_returns_batch_and_clears() {
        let mut buffer = DeltaBuffer::new(100);
        buffer.push(vec![make_insert("t", "1", "a")], cursor(0), cursor(1000));

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.sequence, 1);
        assert_eq!(batch.record_count(), 1);
        assert_eq!(batch.latest_head.as_ref().unwrap().number, 1000);

        // Second flush should be empty
        assert!(buffer.flush().is_none());
    }

    #[test]
    fn sequence_numbers_increment() {
        let mut buffer = DeltaBuffer::new(100);

        buffer.push(vec![make_insert("t", "1", "a")], cursor(0), cursor(1000));
        let b1 = buffer.flush().unwrap();

        buffer.push(vec![make_insert("t", "2", "b")], cursor(0), cursor(1001));
        let b2 = buffer.flush().unwrap();

        assert_eq!(b1.sequence, 1);
        assert_eq!(b2.sequence, 2);
    }

    #[test]
    fn merge_insert_then_update() {
        let mut buffer = DeltaBuffer::new(100);
        buffer.push(vec![make_insert("t", "1", "a")], cursor(0), cursor(1000));
        buffer.push(vec![make_update("t", "1", "b", "a")], cursor(0), cursor(1001));

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        // Net effect: Insert with latest values
        assert_eq!(records[0].operation, DeltaOperation::Insert);
        assert_eq!(
            records[0].values.get("data"),
            Some(&Value::String("b".into()))
        );
        assert!(records[0].prev_values.is_none());
    }

    #[test]
    fn merge_insert_then_delete_cancels() {
        let mut buffer = DeltaBuffer::new(100);
        buffer.push(vec![make_insert("t", "1", "a")], cursor(0), cursor(1000));
        buffer.push(
            vec![DeltaRecord {
                table: "t".to_string(),
                operation: DeltaOperation::Delete,
                key: HashMap::from([("id".to_string(), Value::String("1".to_string()))]),
                values: HashMap::new(),
                prev_values: Some(HashMap::from([("data".to_string(), Value::String("a".to_string()))])),
            }],
            cursor(0),
            cursor(1001),
        );

        // The merged result should be None (cancelled), so flush returns None
        let batch = buffer.flush();
        assert!(batch.is_none());
    }

    #[test]
    fn merge_update_then_update() {
        let mut buffer = DeltaBuffer::new(100);
        buffer.push(vec![make_update("t", "1", "b", "a")], cursor(0), cursor(1000));
        buffer.push(vec![make_update("t", "1", "c", "b")], cursor(0), cursor(1001));

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].operation, DeltaOperation::Update);
        assert_eq!(records[0].values.get("data"), Some(&Value::String("c".into())));
        // prev_values should be from the first update
        assert_eq!(
            records[0].prev_values.as_ref().unwrap().get("data"),
            Some(&Value::String("a".into()))
        );
    }

    #[test]
    fn merge_delete_then_insert() {
        let mut buffer = DeltaBuffer::new(100);
        buffer.push(vec![make_delete("t", "1")], cursor(0), cursor(1000));
        buffer.push(vec![make_insert("t", "1", "new")], cursor(0), cursor(1001));

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        // Delete then Insert = Update
        assert_eq!(records[0].operation, DeltaOperation::Update);
        assert_eq!(records[0].values.get("data"), Some(&Value::String("new".into())));
    }

    #[test]
    fn different_keys_not_merged() {
        let mut buffer = DeltaBuffer::new(100);
        buffer.push(
            vec![make_insert("t", "1", "a"), make_insert("t", "2", "b")],
            cursor(0),
            cursor(1000),
        );

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.record_count(), 2);
    }

    #[test]
    fn different_tables_not_merged() {
        let mut buffer = DeltaBuffer::new(100);
        buffer.push(
            vec![make_insert("t1", "1", "a"), make_insert("t2", "1", "b")],
            cursor(0),
            cursor(1000),
        );

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.record_count(), 2);
        assert_eq!(batch.records_for("t1").len(), 1);
        assert_eq!(batch.records_for("t2").len(), 1);
    }

    #[test]
    fn backpressure_when_full() {
        let mut buffer = DeltaBuffer::new(2);
        assert!(!buffer.is_full());

        buffer.push(vec![make_insert("t", "1", "a"), make_insert("t", "2", "b")], cursor(0), cursor(1000));
        assert!(buffer.is_full());

        buffer.flush();
        assert!(!buffer.is_full());
    }

    #[test]
    fn finalized_and_latest_cursor_tracking() {
        let mut buffer = DeltaBuffer::new(100);

        buffer.push(vec![make_insert("t", "1", "a")], cursor(500), cursor(1000));
        buffer.push(vec![make_insert("t", "2", "b")], cursor(600), cursor(1100));

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.finalized_head.as_ref().unwrap().number, 600);
        assert_eq!(batch.latest_head.as_ref().unwrap().number, 1100);
    }
}
