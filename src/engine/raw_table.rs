use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::schema::ast::TableDef;
use crate::storage::{self, StorageBackend};
use crate::types::{BlockNumber, ColumnRegistry, DeltaOperation, DeltaRecord, Row, RowMap, Value};

/// Manages ingestion, storage, and rollback for a single raw table.
pub struct RawTableEngine {
    def: TableDef,
    storage: Arc<dyn StorageBackend>,
    registry: Arc<ColumnRegistry>,
}

impl RawTableEngine {
    pub fn new(def: TableDef, storage: Arc<dyn StorageBackend>) -> Self {
        let names: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
        let registry = Arc::new(ColumnRegistry::new(names));
        Self { def, storage, registry }
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn def(&self) -> &TableDef {
        &self.def
    }

    pub fn registry(&self) -> &Arc<ColumnRegistry> {
        &self.registry
    }

    /// Ingest a batch of rows for a given block number.
    /// Encodes directly from RowMaps using the column registry (no intermediate Row objects).
    /// Returns delta records (one Insert per row).
    pub fn ingest(&self, block: BlockNumber, row_maps: &[RowMap]) -> Result<Vec<DeltaRecord>> {
        if row_maps.is_empty() {
            return Ok(Vec::new());
        }

        // Encode directly from RowMaps — no Row conversion needed
        let encoded = storage::encode_rows_from_maps(row_maps, &self.registry);
        self.storage.put_raw_rows(&self.def.name, block, &encoded)?;

        let deltas = row_maps
            .iter()
            .enumerate()
            .map(|(idx, values)| {
                let mut key = HashMap::new();
                key.insert("block_number".to_string(), Value::UInt64(block));
                key.insert("_row_index".to_string(), Value::UInt64(idx as u64));

                DeltaRecord {
                    table: self.def.name.clone(),
                    operation: DeltaOperation::Insert,
                    key,
                    values: values.clone(),
                    prev_values: None,
                }
            })
            .collect();

        Ok(deltas)
    }

    /// Ingest rows without creating delta records (for virtual tables).
    /// Stores the rows for replay but skips the expensive delta record allocation.
    pub fn ingest_no_deltas(&self, block: BlockNumber, row_maps: &[RowMap]) -> Result<()> {
        if row_maps.is_empty() {
            return Ok(());
        }
        let encoded = storage::encode_rows_from_maps(row_maps, &self.registry);
        self.storage.put_raw_rows(&self.def.name, block, &encoded)?;
        Ok(())
    }

    /// Roll back all rows where block_number > fork_point.
    /// Returns compensating Delete delta records for the rolled-back rows.
    pub fn rollback(&self, fork_point: BlockNumber) -> Result<Vec<DeltaRecord>> {
        let rolled_back = self
            .storage
            .take_raw_rows_after(&self.def.name, fork_point)?;

        let mut deltas = Vec::new();
        for (block, data) in rolled_back {
            let rows = storage::decode_rows(&data, &self.registry);
            for (idx, row) in rows.into_iter().enumerate() {
                let mut key = HashMap::new();
                key.insert("block_number".to_string(), Value::UInt64(block));
                key.insert("_row_index".to_string(), Value::UInt64(idx as u64));

                deltas.push(DeltaRecord {
                    table: self.def.name.clone(),
                    operation: DeltaOperation::Delete,
                    key,
                    values: row.to_map(),
                    prev_values: None,
                });
            }
        }

        Ok(deltas)
    }

    /// Get all rows for a block range (inclusive). Used for reducer replay.
    pub fn get_rows(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, Vec<Row>)>> {
        let raw = self
            .storage
            .get_raw_rows(&self.def.name, from_block, to_block)?;
        Ok(raw
            .into_iter()
            .map(|(block, data)| (block, storage::decode_rows(&data, &self.registry)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryBackend;
    use crate::types::ColumnType;
    use crate::schema::ast::ColumnDef;

    fn test_table_def() -> TableDef {
        TableDef {
            name: "trades".to_string(),
            columns: vec![
                ColumnDef { name: "block_number".to_string(), column_type: ColumnType::UInt64 },
                ColumnDef { name: "user".to_string(), column_type: ColumnType::String },
                ColumnDef { name: "amount".to_string(), column_type: ColumnType::Float64 },
            ],
            virtual_table: false,
        }
    }

    fn make_row_map(user: &str, amount: f64) -> RowMap {
        HashMap::from([
            ("user".to_string(), Value::String(user.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
        ])
    }

    #[test]
    fn ingest_produces_insert_deltas() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        let rows = vec![make_row_map("alice", 10.0), make_row_map("bob", 20.0)];
        let deltas = engine.ingest(1000, &rows).unwrap();

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].operation, DeltaOperation::Insert);
        assert_eq!(deltas[0].table, "trades");
        assert_eq!(deltas[0].key.get("block_number"), Some(&Value::UInt64(1000)));
        assert_eq!(deltas[0].key.get("_row_index"), Some(&Value::UInt64(0)));
        assert_eq!(deltas[0].values.get("user"), Some(&Value::String("alice".into())));

        assert_eq!(deltas[1].key.get("_row_index"), Some(&Value::UInt64(1)));
        assert_eq!(deltas[1].values.get("user"), Some(&Value::String("bob".into())));
    }

    #[test]
    fn ingest_empty_batch_is_noop() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        let deltas = engine.ingest(1000, &[]).unwrap();
        assert!(deltas.is_empty());
    }

    #[test]
    fn ingest_stores_rows_retrievable() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();
        engine.ingest(1001, &[make_row_map("bob", 20.0)]).unwrap();

        let rows = engine.get_rows(1000, 1001).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1000);
        assert_eq!(rows[1].0, 1001);
    }

    #[test]
    fn rollback_deletes_rows_and_emits_deltas() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();
        engine.ingest(1001, &[make_row_map("bob", 20.0)]).unwrap();
        engine.ingest(1002, &[make_row_map("carol", 30.0), make_row_map("dave", 40.0)]).unwrap();

        // Rollback to block 1000 (delete 1001 and 1002)
        let deltas = engine.rollback(1000).unwrap();

        // Should get 3 Delete deltas (1 from block 1001 + 2 from block 1002)
        assert_eq!(deltas.len(), 3);
        for d in &deltas {
            assert_eq!(d.operation, DeltaOperation::Delete);
            assert_eq!(d.table, "trades");
        }

        // Verify bob's row is in the deltas
        assert!(deltas.iter().any(|d| d.values.get("user") == Some(&Value::String("bob".into()))));

        // Verify storage only has block 1000
        let remaining = engine.get_rows(1000, 1010).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 1000);
    }

    #[test]
    fn rollback_to_latest_is_noop() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();

        let deltas = engine.rollback(1000).unwrap();
        assert!(deltas.is_empty());

        let remaining = engine.get_rows(1000, 1000).unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn full_cycle_ingest_rollback_reingest() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        // Ingest 3 blocks
        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();
        engine.ingest(1001, &[make_row_map("bob", 20.0)]).unwrap();
        engine.ingest(1002, &[make_row_map("carol", 30.0)]).unwrap();

        // Rollback block 1002
        let rollback_deltas = engine.rollback(1001).unwrap();
        assert_eq!(rollback_deltas.len(), 1);
        assert_eq!(
            rollback_deltas[0].values.get("user"),
            Some(&Value::String("carol".into()))
        );

        // Re-ingest block 1002 with different data (reorg)
        let new_deltas = engine.ingest(1002, &[make_row_map("eve", 50.0)]).unwrap();
        assert_eq!(new_deltas.len(), 1);
        assert_eq!(
            new_deltas[0].values.get("user"),
            Some(&Value::String("eve".into()))
        );

        // Verify final state
        let all_rows = engine.get_rows(1000, 1010).unwrap();
        assert_eq!(all_rows.len(), 3);
        assert_eq!(all_rows[2].0, 1002);
        assert_eq!(
            all_rows[2].1[0].get("user"),
            Some(&Value::String("eve".into()))
        );
    }
}
