use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::collections::HashMap;

use crate::db::{Config, DeltaDb as DeltaDbInner, IngestInput as IngestInputInner};
use crate::json_conv::{json_object_to_row, value_map_to_json};
use crate::types::{BlockCursor, DeltaOperation, RowMap, Value};

/// Configuration for opening a DeltaDb instance.
#[napi(object)]
pub struct DeltaDbConfig {
    /// SQL schema definition string.
    pub schema: String,
    /// Path to RocksDB data directory for persistence.
    /// When omitted, uses in-memory storage (data lost on restart).
    pub data_dir: Option<String>,
    /// Maximum buffer size before backpressure (default: 10000).
    pub max_buffer_size: Option<u32>,
}

/// Block cursor: number + hash.
#[napi(object)]
pub struct DeltaDbCursor {
    pub number: u32,
    pub hash: String,
}

/// A single delta record.
#[napi(object)]
pub struct DeltaRecord {
    pub table: String,
    pub operation: String,
    pub key: serde_json::Value,
    pub values: serde_json::Value,
    pub prev_values: Option<serde_json::Value>,
}

/// A batch of delta records.
#[napi(object)]
pub struct DeltaBatch {
    pub sequence: u32,
    pub finalized_head: Option<DeltaDbCursor>,
    pub latest_head: Option<DeltaDbCursor>,
    pub records: Vec<DeltaRecord>,
}

impl From<BlockCursor> for DeltaDbCursor {
    fn from(c: BlockCursor) -> Self {
        DeltaDbCursor {
            number: c.number as u32,
            hash: c.hash,
        }
    }
}

fn option_cursor_to_js(c: Option<BlockCursor>) -> Option<DeltaDbCursor> {
    c.map(|c| c.into())
}

fn batch_to_js(batch: crate::types::DeltaBatch) -> DeltaBatch {
    DeltaBatch {
        sequence: batch.sequence as u32,
        finalized_head: option_cursor_to_js(batch.finalized_head),
        latest_head: option_cursor_to_js(batch.latest_head),
        records: batch
            .records
            .into_iter()
            .map(|r| DeltaRecord {
                table: r.table,
                operation: match r.operation {
                    DeltaOperation::Insert => "insert".to_string(),
                    DeltaOperation::Update => "update".to_string(),
                    DeltaOperation::Delete => "delete".to_string(),
                },
                key: value_map_to_json(&r.key),
                values: value_map_to_json(&r.values),
                prev_values: r.prev_values.as_ref().map(value_map_to_json),
            })
            .collect(),
    }
}

/// Input for the atomic `ingest()` method.
#[napi(object)]
pub struct IngestInput {
    /// Table name → rows. Rows must contain `block_number`.
    pub data: HashMap<String, Vec<serde_json::Value>>,
    /// Unfinalized blocks with hashes for fork resolution.
    pub rollback_chain: Option<Vec<DeltaDbCursor>>,
    /// Finalized head cursor — both number and hash stored.
    pub finalized_head: DeltaDbCursor,
}

/// Delta DB N-API wrapper.
#[napi]
pub struct DeltaDb {
    inner: DeltaDbInner,
}

#[napi]
impl DeltaDb {
    /// Open a new DeltaDb instance.
    #[napi(factory)]
    pub fn open(config: DeltaDbConfig) -> Result<Self> {
        let mut cfg = if let Some(dir) = config.data_dir {
            Config::with_data_dir(config.schema, dir)
        } else {
            Config::new(config.schema)
        };
        if let Some(max) = config.max_buffer_size {
            cfg = cfg.max_buffer_size(max as usize);
        }

        let inner = DeltaDbInner::open(cfg).map_err(|e| {
            Error::new(Status::GenericFailure, format!("{e}"))
        })?;

        Ok(Self { inner })
    }

    /// Process a batch of rows for a raw table.
    /// `rows` is an array of JSON objects.
    /// Returns true if backpressure should be applied.
    #[napi]
    pub fn process_batch(
        &mut self,
        table: String,
        block: u32,
        rows: Vec<serde_json::Value>,
    ) -> Result<bool> {
        let rows: Vec<RowMap> = rows
            .into_iter()
            .map(|v| {
                json_object_to_row(&v)
                    .ok_or_else(|| Error::new(Status::InvalidArg, "row must be a JSON object"))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        self.inner
            .process_batch(&table, block as u64, rows)
            .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
    }

    /// Roll back all state after fork_point.
    #[napi]
    pub fn rollback(&mut self, fork_point: u32) -> Result<()> {
        self.inner
            .rollback(fork_point as u64)
            .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
    }

    /// Finalize all state up to and including the given block.
    #[napi]
    pub fn finalize(&mut self, block: u32) -> Result<()> {
        self.inner
            .finalize(block as u64)
            .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
    }

    /// Atomic ingest: process all tables, store rollback chain, finalize, flush.
    /// Returns the delta batch, or null if no records produced.
    #[napi]
    pub fn ingest(&mut self, input: IngestInput) -> Result<Option<DeltaBatch>> {
        // Convert JS rows to internal rows
        let mut data = std::collections::HashMap::new();
        for (table, js_rows) in input.data {
            let rows: Vec<RowMap> = js_rows
                .into_iter()
                .map(|v| {
                    json_object_to_row(&v).ok_or_else(|| {
                        Error::new(Status::InvalidArg, "row must be a JSON object")
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            data.insert(table, rows);
        }

        let rollback_chain = input
            .rollback_chain
            .unwrap_or_default()
            .into_iter()
            .map(|c| BlockCursor {
                number: c.number as u64,
                hash: c.hash,
            })
            .collect();

        let ingest_input = IngestInputInner {
            data,
            rollback_chain,
            finalized_head: BlockCursor {
                number: input.finalized_head.number as u64,
                hash: input.finalized_head.hash,
            },
        };

        let batch = self
            .inner
            .ingest(ingest_input)
            .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;

        Ok(batch.map(|b| batch_to_js(b)))
    }

    /// Find the common ancestor between our state and the Portal's chain.
    /// Returns the matching block cursor, or null if no common ancestor found.
    #[napi]
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: Vec<DeltaDbCursor>,
    ) -> Option<DeltaDbCursor> {
        let blocks: Vec<(u64, String)> = previous_blocks
            .into_iter()
            .map(|c| (c.number as u64, c.hash))
            .collect();
        let refs: Vec<(u64, &str)> = blocks.iter().map(|(n, h)| (*n, h.as_str())).collect();
        option_cursor_to_js(self.inner.resolve_fork_cursor(&refs))
    }

    /// Flush buffered deltas into a batch. Returns null if no pending records.
    #[napi]
    pub fn flush(&mut self) -> Option<DeltaBatch> {
        self.inner.flush().map(batch_to_js)
    }

    /// Acknowledge a flushed batch by sequence number.
    #[napi]
    pub fn ack(&mut self, sequence: u32) {
        self.inner.ack(sequence as u64);
    }

    /// Number of pending (unflushed) delta records.
    #[napi(getter)]
    pub fn pending_count(&self) -> u32 {
        self.inner.pending_count() as u32
    }

    /// Whether backpressure should be applied.
    #[napi(getter)]
    pub fn is_backpressured(&self) -> bool {
        self.inner.is_backpressured()
    }

    /// Current cursor: latest processed block + hash. Null if no blocks processed.
    #[napi(getter)]
    pub fn cursor(&self) -> Option<DeltaDbCursor> {
        option_cursor_to_js(self.inner.latest_cursor())
    }
}
