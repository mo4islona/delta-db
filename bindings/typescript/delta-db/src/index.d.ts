/** Configuration for opening a DeltaDb instance. */
export interface DeltaDbConfig {
  /** SQL schema definition string. */
  schema: string
  /**
   * Path to RocksDB data directory for persistence.
   * When omitted, uses in-memory storage (data lost on restart).
   */
  dataDir?: string
  /** Maximum buffer size before backpressure (default: 10000). */
  maxBufferSize?: number
}

/** Block cursor: number + hash. */
export interface DeltaDbCursor {
  number: number
  hash: string
}

export type DeltaOperation = 'insert' | 'update' | 'delete'

export interface DeltaRecord {
  table: string
  operation: DeltaOperation
  key: Record<string, any>
  values: Record<string, any>
  prevValues: Record<string, any> | null
}

export interface DeltaBatch {
  sequence: number
  finalizedHead: DeltaDbCursor | null
  latestHead: DeltaDbCursor | null
  /** Records grouped by table name. */
  tables: Record<string, DeltaRecord[]>
}

/** Input for the atomic `ingest()` method. */
export interface IngestInput {
  /** Table name → rows: `{tableName: [{col: val}, ...], ...}`. */
  data: Record<string, Record<string, any>[]>
  /** Unfinalized blocks with hashes for fork resolution. */
  rollbackChain?: DeltaDbCursor[]
  /** Finalized head cursor — both number and hash stored. */
  finalizedHead: DeltaDbCursor
  /** Called with each delta batch. When provided, batch is auto-acked. */
  onDelta?: (batch: DeltaBatch) => void
}

/** Delta DB wrapper. */
export declare class DeltaDb {
  /** Open a new DeltaDb instance. */
  static open(config: DeltaDbConfig): DeltaDb
  /**
   * Process a batch of rows for a raw table.
   * Returns true if backpressure should be applied.
   */
  processBatch(table: string, block: number, rows: Record<string, any>[]): boolean
  /** Roll back all state after fork_point. */
  rollback(forkPoint: number): void
  /** Finalize all state up to and including the given block. */
  finalize(block: number): void
  /**
   * Atomic ingest: process all tables, store rollback chain, finalize, flush.
   * Returns the delta batch, or null if no records produced.
   * When `onDelta` is provided in input, it is called and batch is auto-acked.
   */
  ingest(input: IngestInput): DeltaBatch | null
  /**
   * Find the common ancestor between our state and the Portal's chain.
   * Returns the matching block cursor, or null if no common ancestor found.
   */
  resolveForkCursor(previousBlocks: DeltaDbCursor[]): DeltaDbCursor | null
  /** Flush buffered deltas into a batch. Returns null if no pending records. */
  flush(): DeltaBatch | null
  /** Acknowledge a flushed batch by sequence number. */
  ack(sequence: number): void
  /** Number of pending (unflushed) delta records. */
  get pendingCount(): number
  /** Whether backpressure should be applied. */
  get isBackpressured(): boolean
  /** Current cursor: latest processed block + hash. Null if no blocks processed. */
  get cursor(): DeltaDbCursor | null
}
