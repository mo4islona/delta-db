import { decode, Encoder } from '@msgpack/msgpack'
import type { ReducerCtx } from './ddl'
import { DeltaDb as NativeDeltaDb } from './native/native.js'

const encoder = new Encoder({ useBigInt64: true })

// ─── Types ───────────────────────────────────────────────────────

export interface DeltaDbConfig {
  schema: string
  dataDir?: string
  maxBufferSize?: number
  /** Compression algorithm for RocksDB: "none", "snappy" (default), "zstd", "lz4". */
  compression?: 'none' | 'snappy' | 'zstd' | 'lz4'
  /** Disable RocksDB automatic background compactions. */
  disableCompaction?: boolean
  /** Block cache size in bytes. Omit for RocksDB default (~8MB per CF), 0 to disable. */
  cacheSize?: number
}

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

export type PerfNodeKind = 'pipeline' | 'raw_table' | 'reducer' | 'mv' | 'parallel'

export interface PerfNode {
  kind: PerfNodeKind
  name: string
  durationMs: number
  children: PerfNode[]
}

export interface DeltaBatch {
  sequence: number
  finalizedHead: DeltaDbCursor | null
  latestHead: DeltaDbCursor | null
  tables: Record<string, DeltaRecord[]>
  perf: PerfNode[]
}

export interface IngestInput {
  data: Record<string, Record<string, any>[]>
  rollbackChain?: DeltaDbCursor[]
  finalizedHead: DeltaDbCursor
  onDelta?: (batch: DeltaBatch) => void | Promise<void>
}

export interface StateFieldDef {
  name: string
  columnType: string
  defaultValue: string
}

export interface ExternalReducerOptions<TState = any, TRow = any, TEmit = any> {
  name: string
  source: string
  groupBy: string[]
  state: StateFieldDef[]
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void
}

export type { ReducerCtx } from './ddl'

// ─── DeltaDb class ───────────────────────────────────────────────

export class DeltaDb {
  #native: InstanceType<typeof NativeDeltaDb>

  private constructor(native: InstanceType<typeof NativeDeltaDb>) {
    this.#native = native
  }

  static open(config: DeltaDbConfig): DeltaDb {
    return new DeltaDb(NativeDeltaDb.open(config))
  }

  async ingest(input: IngestInput): Promise<DeltaBatch | null> {
    const buf = this.#native.ingest({
      data: Buffer.from(encoder.encode(input.data)),
      rollbackChain: input.rollbackChain,
      finalizedHead: input.finalizedHead,
    })
    const batch = buf ? (decode(buf) as DeltaBatch) : null
    if (batch && input.onDelta) {
      await input.onDelta(batch)
      this.#native.ack(batch.sequence)
    }
    return batch
  }

  resolveForkCursor(previousBlocks: DeltaDbCursor[]): DeltaDbCursor | null {
    return this.#native.resolveForkCursor(previousBlocks)
  }

  flush(): DeltaBatch | null {
    const buf = this.#native.flush()
    return buf ? (decode(buf) as DeltaBatch) : null
  }

  ack(sequence: number): void {
    this.#native.ack(sequence)
  }

  get pendingCount(): number {
    return this.#native.pendingCount
  }

  get isBackpressured(): boolean {
    return this.#native.isBackpressured
  }

  get cursor(): DeltaDbCursor | null {
    return this.#native.cursor
  }

  registerReducer<TState = any, TRow = any, TEmit = any>(
    options: ExternalReducerOptions<TState, TRow, TEmit>,
  ): void {
    const { reduce } = options

    const batchFn = (groups: { state: TState; rows: TRow[] }[]) => {
      return groups.map(({ state, rows }) => {
        let s = state
        const emits: any[] = []
        const ctx = Object.create(null)
        ctx.update = (newState: TState) => {
          s = newState
          for (const k of Object.keys(newState as any)) {
            ctx[k] = (newState as any)[k]
          }
        }
        ctx.emit = (row: TEmit) => {
          if (row != null) emits.push(row)
        }
        // Initialize readable state properties
        for (const k of Object.keys(state as any)) {
          ctx[k] = (state as any)[k]
        }
        for (const row of rows) {
          reduce(ctx, row)
        }
        return { state: s, emits }
      })
    }

    this.#native.registerReducer(
      {
        name: options.name,
        source: options.source,
        groupBy: options.groupBy,
        state: options.state,
      },
      batchFn,
    )
  }
}
