// @ts-ignore
const { DeltaDb: NativeDeltaDb } = require('./delta-db.node')
const { Encoder, decode } = require('@msgpack/msgpack')

const encoder = new Encoder({ useBigInt64: true })

class DeltaDb {
  /** @type {InstanceType<typeof NativeDeltaDb>} */
  #native

  /** @param {InstanceType<typeof NativeDeltaDb>} native */
  constructor(native) {
    this.#native = native
  }

  /** Open a new DeltaDb instance. */
  static open(config) {
    return new DeltaDb(NativeDeltaDb.open(config))
  }

  /**
   * Process a batch of rows for a raw table.
   * Returns true if backpressure should be applied.
   */
  processBatch(table, block, rows) {
    return this.#native.processBatch(table, block, Buffer.from(encoder.encode(rows)))
  }

  /** Roll back all state after fork_point. */
  rollback(forkPoint) {
    this.#native.rollback(forkPoint)
  }

  /** Finalize all state up to and including the given block. */
  finalize(block) {
    this.#native.finalize(block)
  }

  /**
   * Atomic ingest: process all tables, store rollback chain, finalize, flush.
   * Returns the delta batch, or null if no records produced.
   */
  ingest(input) {
    const buf = this.#native.ingest({
      data: Buffer.from(encoder.encode(input.data)),
      rollbackChain: input.rollbackChain,
      finalizedHead: input.finalizedHead,
    })
    const batch = buf ? decode(buf) : null
    if (batch && input.onDelta) {
      input.onDelta(batch)
      this.#native.ack(batch.sequence)
    }
    return batch
  }

  /**
   * Find the common ancestor between our state and the Portal's chain.
   * Returns the matching block cursor, or null if no common ancestor found.
   */
  resolveForkCursor(previousBlocks) {
    return this.#native.resolveForkCursor(previousBlocks)
  }

  /** Flush buffered deltas into a batch. Returns null if no pending records. */
  flush() {
    const buf = this.#native.flush()
    return buf ? decode(buf) : null
  }

  /** Acknowledge a flushed batch by sequence number. */
  ack(sequence) {
    this.#native.ack(sequence)
  }

  /** Number of pending (unflushed) delta records. */
  get pendingCount() {
    return this.#native.pendingCount
  }

  /** Whether backpressure should be applied. */
  get isBackpressured() {
    return this.#native.isBackpressured
  }

  /** Current cursor: latest processed block + hash. Null if no blocks processed. */
  get cursor() {
    return this.#native.cursor
  }
}

module.exports.DeltaDb = DeltaDb
