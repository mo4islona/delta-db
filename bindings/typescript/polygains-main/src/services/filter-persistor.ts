import type { BlockCursor } from "@subsquid/pipes";
import { saveDetector } from "@/lib/db/bloomfilter";
import type { PersistorTask } from "@/lib/types";

/**
 * Sequential queue for persisting XXHash32Set detector snapshots
 * Ensures only one save operation happens at a time
 */
export class BloomFilterPersistor {
	private queue: PersistorTask[] = [];
	private isProcessing = false;
	private batchCount = 0;
	private lastSaveBatch = 0;
	private readonly SAVE_INTERVAL_BATCHES: number;
	private onSavedCallback?: (cursor: BlockCursor) => Promise<void>;

	constructor(
		saveIntervalBatches = 30,
		onSaved?: (cursor: BlockCursor) => Promise<void>,
	) {
		// Default: save every 30 batches
		this.SAVE_INTERVAL_BATCHES = saveIntervalBatches;
		this.onSavedCallback = onSaved;
	}

	/**
	 * Increment batch counter and enqueue snapshot if threshold reached
	 */
	onBatchProcessed(task: PersistorTask): boolean {
		this.batchCount++;

		if (this.batchCount - this.lastSaveBatch < this.SAVE_INTERVAL_BATCHES) {
			return false; // Not enough batches yet
		}

		this.lastSaveBatch = this.batchCount;
		this.queue.push(task);

		// Start processing if not already running
		if (!this.isProcessing) {
			this.processQueue();
		}

		return true;
	}

	/**
	 * Force save immediately, bypassing batch interval check
	 */
	forceSave(task: PersistorTask) {
		this.lastSaveBatch = this.batchCount;
		this.queue.push(task);

		if (!this.isProcessing) {
			this.processQueue();
		}
	}

	/**
	 * Process queue sequentially (one at a time)
	 */
	private async processQueue() {
		if (this.isProcessing) {
			return;
		}

		this.isProcessing = true;

		while (this.queue.length > 0) {
			// Only process the LATEST snapshot, discard older ones
			const task = this.queue.pop();
			if (!task) break;

			this.queue = []; // Clear queue - we only need the latest snapshot

			try {
				await this.persistSnapshot(task);
			} catch (error) {
				console.error("[Persistor] Failed to save snapshot:", error);
				// Continue processing despite errors
			}
		}

		this.isProcessing = false;
	}

	/**
	 * Save detector snapshots to database (incremental - only unsaved hashes)
	 */
	private async persistSnapshot(task: PersistorTask) {
		const startTime = Date.now();

		try {
			// Save both detectors incrementally (only unsaved) in parallel
			await Promise.all([
				saveDetector(
					"insider",
					task.insiderDetector,
					task.insiderCount,
					task.cursor,
				),
				saveDetector(
					"notinsider",
					task.notInsiderDetector,
					task.notInsiderCount,
					task.cursor,
				),
			]);

			// CRITICAL: clearUnsaved() is called inside saveDetector() AFTER DB write
			// This ensures we don't lose data if save fails

			// Only save cursor AFTER detectors are successfully written
			// This ensures cursor and detector snapshots stay in sync for safe recovery
			if (this.onSavedCallback) {
				await this.onSavedCallback(task.cursor);
			}

			const duration = Date.now() - startTime;
			console.log(
				`[Persistor] ✅ Saved incremental snapshots (insider: ${task.insiderCount}, notinsider: ${task.notInsiderCount}) at block ${task.cursor.number} in ${duration}ms`,
			);
		} catch (error) {
			console.error("[Persistor] ❌ Snapshot save failed:", error);
			throw error;
		}
	}

	/**
	 * Wait for all pending tasks to complete
	 */
	async flush() {
		while (this.isProcessing || this.queue.length > 0) {
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
	}

	/**
	 * Get queue status
	 */
	getStatus() {
		return {
			queueLength: this.queue.length,
			isProcessing: this.isProcessing,
			batchCount: this.batchCount,
			lastSaveBatch: this.lastSaveBatch,
		};
	}
}
