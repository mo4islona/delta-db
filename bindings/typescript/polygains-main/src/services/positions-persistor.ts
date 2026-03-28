import {
	type AccountWalletMappingIncrement,
	type InsiderPositionIncrement,
	updateTokenStats,
	upsertAccountWalletMappings,
	upsertInsiderPositions,
} from "@/lib/db/positions";
import type {
	AccountAddressTask,
	AggregatedPosition,
	PersistTask,
} from "@/lib/types";
import { normalizeInt32 } from "@/lib/utils";

/**
 * Sequential queue that persists insider positions in the background.
 * Multiple pending tasks are merged before writing to keep DB writes compact.
 */
export class InsiderPositionsPersistor {
	private queue: PersistTask[] = [];
	private isProcessing = false;

	enqueue(task: PersistTask) {
		const accountHash = normalizeInt32(task.accountHash);
		if (accountHash === null) return;
		if (Object.keys(task.positions).length === 0) return;
		this.queue.push({ ...task, accountHash });
		if (!this.isProcessing) {
			this.processQueue();
		}
	}

	private async processQueue() {
		if (this.isProcessing) return;
		this.isProcessing = true;

		while (this.queue.length > 0) {
			const batch = this.queue.splice(0, this.queue.length);
			try {
				const merged = this.mergeTasks(batch);
				await upsertInsiderPositions(merged);
			} catch (error) {
				console.error(
					"[InsiderPositionsPersistor] Failed to persist batch:",
					error,
				);
			}
		}

		this.isProcessing = false;
	}

	private mergeTasks(tasks: PersistTask[]): InsiderPositionIncrement[] {
		const merged = new Map<string, AggregatedPosition>();

		for (const task of tasks) {
			for (const [tokenId, stats] of Object.entries(task.positions)) {
				if (stats.trades <= 0) continue;
				const key = `${task.accountHash}-${tokenId}`;
				const existing = merged.get(key);

				if (!existing) {
					merged.set(key, {
						accountHash: task.accountHash,
						tokenId,
						detectedAt: task.detectedAt,
						firstSeen: stats.firstSeen,
						lastSeen: stats.lastSeen,
						volume: stats.volume,
						trades: stats.trades,
						sumPrice: stats.sumPrice,
						sumPriceSq: stats.sumPriceSq,
					});
					continue;
				}

				existing.volume += stats.volume;
				existing.trades += stats.trades;
				existing.sumPrice += stats.sumPrice;
				existing.sumPriceSq += stats.sumPriceSq;
				existing.firstSeen = Math.min(existing.firstSeen, stats.firstSeen);
				existing.lastSeen = Math.max(existing.lastSeen, stats.lastSeen);
				existing.detectedAt = Math.min(existing.detectedAt, task.detectedAt);
			}
		}

		return Array.from(merged.values());
	}

	async flush() {
		while (this.isProcessing || this.queue.length > 0) {
			await new Promise((resolve) => setTimeout(resolve, 50));
		}
	}
}

export class MarketStatsPersistor {
	private readonly flushIntervalBatches: number;
	private readonly maxPendingTokens: number;
	private pendingByToken = new Map<string, InsiderPositionIncrement>();
	private isProcessing = false;
	private batchCount = 0;
	private lastFlushBatch = 0;

	constructor(flushIntervalBatches = 30, maxPendingTokens = 5000) {
		this.flushIntervalBatches = flushIntervalBatches;
		this.maxPendingTokens = maxPendingTokens;
	}

	enqueue(item: InsiderPositionIncrement) {
		if (item.trades <= 0) return;

		const existing = this.pendingByToken.get(item.tokenId);
		if (!existing) {
			this.pendingByToken.set(item.tokenId, { ...item });
		} else {
			existing.volume += item.volume;
			existing.trades += item.trades;
			existing.sumPrice += item.sumPrice;
			existing.sumPriceSq += item.sumPriceSq;
			existing.firstSeen = Math.min(existing.firstSeen, item.firstSeen);
			existing.lastSeen = Math.max(existing.lastSeen, item.lastSeen);
			existing.detectedAt = Math.min(existing.detectedAt, item.detectedAt);
		}

		if (this.pendingByToken.size >= this.maxPendingTokens) {
			this.scheduleFlush();
		}
	}

	enqueueMany(items: InsiderPositionIncrement[]) {
		for (const item of items) {
			this.enqueue(item);
		}
	}

	onBatchProcessed(): boolean {
		this.batchCount += 1;
		if (this.batchCount - this.lastFlushBatch < this.flushIntervalBatches) {
			return false;
		}

		this.lastFlushBatch = this.batchCount;
		this.scheduleFlush();
		return true;
	}

	private scheduleFlush() {
		if (this.isProcessing || this.pendingByToken.size === 0) return;
		void this.processQueue();
	}

	private async processQueue() {
		if (this.isProcessing) return;
		this.isProcessing = true;

		while (this.pendingByToken.size > 0) {
			const batch = Array.from(this.pendingByToken.values());
			this.pendingByToken.clear();

			const chunkSize = 1000;
			for (let i = 0; i < batch.length; i += chunkSize) {
				const chunk = batch.slice(i, i + chunkSize);
				if (chunk.length === 0) continue;
				try {
					await updateTokenStats(chunk);
				} catch (error) {
					console.error(
						"[MarketStatsPersistor] Failed to persist batch:",
						error,
					);
				}
			}
		}

		this.isProcessing = false;

		// Handle items enqueued while previous flush was in progress.
		if (this.pendingByToken.size > 0) {
			try {
				await this.processQueue();
			} catch {
				// no-op; errors are logged in processQueue
			}
		}
	}

	async flush() {
		this.scheduleFlush();
		while (this.isProcessing || this.pendingByToken.size > 0) {
			await new Promise((resolve) => setTimeout(resolve, 50));
		}
	}
}

export class AccountAddressMapPersistor {
	private pendingByHash = new Map<number, AccountWalletMappingIncrement>();
	private isProcessing = false;

	enqueue(task: AccountAddressTask) {
		const accountHash = normalizeInt32(task.accountHash);
		const walletAddress = task.walletAddress.trim().toLowerCase();
		if (accountHash === null || !walletAddress) return;

		const existing = this.pendingByHash.get(accountHash);
		if (!existing) {
			this.pendingByHash.set(accountHash, {
				accountHash,
				walletAddress,
				firstSeen: task.seenAt,
				lastSeen: task.seenAt,
			});
		} else {
			existing.walletAddress = walletAddress;
			existing.firstSeen = Math.min(existing.firstSeen, task.seenAt);
			existing.lastSeen = Math.max(existing.lastSeen, task.seenAt);
		}

		this.scheduleFlush();
	}

	private scheduleFlush() {
		if (this.isProcessing || this.pendingByHash.size === 0) return;
		void this.processQueue();
	}

	private async processQueue() {
		if (this.isProcessing) return;
		this.isProcessing = true;

		while (this.pendingByHash.size > 0) {
			const batch = Array.from(this.pendingByHash.values());
			this.pendingByHash.clear();
			try {
				await upsertAccountWalletMappings(batch);
			} catch (error) {
				console.error(
					"[AccountAddressMapPersistor] Failed to persist batch:",
					error,
				);
			}
		}

		this.isProcessing = false;
		if (this.pendingByHash.size > 0) {
			this.scheduleFlush();
		}
	}

	async flush() {
		this.scheduleFlush();
		while (this.isProcessing || this.pendingByHash.size > 0) {
			await new Promise((resolve) => setTimeout(resolve, 50));
		}
	}
}
