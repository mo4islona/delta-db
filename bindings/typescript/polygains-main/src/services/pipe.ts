import type { BlockCursor } from "@subsquid/pipes";
import {
	BPS_SCALE,
	FIFTEEN_MINUTES,
	MIN_PRICE_BPS,
	START_BLOCK,
	VOLUME_THRESHOLD,
} from "@/lib/const";
import { loadDetector } from "@/lib/db/bloomfilter";
import { hashWallet } from "@/lib/hash";
import { type ParsedOrder, type PositionStats, SIDE } from "@/lib/types";
import { toBigInt, toTokenId, toUsdVolume } from "@/lib/utils";
import { type TraderData, WindowBuffer } from "./buffer";
import { InsiderDetector, NotInsiderDetector, XXHash32Set } from "./detector";
import { BloomFilterPersistor } from "./filter-persistor";
import {
	AccountAddressMapPersistor,
	InsiderPositionsPersistor,
	MarketStatsPersistor,
} from "./positions-persistor";

export { type TraderData, WindowBuffer } from "./buffer";

export class PolymarketPipe {
	private cursor?: BlockCursor;
	private stateFile = Bun.file("state.json");
	private insiderDetector!: InsiderDetector;
	private notInsiderDetector!: NotInsiderDetector;
	private aggPositions = new WindowBuffer<TraderData>(
		FIFTEEN_MINUTES,
		(item) => item.userStats.firstSeen,
		(item) => item.id,
	);
	private persistor!: BloomFilterPersistor;
	private insiderPositionsPersistor = new InsiderPositionsPersistor();
	private marketStatsPersistor = new MarketStatsPersistor();
	private accountAddressMapPersistor = new AccountAddressMapPersistor();
	private insiderCount = 0;
	private notInsiderCount = 0;
	private initialized = false;

	/**
	 * Initialize or recover from database snapshots
	 */
	async initialize(): Promise<void> {
		if (this.initialized) {
			return;
		}

		try {
			const [insiderSnapshot, notInsiderSnapshot] = await Promise.all([
				loadDetector("insider"),
				loadDetector("notinsider"),
			]);

			if (insiderSnapshot && notInsiderSnapshot) {
				console.log(
					"[PolymarketPipe] 🔄 Recovering from detector snapshots...",
				);

				const insiderSet = new XXHash32Set();
				insiderSet.restoreSet(insiderSnapshot.dataSet);

				const notInsiderSet = new XXHash32Set();
				notInsiderSet.restoreSet(notInsiderSnapshot.dataSet);

				this.insiderDetector = new InsiderDetector();
				this.insiderDetector.getDetector().restoreSet(insiderSnapshot.dataSet);

				this.notInsiderDetector = new NotInsiderDetector();
				this.notInsiderDetector
					.getDetector()
					.restoreSet(notInsiderSnapshot.dataSet);

				this.insiderCount = insiderSnapshot.itemCount;
				this.notInsiderCount = notInsiderSnapshot.itemCount;

				if (!this.cursor) {
					const snapshotCursor =
						insiderSnapshot.cursor || notInsiderSnapshot.cursor;
					if (snapshotCursor) {
						this.cursor = {
							...snapshotCursor,
							number: Math.max(snapshotCursor.number, START_BLOCK),
						};
						console.log(
							`[PolymarketPipe] 📍 Restored cursor from snapshot: block ${this.cursor.number}`,
						);
					} else {
						await this.loadCursorFromState();
					}
				}

				console.log(
					`[PolymarketPipe] ✅ Recovered state: ${this.insiderCount} insiders, ${this.notInsiderCount} non-insiders`,
				);
			} else {
				console.log("[PolymarketPipe] 🆕 Starting fresh (no snapshots found)");
				this.insiderDetector = new InsiderDetector();
				this.notInsiderDetector = new NotInsiderDetector();

				if (!this.cursor) {
					await this.loadCursorFromState();
				}
			}
		} catch (error) {
			console.error(
				"[PolymarketPipe] ⚠️  Recovery failed, starting fresh:",
				error,
			);
			this.insiderDetector = new InsiderDetector();
			this.notInsiderDetector = new NotInsiderDetector();
		}

		this.persistor = new BloomFilterPersistor(30, async (cursor) => {
			await this.saveCursor(cursor);
		});

		this.initialized = true;
	}

	/**
	 * @param input.logger - Standard Subsquid logger
	 * @param input.read - Async generator providing batches of data
	 */
	async write({
		logger,
		read,
	}: {
		logger: { error: (error: unknown, message?: string) => void };
		read: (cursor?: BlockCursor) => AsyncIterable<unknown>;
	}) {
		await this.initialize();

		const currentCursor = await this.getCursor();
		const stream = read(currentCursor);
		let latestTimestamp = 0;

		try {
			for await (const rawBatch of stream) {
				const batch = rawBatch as {
					ctx?: { state?: { current?: BlockCursor & { timestamp: number } } };
					header?: { timestamp: number };
					data?: ParsedOrder[];
				};
				try {
					latestTimestamp =
						batch?.ctx?.state?.current?.timestamp ??
						batch?.header?.timestamp ??
						latestTimestamp;

					if (
						(!latestTimestamp || Number.isNaN(latestTimestamp)) &&
						batch?.data?.length > 0
					) {
						latestTimestamp = Number(batch.data[0].timestamp || 0);
					}

					if (!latestTimestamp || Number.isNaN(latestTimestamp)) {
						console.warn(
							"[PolymarketPipe] ⚠️ No timestamp found in batch ctx or header",
						);
					}

					this.flushExpiredTraders(latestTimestamp);

					for (const rawOrder of (batch?.data ?? []) as ParsedOrder[]) {
						const { trader, assetId, side, timestamp } = rawOrder;
						const orderTimestamp = Number(timestamp);
						if (
							Number.isFinite(orderTimestamp) &&
							orderTimestamp > latestTimestamp
						) {
							latestTimestamp = orderTimestamp;
						}

						const usdc = toBigInt(rawOrder.usdc);
						const shares = toBigInt(rawOrder.shares);
						if (shares <= 0n) continue;

						const accountHash = hashWallet(trader);
						const accountKey = String(accountHash);
						const tokenId = toTokenId(assetId);
						const price = Number(usdc) / Number(shares);
						const volume = toUsdVolume(usdc);

						if (!Number.isFinite(price) || !Number.isFinite(volume)) {
							continue;
						}

						// Market stats should represent every matched fill price, regardless
						// of insider-specific filters (side/min-price).
						this.marketStatsPersistor.enqueue({
							accountHash,
							tokenId,
							detectedAt: orderTimestamp,
							firstSeen: orderTimestamp,
							lastSeen: orderTimestamp,
							volume,
							trades: 1,
							sumPrice: price,
							sumPriceSq: price * price,
						});

						// Insider detection remains buy-side and low-priced outcome focused.
						if (side !== SIDE.BUY) continue;
						if (usdc * BPS_SCALE >= shares * MIN_PRICE_BPS) continue;

						if (this.insiderDetector.has(accountHash)) {
							// Already detected insider -> append directly to DB.
							this.accountAddressMapPersistor.enqueue({
								accountHash,
								walletAddress: trader,
								seenAt: orderTimestamp,
							});
							this.insiderPositionsPersistor.enqueue({
								accountHash,
								detectedAt: orderTimestamp,
								positions: {
									[tokenId]: {
										firstSeen: orderTimestamp,
										lastSeen: orderTimestamp,
										volume,
										trades: 1,
										sumPrice: price,
										sumPriceSq: price * price,
									},
								},
							});
							continue;
						}

						if (this.notInsiderDetector.has(accountHash)) {
							continue;
						}

						let user = this.aggPositions.get(accountKey);
						if (!user) {
							user = {
								id: accountKey,
								wallet: trader,
								tokenstats: {},
								userStats: {
									tradeVol: 0n,
									tradeCount: 0,
									firstSeen: orderTimestamp,
									lastSeen: orderTimestamp,
								},
							};
							this.aggPositions.set(accountKey, user);
						} else if (!user.wallet) {
							user.wallet = trader;
						}

						if (orderTimestamp - user.userStats.firstSeen > FIFTEEN_MINUTES) {
							continue;
						}

						user.userStats.tradeVol += usdc;
						user.userStats.tradeCount += 1;
						user.userStats.lastSeen = Math.max(
							user.userStats.lastSeen ?? orderTimestamp,
							orderTimestamp,
						);

						const existingToken = user.tokenstats[tokenId] as
							| PositionStats
							| undefined;
						const tokenStats: PositionStats = existingToken ?? {
							firstSeen: orderTimestamp,
							lastSeen: orderTimestamp,
							volume: 0,
							trades: 0,
							sumPrice: 0,
							sumPriceSq: 0,
						};

						tokenStats.firstSeen = Math.min(
							tokenStats.firstSeen,
							orderTimestamp,
						);
						tokenStats.lastSeen = Math.max(tokenStats.lastSeen, orderTimestamp);
						tokenStats.volume += volume;
						tokenStats.trades += 1;
						tokenStats.sumPrice += price;
						tokenStats.sumPriceSq += price * price;
						user.tokenstats[tokenId] = tokenStats;

						if (
							latestTimestamp - user.userStats.firstSeen <= FIFTEEN_MINUTES &&
							user.userStats.tradeVol >= VOLUME_THRESHOLD
						) {
							this.markInsider(
								accountHash,
								user,
								user.userStats.lastSeen ?? latestTimestamp,
							);
						}
					}

					this.flushExpiredTraders(latestTimestamp);
				} catch (batchErr) {
					console.error("[PolymarketPipe] Batch processing error:", batchErr);
					logger.error(batchErr, "Batch processing error, continuing...");
				}

				const cursor = batch?.ctx?.state?.current;
				if (cursor) {
					this.persistor.onBatchProcessed({
						insiderDetector: this.insiderDetector.getDetector(),
						notInsiderDetector: this.notInsiderDetector.getDetector(),
						insiderCount: this.insiderCount,
						notInsiderCount: this.notInsiderCount,
						cursor,
					});
				}

				this.marketStatsPersistor.onBatchProcessed();
			}
		} catch (err) {
			logger.error(err, "Pipeline write failed");
			console.error("[PolymarketPipe] Pipeline error (non-fatal):", err);
		} finally {
			await Promise.all([
				this.insiderPositionsPersistor.flush(),
				this.marketStatsPersistor.flush(),
				this.accountAddressMapPersistor.flush(),
			]);
		}
	}

	async fork(previousBlocks: BlockCursor[]): Promise<BlockCursor | null> {
		console.warn(
			`Chain reorg: Removing data for ${previousBlocks.length} blocks`,
		);
		return null;
	}

	private flushExpiredTraders(currentTimestamp: number) {
		if (!currentTimestamp || Number.isNaN(currentTimestamp)) return;

		const flushedData = this.aggPositions.flush(currentTimestamp);
		for (const [accountHashRaw, stats] of Object.entries(flushedData)) {
			const parsedAccountHash = Number.parseInt(accountHashRaw, 10);
			if (!Number.isFinite(parsedAccountHash)) {
				continue;
			}
			const accountHash = parsedAccountHash | 0;

			if (
				this.insiderDetector.has(accountHash) ||
				this.notInsiderDetector.has(accountHash)
			) {
				continue;
			}

			if (stats.userStats.tradeVol >= VOLUME_THRESHOLD) {
				this.markInsider(
					accountHash,
					stats,
					stats.userStats.lastSeen ?? currentTimestamp,
				);
				continue;
			}

			if (stats.wallet) {
				this.accountAddressMapPersistor.enqueue({
					accountHash,
					walletAddress: stats.wallet,
					seenAt: stats.userStats.lastSeen ?? currentTimestamp,
				});
			}

			this.notInsiderDetector.add(accountHash);
			this.notInsiderCount++;
		}
	}

	private markInsider(
		accountHash: number,
		stats: TraderData,
		detectedAt: number,
	) {
		const accountKey = String(accountHash);
		if (this.insiderDetector.has(accountHash)) {
			this.aggPositions.delete(accountKey);
			return;
		}

		this.insiderDetector.add(accountHash);
		this.insiderCount++;
		if (stats.wallet) {
			this.accountAddressMapPersistor.enqueue({
				accountHash,
				walletAddress: stats.wallet,
				seenAt: detectedAt,
			});
		}
		this.enqueueInsiderPositions(accountHash, stats, detectedAt);
		this.aggPositions.delete(accountKey);

		const walletAddress = stats.wallet?.toLowerCase() ?? "unknown";
		console.log(
			`[ALERT] Insider detected: hash=${accountHash} u32=${accountHash >>> 0} address=${walletAddress} | Vol: ${stats.userStats.tradeVol.toString()}`,
		);
	}

	private enqueueInsiderPositions(
		accountHash: number,
		stats: TraderData,
		detectedAt: number,
	) {
		const positions: Record<string, PositionStats> = {};
		for (const [tokenId, tokenStats] of Object.entries(stats.tokenstats)) {
			const typed = tokenStats as PositionStats;
			if (!typed || typed.trades <= 0) continue;
			positions[tokenId] = {
				firstSeen: typed.firstSeen,
				lastSeen: typed.lastSeen,
				volume: typed.volume,
				trades: typed.trades,
				sumPrice: typed.sumPrice,
				sumPriceSq: typed.sumPriceSq,
			};
		}

		this.insiderPositionsPersistor.enqueue({
			accountHash,
			detectedAt,
			positions,
		});
	}

	private async loadCursorFromState(): Promise<void> {
		if (await this.stateFile.exists()) {
			try {
				const content = await this.stateFile.text();
				try {
					const cursor = JSON.parse(content);
					if (typeof cursor === "number") {
						this.cursor = { number: cursor };
					} else if (
						cursor &&
						typeof cursor === "object" &&
						typeof cursor.number === "number"
					) {
						this.cursor = cursor as BlockCursor;
					}
				} catch {
					const num = Number.parseInt(content.trim(), 10);
					if (!Number.isNaN(num)) {
						this.cursor = { number: num };
					}
				}
				if (this.cursor) {
					if (this.cursor.number < START_BLOCK) {
						this.cursor = { ...this.cursor, number: START_BLOCK };
					}
					console.log(
						`[PolymarketPipe] 📍 Loaded cursor from state.json: block ${this.cursor.number}`,
					);
				}
			} catch (error) {
				console.warn("Failed to read cursor from state.json:", error);
			}
		}
	}

	private async getCursor(): Promise<BlockCursor | undefined> {
		if (!this.cursor) {
			await this.loadCursorFromState();
		}
		return this.cursor;
	}

	private async saveCursor(cursor: BlockCursor) {
		this.cursor = cursor;
		await Bun.write(this.stateFile, JSON.stringify(cursor));
	}
}
