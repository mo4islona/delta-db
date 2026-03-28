import { sql } from "drizzle-orm";
import { db } from "@/lib/db/init";
import {
	accountWalletMap,
	insiderPositions,
	tokenStats,
} from "@/lib/db/schema";

export interface InsiderPositionIncrement {
	accountHash: number;
	tokenId: string;
	detectedAt: number;
	firstSeen: number;
	lastSeen: number;
	volume: number;
	trades: number;
	sumPrice: number;
	sumPriceSq: number;
}

export interface AccountWalletMappingIncrement {
	accountHash: number;
	walletAddress: string;
	firstSeen: number;
	lastSeen: number;
}

const toPositionId = (accountHash: number, tokenId: string) =>
	`${accountHash}-${tokenId}`;

type TokenAggregate = {
	token: string;
	totalTrades: number;
	totalVol: number;
	sumPrice: number;
	sumPriceSq: number;
	lastPrice: number;
};

const aggregateTokenStats = (
	items: InsiderPositionIncrement[],
): TokenAggregate[] => {
	const perToken = new Map<string, TokenAggregate>();

	for (const item of items) {
		const existing = perToken.get(item.tokenId) ?? {
			token: item.tokenId,
			totalTrades: 0,
			totalVol: 0,
			sumPrice: 0,
			sumPriceSq: 0,
			lastPrice: 0,
		};

		existing.totalTrades += item.trades;
		existing.totalVol += item.volume;
		existing.sumPrice += item.sumPrice;
		existing.sumPriceSq += item.sumPriceSq;
		existing.lastPrice = item.trades > 0 ? item.sumPrice / item.trades : 0;

		perToken.set(item.tokenId, existing);
	}

	return Array.from(perToken.values());
};

/**
 * Persist insider positions.
 * Uses ON CONFLICT updates so repeated detections increment counters safely.
 */
export async function upsertInsiderPositions(
	items: InsiderPositionIncrement[],
) {
	if (items.length === 0) return;

	await db
		.insert(insiderPositions)
		.values(
			items.map((item) => ({
				id: toPositionId(item.accountHash, item.tokenId),
				accountHash: item.accountHash,
				tokenId: item.tokenId,
				totalVolume: item.volume,
				tradeCount: item.trades,
				avgPrice: item.trades > 0 ? item.sumPrice / item.trades : 0,
				sumPrice: item.sumPrice,
				sumPriceSq: item.sumPriceSq,
				firstSeen: item.firstSeen,
				lastSeen: item.lastSeen,
				detectedAt: item.detectedAt,
			})),
		)
		.onConflictDoUpdate({
			target: insiderPositions.id,
			set: {
				totalVolume: sql`coalesce(${insiderPositions.totalVolume}, 0) + excluded.total_volume`,
				tradeCount: sql`coalesce(${insiderPositions.tradeCount}, 0) + excluded.trade_count`,
				sumPrice: sql`coalesce(${insiderPositions.sumPrice}, 0) + excluded.sum_price`,
				sumPriceSq: sql`coalesce(${insiderPositions.sumPriceSq}, 0) + excluded.sum_price_sq`,
				avgPrice: sql`
						CASE
							WHEN (coalesce(${insiderPositions.tradeCount}, 0) + excluded.trade_count) > 0 THEN
								(coalesce(${insiderPositions.sumPrice}, 0) + excluded.sum_price) /
								(coalesce(${insiderPositions.tradeCount}, 0) + excluded.trade_count)
							ELSE excluded.avg_price
						END
					`,
				firstSeen: sql`
						CASE
							WHEN ${insiderPositions.firstSeen} IS NULL THEN excluded.first_seen
							ELSE LEAST(${insiderPositions.firstSeen}, excluded.first_seen)
						END
					`,
				lastSeen: sql`
						CASE
							WHEN ${insiderPositions.lastSeen} IS NULL THEN excluded.last_seen
							ELSE GREATEST(${insiderPositions.lastSeen}, excluded.last_seen)
						END
					`,
				detectedAt: sql`
						CASE
							WHEN ${insiderPositions.detectedAt} IS NULL THEN excluded.detected_at
							ELSE LEAST(${insiderPositions.detectedAt}, excluded.detected_at)
						END
					`,
			},
		});
}

export async function updateTokenStats(items: InsiderPositionIncrement[]) {
	if (items.length === 0) return;

	const tokenAggregates = aggregateTokenStats(items);

	await db
		.insert(tokenStats)
		.values(
			tokenAggregates.map((item) => {
				const totalTrades = item.totalTrades;
				const mean = totalTrades > 0 ? item.sumPrice / totalTrades : 0;
				const variance =
					totalTrades > 0 ? item.sumPriceSq / totalTrades - mean * mean : 0;
				return {
					token: item.token,
					totalTrades: item.totalTrades,
					totalVol: item.totalVol,
					lastPrice: item.lastPrice,
					sumPrice: item.sumPrice,
					sumPriceSq: item.sumPriceSq,
					mean,
					stdDev: Math.sqrt(Math.max(variance, 0)),
				};
			}),
		)
		.onConflictDoUpdate({
			target: tokenStats.token,
			set: {
				totalTrades: sql`coalesce(${tokenStats.totalTrades}, 0) + excluded.total_trades`,
				totalVol: sql`coalesce(${tokenStats.totalVol}, 0) + excluded.total_vol`,
				lastPrice: sql`excluded.last_price`,
				sumPrice: sql`coalesce(${tokenStats.sumPrice}, 0) + excluded.sum_price`,
				sumPriceSq: sql`coalesce(${tokenStats.sumPriceSq}, 0) + excluded.sum_price_sq`,
				mean: sql`
						CASE
							WHEN (coalesce(${tokenStats.totalTrades}, 0) + excluded.total_trades) > 0 THEN
								(coalesce(${tokenStats.sumPrice}, 0) + excluded.sum_price) /
								(coalesce(${tokenStats.totalTrades}, 0) + excluded.total_trades)
							ELSE 0
						END
					`,
				stdDev: sql`
						CASE
							WHEN (coalesce(${tokenStats.totalTrades}, 0) + excluded.total_trades) > 0 THEN
								sqrt(
									GREATEST(
										(
											(coalesce(${tokenStats.sumPriceSq}, 0) + excluded.sum_price_sq) /
											(coalesce(${tokenStats.totalTrades}, 0) + excluded.total_trades)
										) -
										power(
											(
												(coalesce(${tokenStats.sumPrice}, 0) + excluded.sum_price) /
												(coalesce(${tokenStats.totalTrades}, 0) + excluded.total_trades)
											),
											2
										),
										0
									)
								)
							ELSE 0
						END
					`,
			},
		});
}

export async function upsertAccountWalletMappings(
	items: AccountWalletMappingIncrement[],
) {
	if (items.length === 0) return;

	await db
		.insert(accountWalletMap)
		.values(
			items.map((item) => ({
				accountHash: item.accountHash,
				walletAddress: item.walletAddress,
				firstSeen: item.firstSeen,
				lastSeen: item.lastSeen,
			})),
		)
		.onConflictDoUpdate({
			target: accountWalletMap.accountHash,
			set: {
				walletAddress: sql`excluded.wallet_address`,
				firstSeen: sql`
						CASE
							WHEN ${accountWalletMap.firstSeen} IS NULL THEN excluded.first_seen
							ELSE LEAST(${accountWalletMap.firstSeen}, excluded.first_seen)
						END
					`,
				lastSeen: sql`
						CASE
							WHEN ${accountWalletMap.lastSeen} IS NULL THEN excluded.last_seen
							ELSE GREATEST(${accountWalletMap.lastSeen}, excluded.last_seen)
						END
					`,
			},
		});
}

/**
 * Persist insider positions and update market token stats in one transaction.
 * Uses ON CONFLICT updates so repeated detections increment counters safely.
 * @deprecated Use upsertInsiderPositions and updateTokenStats separately
 */
export async function upsertInsiderPositionIncrements(
	items: InsiderPositionIncrement[],
) {
	if (items.length === 0) return;
	await upsertInsiderPositions(items);
	await updateTokenStats(items);
}
