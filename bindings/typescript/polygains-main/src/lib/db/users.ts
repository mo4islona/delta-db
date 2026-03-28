import { eq, sql } from "drizzle-orm";
import { accountStats } from "@/lib/db/schema";
import { db } from "./init";

/**
 * Upsert account statistics
 * Increments trades and volume, updates last trade time
 */
export async function upsertStats(params: {
	acct: string;
	trades: number;
	vol: number;
	time: number;
}) {
	await db
		.insert(accountStats)
		.values({
			account: params.acct,
			totalTrades: params.trades,
			totalVol: params.vol,
			lastTradeTime: params.time,
		})
		.onConflictDoUpdate({
			target: accountStats.account,
			set: {
				totalTrades: sql`${accountStats.totalTrades} + excluded.total_trades`,
				totalVol: sql`${accountStats.totalVol} + excluded.total_vol`,
				lastTradeTime: sql`GREATEST(${accountStats.lastTradeTime}, excluded.last_trade_time)`,
			},
		});
}

/**
 * Bulk upsert account statistics
 */
export async function upsertStatsBulk(
	items: {
		acct: string;
		trades: number;
		vol: number;
		time: number;
	}[],
) {
	if (items.length === 0) return;

	await db
		.insert(accountStats)
		.values(
			items.map((params) => ({
				account: params.acct,
				totalTrades: params.trades,
				totalVol: params.vol,
				lastTradeTime: params.time,
			})),
		)
		.onConflictDoUpdate({
			target: accountStats.account,
			set: {
				totalTrades: sql`${accountStats.totalTrades} + excluded.total_trades`,
				totalVol: sql`${accountStats.totalVol} + excluded.total_vol`,
				lastTradeTime: sql`GREATEST(${accountStats.lastTradeTime}, excluded.last_trade_time)`,
			},
		});
}

/**
 * Get account statistics by account address
 */
export async function getAccountStats(account: string) {
	const result = await db
		.select()
		.from(accountStats)
		.where(eq(accountStats.account, account))
		.limit(1);
	return result[0];
}

/**
 * Get all accounts with statistics
 */
export async function getAllAccountStats() {
	return db.select().from(accountStats);
}
