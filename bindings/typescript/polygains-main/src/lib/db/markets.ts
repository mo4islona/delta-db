import { desc, eq } from "drizzle-orm";
import { tokenMarketLookup, tokenStats, vMarketSummary } from "@/lib/db/schema";
import { db } from "./init";

/**
 * Query top 5 markets by volume
 */
export async function queryTopMarket() {
	const tokens = await db
		.select({
			token: tokenStats.token,
			totalVol: tokenStats.totalVol,
			lastPrice: tokenStats.lastPrice,
			conditionId: tokenMarketLookup.conditionId,
		})
		.from(tokenStats)
		.leftJoin(
			tokenMarketLookup,
			eq(tokenStats.token, tokenMarketLookup.tokenId),
		)
		.orderBy(desc(tokenStats.totalVol))
		.limit(5);

	return tokens.map((token) => ({
		token: token.token,
		total_vol: token.totalVol,
		last_price: token.lastPrice,
		condition_id: token.conditionId,
	}));
}

/**
 * Get all market summaries
 */
export async function getMarketSummaries() {
	return db.select().from(vMarketSummary);
}

/**
 * Get market summary by condition ID
 */
export async function getMarketByConditionId(conditionId: string) {
	return db
		.select()
		.from(vMarketSummary)
		.where(eq(vMarketSummary.conditionId, conditionId));
}
