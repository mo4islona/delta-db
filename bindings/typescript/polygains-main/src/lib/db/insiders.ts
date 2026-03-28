import { desc, eq } from "drizzle-orm";
import { detectedInsiders, vInsidersEnriched } from "@/lib/db/schema";
import { db } from "./init";

/**
 * Insert a detected insider (ignore if already exists)
 */
export async function insertInsider(params: {
	acct: string;
	now: number;
	vol: number;
	token: string;
}) {
	await db
		.insert(detectedInsiders)
		.values({
			account: params.acct,
			detectedAt: params.now,
			volume: params.vol,
			tokenId: params.token,
		})
		.onConflictDoNothing();
}

/**
 * Bulk insert detected insiders
 */
export async function insertInsiderBulk(
	items: {
		acct: string;
		now: number;
		vol: number;
		token: string;
	}[],
) {
	if (items.length === 0) return;

	await db
		.insert(detectedInsiders)
		.values(
			items.map((params) => ({
				account: params.acct,
				detectedAt: params.now,
				volume: params.vol,
				tokenId: params.token,
			})),
		)
		.onConflictDoNothing();
}

/**
 * Query top 5 insiders by volume
 */
export async function queryTopInsiders() {
	const insiders = await db
		.select()
		.from(detectedInsiders)
		.orderBy(desc(detectedInsiders.volume))
		.limit(5);

	return insiders.map((insider) => ({
		account: insider.account,
		volume: insider.volume,
		token_id: insider.tokenId,
		time: insider.detectedAt
			? new Date(insider.detectedAt)
					.toISOString()
					.replace("T", " ")
					.split(".")[0]
			: null,
	}));
}

/**
 * Get all detected insiders with enriched market data
 */
export async function getInsidersEnriched() {
	return db
		.select()
		.from(vInsidersEnriched)
		.orderBy(desc(vInsidersEnriched.detectedAt));
}

/**
 * Get insider by account address
 */
export async function getInsiderByAccount(account: string) {
	const result = await db
		.select()
		.from(detectedInsiders)
		.where(eq(detectedInsiders.account, account))
		.limit(1);
	return result[0];
}
