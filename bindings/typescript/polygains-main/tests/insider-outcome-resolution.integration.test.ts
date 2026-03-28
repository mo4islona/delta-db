import { describe, expect, test } from "bun:test";
import { eq, inArray } from "drizzle-orm";
import { db } from "@/lib/db/init";
import { getInsiderAlerts } from "@/lib/db/queries";
import { setupTestingSchema } from "./helpers/testingSchema";
import {
	accountWalletMap,
	insiderPositions,
	markets,
	marketTokens,
	tokenMarketLookup,
	tokenStats,
} from "@/lib/db/schema";

setupTestingSchema();

async function cleanupFixtures(params: {
	conditionId: string;
	tokenIds: string[];
	positionId: string;
	accountHash: number;
}) {
	await db
		.delete(insiderPositions)
		.where(eq(insiderPositions.id, params.positionId));
	await db
		.delete(accountWalletMap)
		.where(eq(accountWalletMap.accountHash, params.accountHash));
	await db.delete(tokenStats).where(inArray(tokenStats.token, params.tokenIds));
	await db
		.delete(marketTokens)
		.where(eq(marketTokens.marketConditionId, params.conditionId));
	await db
		.delete(tokenMarketLookup)
		.where(eq(tokenMarketLookup.conditionId, params.conditionId));
	await db.delete(markets).where(eq(markets.conditionId, params.conditionId));
}

describe("Insider outcome resolution", () => {
	test("resolves winner via condition join after market closes", async () => {
		const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
		const conditionId = `it-outcome-${suffix}`;
		const yesTokenId = `7001${suffix}`;
		const noTokenId = `7002${suffix}`;
		const tokenIds = [yesTokenId, noTokenId];
		const accountHash = Bun.hash.xxHash32(`it-account-${suffix}`) | 0;
		const positionId = `${accountHash}-${yesTokenId}`;
		const nowMs = Date.now();
		const detectedAt = nowMs + 5_000_000_000;

		await cleanupFixtures({ conditionId, tokenIds, positionId, accountHash });

		try {
			await db.insert(markets).values({
				conditionId,
				question: "IT: winner resolved after alert",
				outcomeTags: "Politics,All",
				active: true,
				closed: false,
				updatedAt: nowMs,
			});

			await db.insert(marketTokens).values([
				{
					tokenId: yesTokenId,
					marketConditionId: conditionId,
					outcome: "Yes",
					tokenIndex: 0,
					outcomeIndex: 0,
					winner: false,
				},
				{
					tokenId: noTokenId,
					marketConditionId: conditionId,
					outcome: "No",
					tokenIndex: 1,
					outcomeIndex: 1,
					winner: false,
				},
			]);

			await db.insert(tokenMarketLookup).values([
				{ tokenId: yesTokenId, conditionId, createdAt: nowMs },
				{ tokenId: noTokenId, conditionId, createdAt: nowMs },
			]);

			await db.insert(tokenStats).values([
				{
					token: yesTokenId,
					lastPrice: 0.53,
					totalTrades: 1,
					totalVol: 123,
				},
				{
					token: noTokenId,
					lastPrice: 0.47,
					totalTrades: 1,
					totalVol: 456,
				},
			]);

			await db.insert(insiderPositions).values({
				id: positionId,
				accountHash,
				tokenId: yesTokenId,
				totalVolume: 5_000,
				tradeCount: 1,
				avgPrice: 0.42,
				sumPrice: 0.42,
				sumPriceSq: 0.1764,
				firstSeen: detectedAt,
				lastSeen: detectedAt,
				detectedAt,
			});

			const openResult = await getInsiderAlerts(20, 0);
			const openAlert = openResult.alerts.find(
				(alert) =>
					String(alert.conditionId) === conditionId &&
					String(alert.tokenId) === yesTokenId,
			);
			expect(openAlert).toBeDefined();
			expect(openAlert?.closed).toBe(false);
			expect(openAlert?.winner ?? null).toBeNull();

			await db
				.update(markets)
				.set({ closed: true, updatedAt: nowMs + 1_000 })
				.where(eq(markets.conditionId, conditionId));
			await db
				.update(marketTokens)
				.set({ winner: false })
				.where(eq(marketTokens.tokenId, yesTokenId));
			await db
				.update(marketTokens)
				.set({ winner: true })
				.where(eq(marketTokens.tokenId, noTokenId));

			const closedLoserResult = await getInsiderAlerts(20, 0);
			const closedLoserAlert = closedLoserResult.alerts.find(
				(alert) =>
					String(alert.conditionId) === conditionId &&
					String(alert.tokenId) === yesTokenId,
			);
			expect(closedLoserAlert).toBeDefined();
			expect(closedLoserAlert?.closed).toBe(true);
			expect(closedLoserAlert?.winner).toBe(false);

			await db
				.update(marketTokens)
				.set({ winner: true })
				.where(eq(marketTokens.tokenId, yesTokenId));
			await db
				.update(marketTokens)
				.set({ winner: false })
				.where(eq(marketTokens.tokenId, noTokenId));

			const closedWinnerResult = await getInsiderAlerts(20, 0);
			const closedWinnerAlert = closedWinnerResult.alerts.find(
				(alert) =>
					String(alert.conditionId) === conditionId &&
					String(alert.tokenId) === yesTokenId,
			);
			expect(closedWinnerAlert).toBeDefined();
			expect(closedWinnerAlert?.winner).toBe(true);
		} finally {
			await cleanupFixtures({ conditionId, tokenIds, positionId, accountHash });
		}
	});

	test("uses unknown winner for closed markets without a resolved token unless price is extreme", async () => {
		const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
		const conditionId = `it-outcome-unknown-${suffix}`;
		const yesTokenId = `7101${suffix}`;
		const noTokenId = `7102${suffix}`;
		const tokenIds = [yesTokenId, noTokenId];
		const accountHash = Bun.hash.xxHash32(`it-account-unknown-${suffix}`) | 0;
		const positionId = `${accountHash}-${yesTokenId}`;
		const nowMs = Date.now();
		const detectedAt = nowMs + 5_100_000_000;

		await cleanupFixtures({ conditionId, tokenIds, positionId, accountHash });

		try {
			await db.insert(markets).values({
				conditionId,
				question: "IT: unknown fallback band",
				outcomeTags: "Politics,All",
				active: true,
				closed: true,
				updatedAt: nowMs,
			});

			await db.insert(marketTokens).values([
				{
					tokenId: yesTokenId,
					marketConditionId: conditionId,
					outcome: "Yes",
					tokenIndex: 0,
					outcomeIndex: 0,
					winner: false,
				},
				{
					tokenId: noTokenId,
					marketConditionId: conditionId,
					outcome: "No",
					tokenIndex: 1,
					outcomeIndex: 1,
					winner: false,
				},
			]);

			await db.insert(tokenMarketLookup).values([
				{ tokenId: yesTokenId, conditionId, createdAt: nowMs },
				{ tokenId: noTokenId, conditionId, createdAt: nowMs },
			]);

			await db.insert(tokenStats).values([
				{
					token: yesTokenId,
					lastPrice: 0.5,
					totalTrades: 1,
					totalVol: 100,
				},
				{
					token: noTokenId,
					lastPrice: 0.5,
					totalTrades: 1,
					totalVol: 100,
				},
			]);

			await db.insert(insiderPositions).values({
				id: positionId,
				accountHash,
				tokenId: yesTokenId,
				totalVolume: 2_000,
				tradeCount: 1,
				avgPrice: 0.4,
				sumPrice: 0.4,
				sumPriceSq: 0.16,
				firstSeen: detectedAt,
				lastSeen: detectedAt,
				detectedAt,
			});

			const midResult = await getInsiderAlerts(20, 0);
			const midAlert = midResult.alerts.find(
				(alert) =>
					String(alert.conditionId) === conditionId &&
					String(alert.tokenId) === yesTokenId,
			);
			expect(midAlert).toBeDefined();
			expect(midAlert?.winner ?? null).toBeNull();

			await db
				.update(tokenStats)
				.set({ lastPrice: 0.99 })
				.where(eq(tokenStats.token, yesTokenId));
			const highResult = await getInsiderAlerts(20, 0);
			const highAlert = highResult.alerts.find(
				(alert) =>
					String(alert.conditionId) === conditionId &&
					String(alert.tokenId) === yesTokenId,
			);
			expect(highAlert?.winner).toBe(true);

			await db
				.update(tokenStats)
				.set({ lastPrice: 0.04 })
				.where(eq(tokenStats.token, yesTokenId));
			const lowResult = await getInsiderAlerts(20, 0);
			const lowAlert = lowResult.alerts.find(
				(alert) =>
					String(alert.conditionId) === conditionId &&
					String(alert.tokenId) === yesTokenId,
			);
			expect(lowAlert?.winner).toBe(false);
		} finally {
			await cleanupFixtures({ conditionId, tokenIds, positionId, accountHash });
		}
	});
});
