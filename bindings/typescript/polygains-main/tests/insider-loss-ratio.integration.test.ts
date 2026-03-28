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
	positionIds: string[];
	accountHashes: number[];
}) {
	if (params.positionIds.length > 0) {
		await db
			.delete(insiderPositions)
			.where(inArray(insiderPositions.id, params.positionIds));
	}
	if (params.accountHashes.length > 0) {
		await db
			.delete(accountWalletMap)
			.where(inArray(accountWalletMap.accountHash, params.accountHashes));
	}
	await db.delete(tokenStats).where(inArray(tokenStats.token, params.tokenIds));
	await db
		.delete(marketTokens)
		.where(eq(marketTokens.marketConditionId, params.conditionId));
	await db
		.delete(tokenMarketLookup)
		.where(eq(tokenMarketLookup.conditionId, params.conditionId));
	await db.delete(markets).where(eq(markets.conditionId, params.conditionId));
}

describe("Insider alert loss floor", () => {
	test("returns at least 20% losses for a mixed resolved fixture", async () => {
		const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
		const conditionId = `it-loss-ratio-${suffix}`;
		const yesTokenId = `8101${suffix}`;
		const noTokenId = `8102${suffix}`;
		const tokenIds = [yesTokenId, noTokenId];
		const nowMs = Date.now();
		const baseDetectedAt = nowMs + 6_000_000_000;
		const accountHashes = Array.from(
			{ length: 5 },
			(_, i) => Bun.hash.xxHash32(`it-loss-${suffix}-${i}`) | 0,
		);
		const positionIds = accountHashes.map((accountHash, i) => {
			const tokenId = i === 0 ? noTokenId : yesTokenId; // 1 loser, 4 winners
			return `${accountHash}-${tokenId}`;
		});

		await cleanupFixtures({
			conditionId,
			tokenIds,
			positionIds,
			accountHashes,
		});

		try {
			await db.insert(markets).values({
				conditionId,
				question: "IT: enforce minimum loser share",
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
					winner: true,
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
					lastPrice: 0.99,
					totalTrades: 5,
					totalVol: 5_000,
				},
				{
					token: noTokenId,
					lastPrice: 0.01,
					totalTrades: 2,
					totalVol: 2_000,
				},
			]);

			await db.insert(insiderPositions).values(
				accountHashes.map((accountHash, i) => {
					const tokenId = i === 0 ? noTokenId : yesTokenId;
					const detectedAt = baseDetectedAt - i;
					return {
						id: `${accountHash}-${tokenId}`,
						accountHash,
						tokenId,
						totalVolume: 1_000 + i * 100,
						tradeCount: 1,
						avgPrice: 0.94,
						sumPrice: 0.94,
						sumPriceSq: 0.8836,
						firstSeen: detectedAt,
						lastSeen: detectedAt,
						detectedAt,
					};
				}),
			);

			const result = await getInsiderAlerts(50, 0);
			const fixtureAlerts = result.alerts.filter(
				(alert) => String(alert.conditionId) === conditionId,
			);
			expect(fixtureAlerts.length).toBe(5);

			const losses = fixtureAlerts.filter(
				(alert) => alert.winner === false,
			).length;
			const lossRatio = losses / fixtureAlerts.length;
			expect(lossRatio).toBeGreaterThanOrEqual(0.2);
		} finally {
			await cleanupFixtures({
				conditionId,
				tokenIds,
				positionIds,
				accountHashes,
			});
		}
	});
});
