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

describe("Insider trigger price filter", () => {
	test("returns only alerts with avg trigger price below 0.95 and exposes trigger price in alert.price", async () => {
		const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
		const conditionId = `it-trigger-price-${suffix}`;
		const yesTokenId = `9201${suffix}`;
		const noTokenId = `9202${suffix}`;
		const tokenIds = [yesTokenId, noTokenId];
		const nowMs = Date.now();
		const accountIncluded = Bun.hash.xxHash32(`it-trigger-in-${suffix}`) | 0;
		const accountExcluded = Bun.hash.xxHash32(`it-trigger-out-${suffix}`) | 0;
		const accountHashes = [accountIncluded, accountExcluded];
		const includedPositionId = `${accountIncluded}-${yesTokenId}`;
		const excludedPositionId = `${accountExcluded}-${noTokenId}`;
		const positionIds = [includedPositionId, excludedPositionId];
		const detectedAt = nowMs + 7_000_000_000;

		await cleanupFixtures({
			conditionId,
			tokenIds,
			positionIds,
			accountHashes,
		});

		try {
			await db.insert(markets).values({
				conditionId,
				question: "IT: trigger price filtering",
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
					lastPrice: 0.99,
					totalTrades: 2,
					totalVol: 2_000,
				},
				{
					token: noTokenId,
					lastPrice: 0.12,
					totalTrades: 2,
					totalVol: 2_000,
				},
			]);

			await db.insert(insiderPositions).values([
				{
					id: includedPositionId,
					accountHash: accountIncluded,
					tokenId: yesTokenId,
					totalVolume: 2_500,
					tradeCount: 2,
					avgPrice: 0.94,
					sumPrice: 1.88,
					sumPriceSq: 1.7672,
					firstSeen: detectedAt,
					lastSeen: detectedAt,
					detectedAt,
				},
				{
					id: excludedPositionId,
					accountHash: accountExcluded,
					tokenId: noTokenId,
					totalVolume: 3_000,
					tradeCount: 2,
					avgPrice: 0.97,
					sumPrice: 1.94,
					sumPriceSq: 1.8818,
					firstSeen: detectedAt - 1,
					lastSeen: detectedAt - 1,
					detectedAt: detectedAt - 1,
				},
			]);

			const result = await getInsiderAlerts(200, 0);
			const fixtureAlerts = result.alerts.filter(
				(alert) => String(alert.conditionId) === conditionId,
			);

			const includedAlert = fixtureAlerts.find(
				(alert) =>
					String(alert.conditionId) === conditionId &&
					String(alert.tokenId) === yesTokenId &&
					String(alert.user) === String(accountIncluded),
			);
			expect(includedAlert).toBeDefined();
			expect(Number(includedAlert?.price ?? 0)).toBeCloseTo(0.94, 6);
			expect(Number(includedAlert?.market_price ?? 0)).toBeCloseTo(0.99, 6);

			const excludedAlert = fixtureAlerts.find(
				(alert) =>
					String(alert.conditionId) === conditionId &&
					String(alert.tokenId) === noTokenId &&
					String(alert.user) === accountExcluded,
			);
			expect(excludedAlert).toBeUndefined();

			expect(
				fixtureAlerts.every((alert) => Number(alert.price) <= 0.950001),
			).toBe(true);
		} finally {
			await cleanupFixtures({
				conditionId,
				tokenIds,
				positionIds,
				accountHashes,
			});
		}
	});

	test("includes near-0.95 floating values but excludes prices materially above 0.95", async () => {
		const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
		const conditionId = `it-trigger-eps-${suffix}`;
		const yesTokenId = `9301${suffix}`;
		const noTokenId = `9302${suffix}`;
		const tokenIds = [yesTokenId, noTokenId];
		const nowMs = Date.now();
		const accountBoundary =
			Bun.hash.xxHash32(`it-trigger-boundary-${suffix}`) | 0;
		const accountTooHigh = Bun.hash.xxHash32(`it-trigger-high-${suffix}`) | 0;
		const accountHashes = [accountBoundary, accountTooHigh];
		const boundaryPositionId = `${accountBoundary}-${yesTokenId}`;
		const highPositionId = `${accountTooHigh}-${noTokenId}`;
		const positionIds = [boundaryPositionId, highPositionId];
		const detectedAt = nowMs + 7_100_000_000;

		await cleanupFixtures({
			conditionId,
			tokenIds,
			positionIds,
			accountHashes,
		});

		try {
			await db.insert(markets).values({
				conditionId,
				question: "IT: trigger epsilon boundary",
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
					lastPrice: 0.99,
					totalTrades: 1,
					totalVol: 1_000,
				},
				{
					token: noTokenId,
					lastPrice: 0.98,
					totalTrades: 1,
					totalVol: 1_000,
				},
			]);

			await db.insert(insiderPositions).values([
				{
					id: boundaryPositionId,
					accountHash: accountBoundary,
					tokenId: yesTokenId,
					totalVolume: 1_100,
					tradeCount: 1,
					avgPrice: 0.9500005,
					sumPrice: 0.9500005,
					sumPriceSq: 0.90250095000025,
					firstSeen: detectedAt,
					lastSeen: detectedAt,
					detectedAt,
				},
				{
					id: highPositionId,
					accountHash: accountTooHigh,
					tokenId: noTokenId,
					totalVolume: 1_200,
					tradeCount: 1,
					avgPrice: 0.9501,
					sumPrice: 0.9501,
					sumPriceSq: 0.90269001,
					firstSeen: detectedAt - 1,
					lastSeen: detectedAt - 1,
					detectedAt: detectedAt - 1,
				},
			]);

			const result = await getInsiderAlerts(200, 0);
			const fixtureAlerts = result.alerts.filter(
				(alert) => String(alert.conditionId) === conditionId,
			);

			const boundaryAlert = fixtureAlerts.find(
				(alert) =>
					String(alert.tokenId) === yesTokenId &&
					String(alert.user) === String(accountBoundary),
			);
			expect(boundaryAlert).toBeDefined();
			expect(Number(boundaryAlert?.price ?? 0)).toBeCloseTo(0.9500005, 6);

			const highAlert = fixtureAlerts.find(
				(alert) =>
					String(alert.tokenId) === noTokenId &&
					String(alert.user) === accountTooHigh,
			);
			expect(highAlert).toBeUndefined();
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
