import { describe, expect, test } from "bun:test";
import { eq, inArray } from "drizzle-orm";
import { db } from "@/lib/db/init";
import { getMarketByCondition } from "@/lib/db/queries";
import { setupTestingSchema } from "./helpers/testingSchema";
import {
	markets,
	marketTokens,
	tokenMarketLookup,
	tokenStats,
} from "@/lib/db/schema";
import { SIDE } from "@/lib/types";
import { InsiderDetector, NotInsiderDetector } from "@/services/detector";
import { PolymarketPipe } from "@/services/pipe";

setupTestingSchema();

interface TestablePipe {
	initialized: boolean;
	cursor: { number: number };
	insiderDetector: InsiderDetector;
	notInsiderDetector: NotInsiderDetector;
	persistor: { onBatchProcessed: () => boolean };
}

function buildPipeForIntegrationTest() {
	const pipe = new PolymarketPipe();
	const tp = pipe as unknown as TestablePipe;

	// Keep this test focused on market-stats ingestion, not snapshot recovery.
	tp.initialized = true;
	tp.cursor = { number: 1 };
	tp.insiderDetector = new InsiderDetector();
	tp.notInsiderDetector = new NotInsiderDetector();
	tp.persistor = {
		onBatchProcessed: () => false,
	};

	return pipe;
}

async function cleanupMarketFixtures(conditionId: string, tokenIds: string[]) {
	await db.delete(tokenStats).where(inArray(tokenStats.token, tokenIds));
	await db
		.delete(marketTokens)
		.where(eq(marketTokens.marketConditionId, conditionId));
	await db
		.delete(tokenMarketLookup)
		.where(eq(tokenMarketLookup.conditionId, conditionId));
	await db.delete(markets).where(eq(markets.conditionId, conditionId));
}

describe("Market stats integration", () => {
	test("persists stats for both outcomes with multiple matched fills per side", async () => {
		const suffix = `${Date.now()}${Math.floor(Math.random() * 10000)}`;
		const conditionId = `it-market-${suffix}`;
		const yesTokenId = `9001${suffix}`;
		const noTokenId = `9002${suffix}`;
		const tokenIds = [yesTokenId, noTokenId];
		const nowMs = Date.now();
		const nowSec = Math.floor(nowMs / 1000);

		await cleanupMarketFixtures(conditionId, tokenIds);

		try {
			await db.insert(markets).values({
				conditionId,
				question: "IT: Market stats should include both outcomes",
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
				{
					tokenId: yesTokenId,
					conditionId,
					createdAt: nowMs,
				},
				{
					tokenId: noTokenId,
					conditionId,
					createdAt: nowMs,
				},
			]);

			const pipe = buildPipeForIntegrationTest();
			const read = async function* () {
				yield {
					ctx: { state: { current: { number: 1, timestamp: nowSec } } },
					data: [
						// Regression case: SELL-side fills must contribute to market stats.
						{
							trader: "0x0000000000000000000000000000000000000001",
							assetId: yesTokenId,
							usdc: 350000n,
							shares: 1000000n,
							side: SIDE.SELL,
							timestamp: nowSec,
						},
						{
							trader: "0x0000000000000000000000000000000000000003",
							assetId: yesTokenId,
							usdc: 420000n,
							shares: 1000000n,
							side: SIDE.SELL,
							timestamp: nowSec + 1,
						},
						{
							trader: "0x0000000000000000000000000000000000000002",
							assetId: noTokenId,
							usdc: 960000n,
							shares: 1000000n,
							side: SIDE.BUY,
							timestamp: nowSec + 2,
						},
						{
							trader: "0x0000000000000000000000000000000000000004",
							assetId: noTokenId,
							usdc: 910000n,
							shares: 1000000n,
							side: SIDE.BUY,
							timestamp: nowSec + 3,
						},
					],
				};
			};

			await pipe.write({
				logger: { error: () => {} },
				read,
			});

			const market = await getMarketByCondition(conditionId);
			expect(market).not.toBeNull();
			expect(market?.outcomes.length).toBe(2);

			const byOutcome = new Map(
				(market?.outcomes ?? []).map((row) => [
					String(row.outcome).toUpperCase(),
					row,
				]),
			);
			const yes = byOutcome.get("YES");
			const no = byOutcome.get("NO");

			expect(yes).toBeDefined();
			expect(no).toBeDefined();

			expect(yes?.totalTrades ?? 0).toBeGreaterThan(1);
			expect(no?.totalTrades ?? 0).toBeGreaterThan(1);
			expect(yes?.volume ?? 0).toBeGreaterThan(0);
			expect(no?.volume ?? 0).toBeGreaterThan(0);
			expect(yes?.mean ?? null).not.toBeNull();
			expect(no?.mean ?? null).not.toBeNull();
			expect(yes?.stdDev ?? 0).toBeGreaterThan(0);
			expect(no?.stdDev ?? 0).toBeGreaterThan(0);
			expect(yes?.mean ?? 0).toBeCloseTo(0.385, 6);
			expect(no?.mean ?? 0).toBeCloseTo(0.935, 6);
		} finally {
			await cleanupMarketFixtures(conditionId, tokenIds);
		}
	});
});
