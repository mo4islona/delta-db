import { describe, expect, test } from "bun:test";
import { hashWallet } from "@/lib/hash";
import { SIDE } from "@/lib/types";
import { InsiderDetector, NotInsiderDetector } from "@/services/detector";
import { PolymarketPipe } from "@/services/pipe";

interface TestablePipe {
	insiderDetector: InsiderDetector;
	notInsiderDetector: NotInsiderDetector;
	insiderCount: number;
	notInsiderCount: number;
	initialized: boolean;
	persistor: { onBatchProcessed: (task: unknown) => void };
	stateFile: unknown;
	aggPositions: { has: (key: string) => boolean };
}

function buildPipeForUnitTest() {
	const pipe = new PolymarketPipe();
	const insiderDetector = new InsiderDetector();
	const notInsiderDetector = new NotInsiderDetector();
	const tp = pipe as unknown as TestablePipe;

	tp.insiderDetector = insiderDetector;
	tp.notInsiderDetector = notInsiderDetector;
	tp.insiderCount = 0;
	tp.notInsiderCount = 0;
	tp.initialized = true;
	tp.persistor = {
		onBatchProcessed: () => {},
	};
	tp.stateFile = Bun.file(
		`/tmp/poly-sqd-ts-pipe-test-${Date.now()}-${Math.random().toString(16).slice(2)}.json`,
	);

	return { pipe, insiderDetector, notInsiderDetector };
}

describe("PolymarketPipe classification window", () => {
	test("marks sub-4k trader as non-insider when only post-window trades push total above threshold", async () => {
		const { pipe, insiderDetector, notInsiderDetector } =
			buildPipeForUnitTest();
		const trader = "0x1111111111111111111111111111111111111111";

		const firstBatchOrder = {
			trader,
			assetId: 1001n,
			usdc: 3_000_000_000n, // $3k
			shares: 3_200_000_000n, // price = 0.9375 (< 0.95)
			side: SIDE.BUY,
			timestamp: 1_000,
		};

		const postWindowOrder = {
			trader,
			assetId: 1001n,
			usdc: 2_000_000_000n, // $2k, but outside first 15 minutes
			shares: 2_200_000_000n, // price ~= 0.9091 (< 0.95)
			side: SIDE.BUY,
			timestamp: 2_000, // 1000s later (> 900s window)
		};

		const batches = [
			{
				ctx: { state: { current: { number: 1, timestamp: 1_000 } } },
				data: [firstBatchOrder],
			},
			{
				ctx: { state: { current: { number: 2, timestamp: 2_000 } } },
				data: [postWindowOrder],
			},
		];

		const read = async function* () {
			for (const batch of batches) {
				yield batch;
			}
		};

		await pipe.write({
			logger: { error: () => {} },
			read,
		});

		expect(insiderDetector.has(trader)).toBe(false);
		expect(notInsiderDetector.has(trader)).toBe(true);
	});

	test("only tracks BUY orders priced below 0.95", async () => {
		const { pipe } = buildPipeForUnitTest();
		const tp = pipe as unknown as TestablePipe;
		const highTrader = "0x2222222222222222222222222222222222222222";
		const lowTrader = "0x3333333333333333333333333333333333333333";
		const highHash = hashWallet(highTrader);
		const lowHash = hashWallet(lowTrader);

		const highPriceOrder = {
			trader: highTrader,
			assetId: 2002n,
			usdc: 1_000_000_000n,
			shares: 1_000_000_000n, // price = 1.0 (filtered out)
			side: SIDE.BUY,
			timestamp: 3_000,
		};

		const lowPriceOrder = {
			trader: lowTrader,
			assetId: 3003n,
			usdc: 900_000_000n,
			shares: 1_000_000_000n, // price = 0.9 (kept)
			side: SIDE.BUY,
			timestamp: 3_000,
		};

		const batches = [
			{
				ctx: { state: { current: { number: 3, timestamp: 3_000 } } },
				data: [highPriceOrder, lowPriceOrder],
			},
		];

		const read = async function* () {
			for (const batch of batches) {
				yield batch;
			}
		};

		await pipe.write({
			logger: { error: () => {} },
			read,
		});

		expect(tp.aggPositions.has(String(highHash))).toBe(false);
		expect(tp.aggPositions.has(String(lowHash))).toBe(true);
	});
});
