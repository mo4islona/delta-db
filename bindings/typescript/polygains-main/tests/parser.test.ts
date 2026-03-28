import { describe, expect, test } from "bun:test";
import { parseOrder } from "@/lib/parser";
import { EVENT, SIDE } from "@/lib/types";

describe("parseOrder", () => {
	describe("Buy orders", () => {
		test("should correctly parse a buy order (takerAssetId = 0)", () => {
			const mockOrder = {
				block: { number: 42118906 },
				event: {
					takerAssetId: 0n,
					makerAssetId: 123456789n,
					makerAmountFilled: 1000000n, // shares
					takerAmountFilled: 500000n, // USDC (6 decimals)
					takerOrderMaker: "0x1234567890123456789012345678901234567890",
				},
				rawEvent: {
					logIndex: 5,
					transactionIndex: 2,
				},
				timestamp: 1700000000,
			};

			const result = parseOrder(mockOrder);

			expect(result).toMatchObject({
				kind: EVENT.ORDER,
				trader: "0x1234567890123456789012345678901234567890",
				assetId: 123456789n,
				side: SIDE.BUY,
				shares: 1000000n,
				usdc: 500000n,
				block: 42118906,
				logIndex: 5,
				transactionIndex: 2,
				timestamp: 1700000000,
			});
		});

		test("should handle large buy order values", () => {
			const mockOrder = {
				block: { number: 50000000 },
				event: {
					takerAssetId: 0n,
					makerAssetId: 999999999999999999n,
					makerAmountFilled: 1000000000000n,
					takerAmountFilled: 950000000000n,
					takerOrderMaker: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				},
				rawEvent: {
					logIndex: 100,
					transactionIndex: 50,
				},
				timestamp: 1800000000,
			};

			const result = parseOrder(mockOrder);

			expect(result.shares).toBe(1000000000000n);
			expect(result.usdc).toBe(950000000000n);
			expect(result.side).toBe(SIDE.BUY);
		});
	});

	describe("Sell orders", () => {
		test("should correctly parse a sell order (takerAssetId != 0)", () => {
			const mockOrder = {
				block: { number: 42118907 },
				event: {
					takerAssetId: 987654321n,
					makerAssetId: 0n,
					makerAmountFilled: 750000n, // USDC
					takerAmountFilled: 1000000n, // shares
					takerOrderMaker: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				},
				rawEvent: {
					logIndex: 10,
					transactionIndex: 8,
				},
				timestamp: 1700001000,
			};

			const result = parseOrder(mockOrder);

			expect(result).toMatchObject({
				kind: EVENT.ORDER,
				trader: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				assetId: 987654321n,
				side: SIDE.SELL,
				shares: 1000000n,
				usdc: 750000n,
				block: 42118907,
				logIndex: 10,
				transactionIndex: 8,
				timestamp: 1700001000,
			});
		});

		test("should handle sell order with non-zero takerAssetId", () => {
			const mockOrder = {
				block: { number: 42200000 },
				event: {
					takerAssetId: 111111n,
					makerAssetId: 222222n,
					makerAmountFilled: 2000000n,
					takerAmountFilled: 2100000n,
					takerOrderMaker: "0xcccccccccccccccccccccccccccccccccccccccc",
				},
				rawEvent: {
					logIndex: 3,
					transactionIndex: 1,
				},
				timestamp: 1700002000,
			};

			const result = parseOrder(mockOrder);

			expect(result.side).toBe(SIDE.SELL);
			expect(result.assetId).toBe(111111n);
			expect(result.shares).toBe(2100000n);
			expect(result.usdc).toBe(2000000n);
		});
	});

	describe("Edge cases", () => {
		test("should handle zero amounts", () => {
			const mockOrder = {
				block: { number: 42000000 },
				event: {
					takerAssetId: 0n,
					makerAssetId: 100n,
					makerAmountFilled: 0n,
					takerAmountFilled: 0n,
					takerOrderMaker: "0xdddddddddddddddddddddddddddddddddddddddd",
				},
				rawEvent: {
					logIndex: 0,
					transactionIndex: 0,
				},
				timestamp: 1600000000,
			};

			const result = parseOrder(mockOrder);

			expect(result.shares).toBe(0n);
			expect(result.usdc).toBe(0n);
			expect(result.side).toBe(SIDE.BUY);
		});

		test("should preserve log and transaction indices", () => {
			const mockOrder = {
				block: { number: 43000000 },
				event: {
					takerAssetId: 0n,
					makerAssetId: 500n,
					makerAmountFilled: 100n,
					takerAmountFilled: 95n,
					takerOrderMaker: "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				},
				rawEvent: {
					logIndex: 999,
					transactionIndex: 888,
				},
				timestamp: 1700003000,
			};

			const result = parseOrder(mockOrder);

			expect(result.logIndex).toBe(999);
			expect(result.transactionIndex).toBe(888);
		});

		test("should handle maximum safe integer values", () => {
			const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);

			const mockOrder = {
				block: { number: Number.MAX_SAFE_INTEGER },
				event: {
					takerAssetId: 0n,
					makerAssetId: maxSafe,
					makerAmountFilled: maxSafe,
					takerAmountFilled: maxSafe,
					takerOrderMaker: "0xffffffffffffffffffffffffffffffffffffffff",
				},
				rawEvent: {
					logIndex: Number.MAX_SAFE_INTEGER,
					transactionIndex: Number.MAX_SAFE_INTEGER,
				},
				timestamp: Number.MAX_SAFE_INTEGER,
			};

			const result = parseOrder(mockOrder);

			expect(result.shares).toBe(maxSafe);
			expect(result.usdc).toBe(maxSafe);
			expect(result.block).toBe(Number.MAX_SAFE_INTEGER);
		});
	});

	describe("Asset ID handling", () => {
		test("should select correct assetId for buy orders", () => {
			const mockOrder = {
				block: { number: 42000000 },
				event: {
					takerAssetId: 0n,
					makerAssetId: 12345n,
					makerAmountFilled: 100n,
					takerAmountFilled: 50n,
					takerOrderMaker: "0x1111111111111111111111111111111111111111",
				},
				rawEvent: { logIndex: 1, transactionIndex: 1 },
				timestamp: 1700000000,
			};

			const result = parseOrder(mockOrder);

			// Buy order: assetId should be makerAssetId
			expect(result.assetId).toBe(12345n);
		});

		test("should select correct assetId for sell orders", () => {
			const mockOrder = {
				block: { number: 42000000 },
				event: {
					takerAssetId: 54321n,
					makerAssetId: 12345n,
					makerAmountFilled: 100n,
					takerAmountFilled: 50n,
					takerOrderMaker: "0x2222222222222222222222222222222222222222",
				},
				rawEvent: { logIndex: 1, transactionIndex: 1 },
				timestamp: 1700000000,
			};

			const result = parseOrder(mockOrder);

			// Sell order: assetId should be takerAssetId
			expect(result.assetId).toBe(54321n);
		});
	});

	describe("Invariants", () => {
		test("should always return EVENT.ORDER kind", () => {
			const mockOrder = {
				block: { number: 1 },
				event: {
					takerAssetId: 0n,
					makerAssetId: 1n,
					makerAmountFilled: 1n,
					takerAmountFilled: 1n,
					takerOrderMaker: "0x0000000000000000000000000000000000000000",
				},
				rawEvent: { logIndex: 0, transactionIndex: 0 },
				timestamp: 0,
			};

			const result = parseOrder(mockOrder);

			expect(result.kind).toBe(EVENT.ORDER);
		});

		test("should always return valid SIDE enum", () => {
			const orders = [
				{ takerAssetId: 0n, expectedSide: SIDE.BUY },
				{ takerAssetId: 1n, expectedSide: SIDE.SELL },
				{ takerAssetId: 999999n, expectedSide: SIDE.SELL },
			];

			orders.forEach(({ takerAssetId, expectedSide }) => {
				const mockOrder = {
					block: { number: 1 },
					event: {
						takerAssetId,
						makerAssetId: 100n,
						makerAmountFilled: 100n,
						takerAmountFilled: 100n,
						takerOrderMaker: "0x0000000000000000000000000000000000000000",
					},
					rawEvent: { logIndex: 0, transactionIndex: 0 },
					timestamp: 0,
				};

				const result = parseOrder(mockOrder);
				expect(result.side).toBe(expectedSide);
			});
		});

		test("should preserve all required fields", () => {
			const mockOrder = {
				block: { number: 42118906 },
				event: {
					takerAssetId: 0n,
					makerAssetId: 123n,
					makerAmountFilled: 1000n,
					takerAmountFilled: 500n,
					takerOrderMaker: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
				},
				rawEvent: { logIndex: 5, transactionIndex: 2 },
				timestamp: 1700000000,
			};

			const result = parseOrder(mockOrder);

			// Verify all required fields are present
			const requiredFields = [
				"kind",
				"trader",
				"assetId",
				"side",
				"shares",
				"usdc",
				"block",
				"logIndex",
				"transactionIndex",
				"timestamp",
			];

			requiredFields.forEach((field) => {
				expect(result).toHaveProperty(field);
				expect(result[field]).toBeDefined();
			});
		});
	});
});
