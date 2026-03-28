import { beforeEach, describe, expect, test } from "bun:test";
import { type TraderData, WindowBuffer } from "@/services/buffer";

describe("WindowBuffer", () => {
	let buffer: WindowBuffer<TraderData>;
	const windowSize = 900; // 15 minutes in seconds

	beforeEach(() => {
		buffer = new WindowBuffer<TraderData>(windowSize);
	});

	test("should store and retrieve items", () => {
		const trader: TraderData = {
			id: "t1",
			tokenstats: {},
			userStats: { tradeVol: 100n, tradeCount: 1, firstSeen: 1000 },
		};
		buffer.set("t1", trader);
		expect(buffer.get("t1")).toBe(trader);
		expect(buffer.size).toBe(1);
	});

	test("should flush expired items", () => {
		const t1: TraderData = {
			id: "t1",
			tokenstats: {},
			userStats: { tradeVol: 100n, tradeCount: 1, firstSeen: 1000 },
		};
		const t2: TraderData = {
			id: "t2",
			tokenstats: {},
			userStats: { tradeVol: 200n, tradeCount: 1, firstSeen: 1200 },
		};

		buffer.set("t1", t1);
		buffer.set("t2", t2);

		// Current time: 1850 -> 1850 - 1000 = 850 (< 900), nothing should flush
		let flushed = buffer.flush(1850);
		expect(Object.keys(flushed).length).toBe(0);

		// Current time: 1900 -> 1900 - 1000 = 900 (>= 900), t1 should flush
		flushed = buffer.flush(1900);
		expect(Object.keys(flushed).length).toBe(1);
		expect(flushed.t1).toBe(t1);

		// Current time: 2100 -> 2100 - 1200 = 900 (>= 900), t2 should flush
		flushed = buffer.flush(2100);
		expect(Object.keys(flushed).length).toBe(1);
		expect(flushed.t2).toBe(t2);
	});

	test("should handle lazy deletions correctly", () => {
		const t1: TraderData = {
			id: "t1",
			tokenstats: {},
			userStats: { tradeVol: 100n, tradeCount: 1, firstSeen: 1000 },
		};
		buffer.set("t1", t1);
		buffer.delete("t1");

		// Should not be in the Map
		expect(buffer.has("t1")).toBe(false);

		// Should not flush because it was deleted
		const flushed = buffer.flush(2000);
		expect(Object.keys(flushed).length).toBe(0);
	});

	test("should handle re-adding after deletion", () => {
		const t1: TraderData = {
			id: "t1",
			tokenstats: {},
			userStats: { tradeVol: 100n, tradeCount: 1, firstSeen: 1000 },
		};
		buffer.set("t1", t1);
		buffer.delete("t1");

		const t1_new: TraderData = {
			id: "t1",
			tokenstats: {},
			userStats: { tradeVol: 500n, tradeCount: 2, firstSeen: 1500 },
		};
		buffer.set("t1", t1_new);

		// Flush at 2300: 2300 - 1500 = 800 (not yet)
		let flushed = buffer.flush(2300);
		expect(Object.keys(flushed).length).toBe(0);

		// Flush at 2400: 2400 - 1500 = 900 (expired)
		flushed = buffer.flush(2400);
		expect(Object.keys(flushed).length).toBe(1);
		expect(flushed.t1).toBe(t1_new);
	});

	test("should not re-push to heap on updates", () => {
		// We can't easily check the heap directly, but we can check if it flushes twice if we are not careful
		// Actually the current implementation only pushes if !has(key).
		const t1: TraderData = {
			id: "t1",
			tokenstats: {},
			userStats: { tradeVol: 100n, tradeCount: 1, firstSeen: 1000 },
		};
		buffer.set("t1", t1);

		t1.userStats.tradeVol = 200n;
		buffer.set("t1", t1);

		const flushed = buffer.flush(2000);
		expect(Object.keys(flushed).length).toBe(1);

		const flushed2 = buffer.flush(3000);
		expect(Object.keys(flushed2).length).toBe(0);
	});

	test("should return empty if currentTimestamp is undefined or NaN", () => {
		const t1: TraderData = {
			id: "t1",
			tokenstats: {},
			userStats: { tradeVol: 100n, tradeCount: 1, firstSeen: 1000 },
		};
		buffer.set("t1", t1);

		expect(
			Object.keys(buffer.flush(undefined as unknown as number)).length,
		).toBe(0);
		expect(Object.keys(buffer.flush(NaN)).length).toBe(0);
	});

	test("BUG REPRO: should fail if timestamps are mismatched (ms vs s)", () => {
		// If system expects seconds but receives ms
		const t1: TraderData = {
			id: "t1",
			tokenstats: {},
			userStats: { tradeVol: 100n, tradeCount: 1, firstSeen: 1700000000000 }, // ms
		};
		buffer.set("t1", t1);

		// currentTimestamp is in seconds
		const flushed = buffer.flush(1700000900);
		// 1700000900 - 1700000000000 is very negative.
		expect(Object.keys(flushed).length).toBe(0);
	});
});
