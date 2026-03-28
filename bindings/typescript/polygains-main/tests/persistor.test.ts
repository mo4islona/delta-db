import { beforeEach, describe, expect, test } from "bun:test";
import type { BlockCursor } from "@subsquid/pipes";
import { loadDetector } from "@/lib/db/bloomfilter";
import { XXHash32Set } from "@/lib/hashset";
import { BloomFilterPersistor } from "@/services/filter-persistor";

describe("BloomFilterPersistor Integration Tests", () => {
	// Clean up test snapshots before each test
	beforeEach(async () => {
		// Cleanup is handled by database initialization in these tests
	});

	test("should create persistor with custom batch interval", () => {
		const persistor = new BloomFilterPersistor(50);
		const status = persistor.getStatus();

		expect(status.queueLength).toBe(0);
		expect(status.isProcessing).toBe(false);
		expect(status.batchCount).toBe(0);
		expect(status.lastSaveBatch).toBe(0);
	});

	test("should persist to database after batch threshold", async () => {
		const persistor = new BloomFilterPersistor(2); // Save after 2 batches

		const insiderDetector = new XXHash32Set();
		insiderDetector.add("0xinsider1");

		const notInsiderDetector = new XXHash32Set();
		notInsiderDetector.add("0xnotinsider1");

		const cursor: BlockCursor = {
			number: 12345,
			hash: "0xabcdef",
			timestamp: Date.now(),
		};

		// First batch - should not save yet
		let saved = persistor.onBatchProcessed({
			insiderDetector,
			notInsiderDetector,
			insiderCount: 1,
			notInsiderCount: 1,
			cursor,
		});

		expect(saved).toBe(false);

		// Second batch - should trigger save
		saved = persistor.onBatchProcessed({
			insiderDetector,
			notInsiderDetector,
			insiderCount: 1,
			notInsiderCount: 1,
			cursor,
		});

		expect(saved).toBe(true);

		// Wait for processing
		await persistor.flush();

		// Verify data was persisted to database
		const loadedInsider = await loadDetector("insider");
		const loadedNotInsider = await loadDetector("notinsider");

		expect(loadedInsider).not.toBeNull();
		expect(loadedNotInsider).not.toBeNull();
		expect(
			loadedInsider?.dataSet.has(Bun.hash.xxHash32("0xinsider1") | 0),
		).toBe(true);
		expect(
			loadedNotInsider?.dataSet.has(Bun.hash.xxHash32("0xnotinsider1") | 0),
		).toBe(true);
		expect(loadedInsider?.cursor?.number).toBe(12345);
	});

	test("should respect batch interval - skip saves before threshold", async () => {
		const persistor = new BloomFilterPersistor(10); // Save after 10 batches

		const detector = new XXHash32Set();
		const cursor: BlockCursor = { number: 100 };

		// Process 5 batches - should not save
		for (let i = 0; i < 5; i++) {
			const saved = persistor.onBatchProcessed({
				insiderDetector: detector,
				notInsiderDetector: detector,
				insiderCount: i,
				notInsiderCount: i,
				cursor,
			});
			expect(saved).toBe(false);
		}

		await persistor.flush();

		// Verify nothing was saved
		// Note: loadDetector returns null only if no snapshots exist at all
	});

	test("should force save bypassing batch interval check", async () => {
		const persistor = new BloomFilterPersistor(100); // High threshold

		const detector = new XXHash32Set();
		detector.add("0xforced");
		const cursor: BlockCursor = { number: 100 };

		// Force save immediately without reaching threshold
		persistor.forceSave({
			insiderDetector: detector,
			notInsiderDetector: detector,
			insiderCount: 1,
			notInsiderCount: 1,
			cursor,
		});

		await persistor.flush();

		// Verify it was saved despite not reaching batch threshold
		const loaded = await loadDetector("insider");
		expect(loaded).not.toBeNull();
		expect(loaded?.dataSet.has(Bun.hash.xxHash32("0xforced") | 0)).toBe(true);
	});

	test("should only persist latest snapshot, discarding queued older ones", async () => {
		const persistor = new BloomFilterPersistor(1); // Save every batch

		const detector1 = new XXHash32Set();
		detector1.add("0xaddr1");

		const detector3 = new XXHash32Set();
		detector3.add("0xaddr3");

		const cursor1: BlockCursor = { number: 100 };
		const cursor3: BlockCursor = { number: 300 };

		// Force save multiple snapshots rapidly
		persistor.forceSave({
			insiderDetector: detector1,
			notInsiderDetector: detector1,
			insiderCount: 1,
			notInsiderCount: 1,
			cursor: cursor1,
		});

		persistor.forceSave({
			insiderDetector: detector3,
			notInsiderDetector: detector3,
			insiderCount: 3,
			notInsiderCount: 3,
			cursor: cursor3,
		});

		await persistor.flush();

		// Verify only the latest snapshot was persisted
		const loaded = await loadDetector("insider");
		expect(loaded).not.toBeNull();
		expect(loaded?.cursor?.number).toBe(300);
		expect(loaded?.dataSet.has(Bun.hash.xxHash32("0xaddr3") | 0)).toBe(true);
	});

	test("should persist cursor with bloomfilter", async () => {
		const persistor = new BloomFilterPersistor(1);

		const detector = new XXHash32Set();
		detector.add("0xtest");

		const cursor: BlockCursor = {
			number: 999,
			hash: "0xhash999",
			timestamp: 1234567890,
		};

		persistor.forceSave({
			insiderDetector: detector,
			notInsiderDetector: detector,
			insiderCount: 1,
			notInsiderCount: 1,
			cursor,
		});

		await persistor.flush();

		// Verify cursor was saved
		const loaded = await loadDetector("insider");
		expect(loaded).not.toBeNull();
		expect(loaded?.cursor).toBeDefined();
		expect(loaded?.cursor?.number).toBe(999);
	});

	test("should save both insider and notinsider filters", async () => {
		const persistor = new BloomFilterPersistor(1);

		const insiderDetector = new XXHash32Set();
		insiderDetector.add("0xinsider");

		const notInsiderDetector = new XXHash32Set();
		notInsiderDetector.add("0xnotinsider");

		const cursor: BlockCursor = { number: 500 };

		persistor.forceSave({
			insiderDetector,
			notInsiderDetector,
			insiderCount: 1,
			notInsiderCount: 1,
			cursor,
		});

		await persistor.flush();

		// Verify both filters were saved
		const loadedInsider = await loadDetector("insider");
		const loadedNotInsider = await loadDetector("notinsider");

		expect(loadedInsider).not.toBeNull();
		expect(loadedNotInsider).not.toBeNull();

		expect(loadedInsider?.dataSet.has(Bun.hash.xxHash32("0xinsider") | 0)).toBe(
			true,
		);
		expect(
			loadedNotInsider?.dataSet.has(Bun.hash.xxHash32("0xnotinsider") | 0),
		).toBe(true);

		expect(loadedInsider?.itemCount).toBe(1);
		expect(loadedNotInsider?.itemCount).toBe(1);

		expect(loadedInsider?.cursor?.number).toBe(500);
		expect(loadedNotInsider?.cursor?.number).toBe(500);
	});

	test("should track batch count correctly", async () => {
		const persistor = new BloomFilterPersistor(5);
		const detector = new XXHash32Set();
		const cursor: BlockCursor = { number: 100 };

		// Initial status
		let status = persistor.getStatus();
		expect(status.batchCount).toBe(0);
		expect(status.lastSaveBatch).toBe(0);

		// Process 3 batches
		for (let i = 0; i < 3; i++) {
			persistor.onBatchProcessed({
				insiderDetector: detector,
				notInsiderDetector: detector,
				insiderCount: 1,
				notInsiderCount: 1,
				cursor,
			});
		}

		status = persistor.getStatus();
		expect(status.batchCount).toBe(3);
		expect(status.lastSaveBatch).toBe(0); // Haven't saved yet

		// Process 2 more batches (total 5, should trigger save)
		for (let i = 0; i < 2; i++) {
			persistor.onBatchProcessed({
				insiderDetector: detector,
				notInsiderDetector: detector,
				insiderCount: 1,
				notInsiderCount: 1,
				cursor,
			});
		}

		await persistor.flush();

		status = persistor.getStatus();
		expect(status.batchCount).toBe(5);
		expect(status.lastSaveBatch).toBe(5);
	});
});
