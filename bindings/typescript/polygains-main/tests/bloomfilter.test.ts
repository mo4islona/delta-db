import { beforeEach, describe, expect, mock, test } from "bun:test";
import { BloomFilter } from "bloomfilter";
import {
	type BloomFilterId,
	type BloomFilterInternals,
	deleteBloomFilter,
	getBloomFilterMetadata,
	loadBloomFilter,
	saveBloomFilter,
} from "@/lib/db/bloomfilter";

// Mock the database module
const mockDb = {
	insert: mock(() => ({
		values: mock(() => ({
			onConflictDoUpdate: mock(async () => {}),
		})),
	})),
	query: {
		bloomfilterSnapshots: {
			findFirst: mock(async () => null),
		},
	},
	delete: mock(() => ({
		where: mock(async () => {}),
	})),
};

mock.module("@/lib/db/init", () => ({
	db: mockDb,
}));

describe("BloomFilter Persistence", () => {
	beforeEach(() => {
		// Reset all mocks before each test
		mockDb.insert.mockClear();
		mockDb.query.bloomfilterSnapshots.findFirst.mockClear();
		mockDb.delete.mockClear();
	});

	describe("saveBloomFilter", () => {
		test("should serialize Int32Array to Buffer efficiently", async () => {
			const filter = new BloomFilter(32 * 25600, 4);
			filter.add("0x1234567890abcdef");

			await saveBloomFilter("insider", filter, 1);

			expect(mockDb.insert).toHaveBeenCalledTimes(1);
			const insertCall = mockDb.insert.mock.results[0].value;
			expect(insertCall.values).toHaveBeenCalledTimes(1);

			const values = insertCall.values.mock.calls[0][0];
			expect(values.id).toBe("insider");
			expect(values.bits).toBe(32 * 25600);
			expect(values.hashes).toBe(4);
			expect(values.itemCount).toBe(1);
			expect(values.buckets).toBeInstanceOf(Buffer);
			expect(values.buckets.length).toBe(25600 * 4); // 32-bit integers = 4 bytes each
		});

		test("should handle empty bloom filter", async () => {
			const filter = new BloomFilter(1024, 3);

			await saveBloomFilter("notinsider", filter, 0);

			const insertCall = mockDb.insert.mock.results[0].value;
			const values = insertCall.values.mock.calls[0][0];

			expect(values.itemCount).toBe(0);
			expect(values.buckets).toBeInstanceOf(Buffer);
		});

		test("should use upsert semantics for existing filters", async () => {
			const filter = new BloomFilter(1024, 3);

			await saveBloomFilter("insider", filter, 100);

			const insertCall = mockDb.insert.mock.results[0].value;
			const valuesCall = insertCall.values.mock.results[0].value;

			expect(valuesCall.onConflictDoUpdate).toHaveBeenCalledTimes(1);
			const upsertConfig = valuesCall.onConflictDoUpdate.mock.calls[0][0];

			expect(upsertConfig.target).toBeDefined();
			expect(upsertConfig.set).toMatchObject({
				bits: 1024,
				hashes: 3,
				itemCount: 100,
			});
		});
	});

	describe("loadBloomFilter", () => {
		test("should return null when snapshot not found", async () => {
			mockDb.query.bloomfilterSnapshots.findFirst.mockResolvedValueOnce(null);

			const result = await loadBloomFilter("insider");

			expect(result).toBeNull();
			expect(mockDb.query.bloomfilterSnapshots.findFirst).toHaveBeenCalledTimes(
				1,
			);
		});

		test("should reconstruct BloomFilter from binary data", async () => {
			const originalFilter = new BloomFilter(1024, 3);
			originalFilter.add("0xdeadbeef");
			originalFilter.add("0xcafebabe");

			const buckets = (originalFilter as unknown as BloomFilterInternals)
				.buckets;
			const buffer = Buffer.from(buckets.buffer);

			mockDb.query.bloomfilterSnapshots.findFirst.mockResolvedValueOnce({
				id: "insider",
				buckets: buffer,
				bits: 1024,
				hashes: 3,
				itemCount: 2,
				updatedAt: Date.now(),
			});

			const loaded = await loadBloomFilter("insider");
			const loadedFilter = loaded?.filter;

			expect(loadedFilter).not.toBeNull();
			expect(loadedFilter?.test("0xdeadbeef")).toBe(true);
			expect(loadedFilter?.test("0xcafebabe")).toBe(true);
			expect(loadedFilter?.test("0x00000000")).toBe(false);
		});

		test("should preserve bloom filter properties across save/load cycle", async () => {
			const testAddresses = [
				"0x1111111111111111111111111111111111111111",
				"0x2222222222222222222222222222222222222222",
				"0x3333333333333333333333333333333333333333",
			];

			const originalFilter = new BloomFilter(32 * 25600, 4);
			for (const addr of testAddresses) {
				originalFilter.add(addr);
			}

			const buckets = (originalFilter as unknown as { buckets: Int32Array })
				.buckets;
			const buffer = Buffer.from(buckets.buffer);

			mockDb.query.bloomfilterSnapshots.findFirst.mockResolvedValueOnce({
				id: "insider",
				buckets: buffer,
				bits: 32 * 25600,
				hashes: 4,
				itemCount: 3,
				updatedAt: Date.now(),
			});

			const loaded = await loadBloomFilter("insider");
			const loadedFilter = loaded?.filter;

			// All original items should test positive
			for (const addr of testAddresses) {
				expect(loadedFilter?.test(addr)).toBe(true);
			}

			// Random address should likely test negative (with acceptable false positive rate)
			const randomAddr = "0x9999999999999999999999999999999999999999";
			const falsePositive = loadedFilter?.test(randomAddr);
			// With good parameters, false positive rate should be low, but we can't guarantee
			// So we just verify the filter is working, not the specific result
			expect(typeof falsePositive).toBe("boolean");
		});
	});

	describe("getBloomFilterMetadata", () => {
		test("should return metadata without loading full buckets", async () => {
			const mockMetadata = {
				id: "insider" as BloomFilterId,
				bits: 819200,
				hashes: 4,
				itemCount: 1500,
				updatedAt: Date.now(),
			};

			mockDb.query.bloomfilterSnapshots.findFirst.mockResolvedValueOnce(
				mockMetadata,
			);

			const metadata = await getBloomFilterMetadata("insider");

			expect(metadata).toEqual(mockMetadata);
			expect(mockDb.query.bloomfilterSnapshots.findFirst).toHaveBeenCalledTimes(
				1,
			);

			// Verify columns parameter excludes buckets (check the call)
			const call = mockDb.query.bloomfilterSnapshots.findFirst.mock.calls[0][0];
			expect(call.columns).toBeDefined();
			expect(call.columns.buckets).toBeUndefined();
		});
	});

	describe("deleteBloomFilter", () => {
		test("should delete bloom filter snapshot", async () => {
			await deleteBloomFilter("notinsider");

			expect(mockDb.delete).toHaveBeenCalledTimes(1);
			const deleteCall = mockDb.delete.mock.results[0].value;
			expect(deleteCall.where).toHaveBeenCalledTimes(1);
		});
	});

	describe("Binary storage efficiency", () => {
		test("should use less space than JSON serialization", () => {
			const filter = new BloomFilter(32 * 25600, 4);

			// Populate with enough data to ensure numbers are large/frequent enough
			// 10k items should flip many bits
			for (let i = 0; i < 10000; i++) {
				filter.add(`0xaddress${i}`);
			}

			const buckets = (filter as unknown as BloomFilterInternals).buckets;

			const binaryBuffer = Buffer.from(buckets.buffer);
			const jsonString = JSON.stringify(Array.from(buckets));

			// Binary should be smaller than JSON when data is present
			// (JSON of large numbers like -1234567890 is much larger than 4 bytes)
			expect(binaryBuffer.length).toBeLessThan(jsonString.length);
			expect(binaryBuffer.length).toBe(25600 * 4); // Exact size: 102,400 bytes
		});
	});
});
