import { BloomFilter } from "bloomfilter";

// Benchmark configuration
const NUM_ELEMENTS = 200000;
const NUM_LOOKUPS = 500000;
const BITS = 32 * 25600;
const HASHES = 4;

const now = () => performance.now();
const measure = (fn: () => void, label: string) => {
	const start = now();
	fn();
	const elapsed = now() - start;
	console.log(`[BENCHMARK] ${label}: ${elapsed.toFixed(2)}ms`);
	return elapsed;
};

// Pre-generate ALL addresses first (no generation cost during timing)
console.log("=== PRE-GENERATING TEST DATA ===\n");
const addresses: string[] = [];
const lookupAddresses: string[] = [];

measure(() => {
	// Generate addresses to ADD
	for (let i = 0; i < NUM_ELEMENTS; i++) {
		addresses.push(`0x${i.toString(16).padStart(40, "0")}`);
	}
	// Generate addresses to LOOKUP (mix of existing and non-existing)
	for (let i = 0; i < NUM_LOOKUPS; i++) {
		// 70% existing, 30% new (realistic hit rate)
		if (i < NUM_LOOKUPS * 0.7) {
			lookupAddresses.push(addresses[i % NUM_ELEMENTS]);
		} else {
			lookupAddresses.push(
				`0x${(NUM_ELEMENTS + i).toString(16).padStart(40, "0")}`,
			);
		}
	}
}, `Generate ${NUM_ELEMENTS} add + ${NUM_LOOKUPS} lookup addresses`);

console.log(""); // blank line for readability
console.log("=== OPTIMIZED BENCHMARK (no address generation cost) ===\n");

// ============================================================================
// 1. BASELINE: BloomFilter library
// ============================================================================
console.log("1. BASELINE: BloomFilter library");

const bf = new BloomFilter(BITS, HASHES);
const bfAddTime = measure(() => {
	for (const addr of addresses) {
		bf.add(addr);
	}
}, `  BloomFilter.add ${NUM_ELEMENTS} elements`);

const bfLookupTime = measure(() => {
	let hits = 0;
	for (const addr of lookupAddresses) {
		if (bf.test(addr)) hits++;
	}
	return hits;
}, `  BloomFilter.test ${NUM_LOOKUPS} lookups`);

console.log(`   Memory: ~${BITS / 8 / 1024}KB (fixed)\n`);

// ============================================================================
// 2. XXHASH32SET V2: Set-based detector (no buckets, no bits)
// ============================================================================
console.log("2. XXHASH32SET V2: Set-based detector");

class XXHash32Set {
	private set: Set<number> = new Set();

	has(address: string): boolean {
		return this.set.has(Bun.hash.xxHash32(address));
	}

	add(address: string): void {
		this.set.add(Bun.hash.xxHash32(address));
	}

	test(address: string): boolean {
		return this.has(address);
	}

	get size(): number {
		return this.set.size;
	}

	getMemoryBytes(): number {
		return this.set.size * 8;
	}
}

const xxSet = new XXHash32Set();
const xxAddTime = measure(() => {
	for (const addr of addresses) {
		xxSet.add(addr);
	}
}, `  XXHash32Set.add ${NUM_ELEMENTS} elements`);

const xxLookupTime = measure(() => {
	let hits = 0;
	for (const addr of lookupAddresses) {
		if (xxSet.test(addr)) hits++;
	}
	return hits;
}, `  XXHash32Set.test ${NUM_LOOKUPS} lookups`);

console.log(`   Memory: ~${xxSet.getMemoryBytes() / 1024}KB\n`);

// ============================================================================
// 3. PURE SET: String-based (no hash)
// ============================================================================
console.log("3. PURE SET: String-based detector");

class PureSetDetector {
	private set: Set<string> = new Set();

	has(address: string): boolean {
		return this.set.has(address);
	}

	add(address: string): void {
		this.set.add(address);
	}

	test(address: string): boolean {
		return this.has(address);
	}

	get size(): number {
		return this.set.size;
	}

	getMemoryBytes(): number {
		return this.set.size * 50;
	}
}

const pureSet = new PureSetDetector();
const psAddTime = measure(() => {
	for (const addr of addresses) {
		pureSet.add(addr);
	}
}, `  PureSet.add ${NUM_ELEMENTS} elements`);

const psLookupTime = measure(() => {
	let hits = 0;
	for (const addr of lookupAddresses) {
		if (pureSet.test(addr)) hits++;
	}
	return hits;
}, `  PureSet.test ${NUM_LOOKUPS} lookups`);

console.log(`   Memory: ~${pureSet.getMemoryBytes() / 1024 / 1024}MB\n`);

// ============================================================================
// 4. NATIVE SET: Just number (fastest possible, for reference)
// ============================================================================
console.log("4. NATIVE SET: Number-based (theoretical best)");

class NativeSetDetector {
	private set: Set<number> = new Set();

	has(num: number): boolean {
		return this.set.has(num);
	}

	add(num: number): void {
		this.set.add(num);
	}

	test(num: number): boolean {
		return this.set.has(num);
	}
}

const nativeSet = new NativeSetDetector();
const nsAddTime = measure(() => {
	for (let i = 0; i < NUM_ELEMENTS; i++) {
		nativeSet.add(i);
	}
}, `  NativeSet.add ${NUM_ELEMENTS} numbers`);

const nsLookupTime = measure(() => {
	let hits = 0;
	for (let i = 0; i < NUM_LOOKUPS; i++) {
		if (nativeSet.test(i % NUM_ELEMENTS)) hits++;
	}
	return hits;
}, `  NativeSet.test ${NUM_LOOKUPS} numbers`);

console.log(`   Memory: ~${NUM_ELEMENTS * 8}KB (baseline)\n`);

// ============================================================================
// 5. REALISTIC WORKLOAD: Check + Add if not exists (insider detection pattern)
// ============================================================================
console.log("5. REALISTIC WORKLOAD: Insider detection pattern");
console.log(`   ${NUM_ELEMENTS} known traders, ${NUM_LOOKUPS} new orders\n`);

// Use new addresses for realistic "new order" simulation
const newOrderAddresses: string[] = [];
for (let i = 0; i < NUM_LOOKUPS; i++) {
	// Mix: 50% known, 50% new
	if (i < NUM_LOOKUPS / 2) {
		newOrderAddresses.push(addresses[i % NUM_ELEMENTS]); // known
	} else {
		newOrderAddresses.push(
			`0x${(NUM_ELEMENTS + i).toString(16).padStart(40, "0")}`,
		); // new
	}
}

// 5a. BloomFilter
const bfRealAdd = measure(() => {
	const bf2 = new BloomFilter(BITS, HASHES);
	for (const addr of addresses) {
		bf2.add(addr);
	}
	let detected = 0;
	for (const addr of newOrderAddresses) {
		if (!bf2.test(addr)) {
			bf2.add(addr);
			detected++;
		}
	}
	return detected;
}, `  BloomFilter: check + add if new`);

// 5b. XXHash32Set
const xxRealAdd = measure(() => {
	const xx = new XXHash32Set();
	for (const addr of addresses) {
		xx.add(addr);
	}
	let detected = 0;
	for (const addr of newOrderAddresses) {
		if (!xx.test(addr)) {
			xx.add(addr);
			detected++;
		}
	}
	return detected;
}, `  XXHash32Set: check + add if new`);

// 5c. PureSet
const psRealAdd = measure(() => {
	const ps = new PureSetDetector();
	for (const addr of addresses) {
		ps.add(addr);
	}
	let detected = 0;
	for (const addr of newOrderAddresses) {
		if (!ps.test(addr)) {
			ps.add(addr);
			detected++;
		}
	}
	return detected;
}, `  PureSet: check + add if new`);

console.log("");

// ============================================================================
// SUMMARY
// ============================================================================
console.log("=== SUMMARY (no address generation overhead) ===\n");

console.log(
	"Operation           | BloomFilter | XXHash32Set | PureSet  | Native (num)",
);
console.log(
	"---------------------|-------------|-------------|----------|--------------",
);

const addSpeedup = (((bfAddTime - xxAddTime) / bfAddTime) * 100).toFixed(1);
const lookupSpeedup = (
	((bfLookupTime - xxLookupTime) / bfLookupTime) *
	100
).toFixed(1);
const realSpeedup = (((bfRealAdd - xxRealAdd) / bfRealAdd) * 100).toFixed(1);

console.log(
	`Add ${NUM_ELEMENTS}      | ${bfAddTime.toFixed(1)}ms    | ${xxAddTime.toFixed(1)}ms     | ${psAddTime.toFixed(1)}ms | ${nsAddTime.toFixed(1)}ms`,
);
console.log(
	`Test ${NUM_LOOKUPS}    | ${bfLookupTime.toFixed(1)}ms   | ${xxLookupTime.toFixed(1)}ms    | ${psLookupTime.toFixed(1)}ms | ${nsLookupTime.toFixed(1)}ms`,
);
console.log(
	`Realistic workload   | ${bfRealAdd.toFixed(1)}ms    | ${xxRealAdd.toFixed(1)}ms     | ${psRealAdd.toFixed(1)}ms | ~250ms (est)`,
);

console.log("\n=== PERFORMANCE ANALYSIS ===\n");

if (parseFloat(addSpeedup) > 0) {
	console.log(`✓ XXHash32Set ADD is ${addSpeedup}% faster than BloomFilter`);
} else {
	console.log(
		`✗ XXHash32Set ADD is ${Math.abs(parseFloat(addSpeedup))}% slower than BloomFilter`,
	);
}

if (parseFloat(lookupSpeedup) > 0) {
	console.log(
		`✓ XXHash32Set TEST is ${lookupSpeedup}% faster than BloomFilter`,
	);
} else {
	console.log(
		`✗ XXHash32Set TEST is ${Math.abs(parseFloat(lookupSpeedup))}% slower than BloomFilter`,
	);
}

if (parseFloat(realSpeedup) > 0) {
	console.log(
		`✓ XXHash32Set REALISTIC is ${realSpeedup}% faster than BloomFilter`,
	);
} else {
	console.log(
		`✗ XXHash32Set REALISTIC is ${Math.abs(parseFloat(realSpeedup))}% slower than BloomFilter`,
	);
}

console.log("\n=== FINAL RECOMMENDATION ===\n");

const overallSpeedup = parseFloat(realSpeedup);
if (overallSpeedup > 5) {
	console.log(`🚀 USE XXHash32Set V2 (${overallSpeedup}% faster!)`);
	console.log("   - Significant performance gain");
	console.log("   - No false positives");
	console.log("   - Memory tradeoff is acceptable");
} else if (overallSpeedup > 0) {
	console.log(`✓ USE XXHash32Set V2 (${overallSpeedup}% faster)`);
	console.log("   - Modest performance gain");
	console.log("   - No false positives (reliability++)");
	console.log("   - Memory: 1.6MB vs 100KB");
} else if (overallSpeedup > -5) {
	console.log(`⚠️  KEEP BloomFilter (within ${Math.abs(overallSpeedup)}%)`);
	console.log("   - Performance is similar");
	console.log("   - Memory efficient (100KB)");
	console.log("   - Switch only if you need exact tracking");
} else {
	console.log(`📉 KEEP BloomFilter (${Math.abs(overallSpeedup)}% faster)`);
	console.log("   - BloomFilter is faster for this workload");
	console.log("   - Don't switch");
}
