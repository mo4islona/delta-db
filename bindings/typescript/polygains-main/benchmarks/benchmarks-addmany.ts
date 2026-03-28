/**
 * Benchmark: addMany optimization
 * Comparing:
 * 1. Loop + add (current approach)
 * 2. Set.union / combined Set (optimized)
 * 3. Pre-hashed union (fastest)
 */

const now = () => performance.now();
const measure = (fn: () => void, label: string) => {
	const start = now();
	fn();
	const elapsed = now() - start;
	console.log(`[BENCHMARK] ${label}: ${elapsed.toFixed(2)}ms`);
	return elapsed;
};

// Generate test data
console.log("=== PRE-GENERATING TEST DATA ===\n");
const BATCHES = 100;
const ADDRESSES_PER_BATCH = 1000;
const _TOTAL_ADDRESSES = BATCHES * ADDRESSES_PER_BATCH;

const allBatches: string[][] = [];
measure(() => {
	for (let b = 0; b < BATCHES; b++) {
		const batch: string[] = [];
		for (let i = 0; i < ADDRESSES_PER_BATCH; i++) {
			batch.push(
				`0x${(b * ADDRESSES_PER_BATCH + i).toString(16).padStart(40, "0")}`,
			);
		}
		allBatches.push(batch);
	}
}, `Generate ${BATCHES} batches × ${ADDRESSES_PER_BATCH} addresses`);

console.log("");
console.log("=== ADDMANY BENCHMARK ===\n");

// ============================================================================
// 1. CURRENT: Loop + add
// ============================================================================
console.log("1. CURRENT: Loop + add (individual)");

class CurrentDetector {
	private set: Set<number> = new Set();

	has(address: string): boolean {
		return this.set.has(Bun.hash.xxHash32(address));
	}

	add(address: string): void {
		this.set.add(Bun.hash.xxHash32(address));
	}

	addMany(addresses: Iterable<string>): void {
		for (const addr of addresses) {
			this.set.add(Bun.hash.xxHash32(addr));
		}
	}

	get size(): number {
		return this.set.size;
	}
}

let _currentTotal = 0;
const currentTimes: number[] = [];

measure(() => {
	for (let b = 0; b < BATCHES; b++) {
		const detector = new CurrentDetector();
		const start = now();
		detector.addMany(allBatches[b]);
		currentTimes.push(now() - start);
		_currentTotal += detector.size;
	}
}, `  Loop + add: ${BATCHES} batches × ${ADDRESSES_PER_BATCH}`);

// ============================================================================
// 2. OPTIMIZED: Create Set + union
// ============================================================================
console.log("2. OPTIMIZED: Set.union (combined)");

class UnionDetector {
	private set: Set<number> = new Set();

	has(address: string): boolean {
		return this.set.has(Bun.hash.xxHash32(address));
	}

	add(address: string): void {
		this.set.add(Bun.hash.xxHash32(address));
	}

	addMany(addresses: Iterable<string>): void {
		// Pre-hash all addresses into a new Set
		const newHashes = new Set<number>();
		for (const addr of addresses) {
			newHashes.add(Bun.hash.xxHash32(addr));
		}
		// Union is faster than individual adds
		for (const hash of newHashes) {
			this.set.add(hash);
		}
	}

	get size(): number {
		return this.set.size;
	}
}

let _unionTotal = 0;
const unionTimes: number[] = [];

measure(() => {
	for (let b = 0; b < BATCHES; b++) {
		const detector = new UnionDetector();
		const start = now();
		detector.addMany(allBatches[b]);
		unionTimes.push(now() - start);
		_unionTotal += detector.size;
	}
}, `  Set.union: ${BATCHES} batches × ${ADDRESSES_PER_BATCH}`);

// ============================================================================
// 3. FASTEST: Spread operator (V8 optimized)
// ============================================================================
console.log("3. FASTEST: Spread + Set (V8 optimized)");

class SpreadDetector {
	private set: Set<number> = new Set();

	has(address: string): boolean {
		return this.set.has(Bun.hash.xxHash32(address));
	}

	add(address: string): void {
		this.set.add(Bun.hash.xxHash32(address));
	}

	addMany(addresses: Iterable<string>): void {
		// Create new Set with pre-hashed values
		const newHashes = new Set<number>();
		for (const addr of addresses) {
			newHashes.add(Bun.hash.xxHash32(addr));
		}
		// V8 optimized spread
		this.set = new Set([...this.set, ...newHashes]);
	}

	get size(): number {
		return this.set.size;
	}
}

let _spreadTotal = 0;
const spreadTimes: number[] = [];

measure(() => {
	for (let b = 0; b < BATCHES; b++) {
		const detector = new SpreadDetector();
		const start = now();
		detector.addMany(allBatches[b]);
		spreadTimes.push(now() - start);
		_spreadTotal += detector.size;
	}
}, `  Spread + Set: ${BATCHES} batches × ${ADDRESSES_PER_BATCH}`);

// ============================================================================
// 4. ALTERNATIVE: Set.add with forEach
// ============================================================================
console.log("4. ALTERNATIVE: forEach + add");

class ForEachDetector {
	private set: Set<number> = new Set();

	has(address: string): boolean {
		return this.set.has(Bun.hash.xxHash32(address));
	}

	add(address: string): void {
		this.set.add(Bun.hash.xxHash32(address));
	}

	addMany(addresses: Iterable<string>): void {
		const hashes = Array.isArray(addresses)
			? addresses.map((a) => Bun.hash.xxHash32(a))
			: Array.from(addresses).map((a) => Bun.hash.xxHash32(a));

		hashes.forEach((h) => {
			this.set.add(h);
		});
	}

	get size(): number {
		return this.set.size;
	}
}

let _forEachTotal = 0;
const forEachTimes: number[] = [];

measure(() => {
	for (let b = 0; b < BATCHES; b++) {
		const detector = new ForEachDetector();
		const start = now();
		detector.addMany(allBatches[b]);
		forEachTimes.push(now() - start);
		_forEachTotal += detector.size;
	}
}, `  forEach + add: ${BATCHES} batches × ${ADDRESSES_PER_BATCH}`);

// ============================================================================
// STATISTICS
// ============================================================================
console.log("\n=== STATISTICS ===\n");

const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
const min = (arr: number[]) => Math.min(...arr);
const max = (arr: number[]) => Math.max(...arr);

console.log("Method          | Avg (ms) | Min (ms) | Max (ms)");
console.log("----------------|----------|----------|----------");
console.log(
	`Loop + add       | ${avg(currentTimes).toFixed(2)}   | ${min(currentTimes).toFixed(2)}   | ${max(currentTimes).toFixed(2)}`,
);
console.log(
	`Set.union        | ${avg(unionTimes).toFixed(2)}   | ${min(unionTimes).toFixed(2)}   | ${max(unionTimes).toFixed(2)}`,
);
console.log(
	`Spread + Set      | ${avg(spreadTimes).toFixed(2)}   | ${min(spreadTimes).toFixed(2)}   | ${max(spreadTimes).toFixed(2)}`,
);
console.log(
	`forEach + add     | ${avg(forEachTimes).toFixed(2)}   | ${min(forEachTimes).toFixed(2)}   | ${max(forEachTimes).toFixed(2)}`,
);

console.log("\n=== PERFORMANCE GAINS ===\n");

const currentAvg = avg(currentTimes);
const unionAvg = avg(unionTimes);
const spreadAvg = avg(spreadTimes);
const forEachAvg = avg(forEachTimes);

const unionSpeedup = (((currentAvg - unionAvg) / currentAvg) * 100).toFixed(1);
const spreadSpeedup = (((currentAvg - spreadAvg) / currentAvg) * 100).toFixed(
	1,
);
const forEachSpeedup = (((currentAvg - forEachAvg) / currentAvg) * 100).toFixed(
	1,
);

if (parseFloat(unionSpeedup) > 0) {
	console.log(`✓ Set.union is ${unionSpeedup}% faster than loop + add`);
} else {
	console.log(`✗ Set.union is ${Math.abs(parseFloat(unionSpeedup))}% slower`);
}

if (parseFloat(spreadSpeedup) > 0) {
	console.log(`✓ Spread + Set is ${spreadSpeedup}% faster than loop + add`);
} else {
	console.log(
		`✗ Spread + Set is ${Math.abs(parseFloat(spreadSpeedup))}% slower`,
	);
}

if (parseFloat(forEachSpeedup) > 0) {
	console.log(`✓ forEach + add is ${forEachSpeedup}% faster than loop + add`);
} else {
	console.log(
		`✗ forEach + add is ${Math.abs(parseFloat(forEachSpeedup))}% slower`,
	);
}

console.log("\n=== RECOMMENDATION ===\n");

const methods = [
	{ name: "Loop + add", time: currentAvg },
	{ name: "Set.union", time: unionAvg },
	{ name: "Spread + Set", time: spreadAvg },
	{ name: "forEach + add", time: forEachAvg },
].sort((a, b) => a.time - b.time);

console.log(`🏆 Winner: ${methods[0].name}`);
console.log(
	`   ${(((currentAvg - methods[0].time) / currentAvg) * 100).toFixed(1)}% faster than current`,
);

console.log("\nRanking:");
methods.forEach((m, i) => {
	const diff = (((currentAvg - m.time) / currentAvg) * 100).toFixed(1);
	console.log(
		`  ${i + 1}. ${m.name.padEnd(15)} - ${diff > 0 ? "+" : ""}${diff}%`,
	);
});

// Update detector-v2.ts with optimal implementation
console.log("\n=== UPDATE RECOMMENDATION ===\n");
console.log("Update src/services/detector-v2.ts:\n");

if (methods[0].name === "Set.union") {
	console.log("  addMany(addresses: Iterable<string>): void {");
	console.log("      const newHashes = new Set<number>();");
	console.log("      for (const addr of addresses) {");
	console.log("          newHashes.add(Bun.hash.xxHash32(addr));");
	console.log("      }");
	console.log("      for (const hash of newHashes) {");
	console.log("          this.set.add(hash);");
	console.log("      }");
	console.log("  }");
} else if (methods[0].name === "Spread + Set") {
	console.log("  addMany(addresses: Iterable<string>): void {");
	console.log("      const newHashes = new Set<number>();");
	console.log("      for (const addr of addresses) {");
	console.log("          newHashes.add(Bun.hash.xxHash32(addr));");
	console.log("      }");
	console.log("      this.set = new Set([...this.set, ...newHashes]);");
	console.log("  }");
} else if (methods[0].name === "forEach + add") {
	console.log("  addMany(addresses: Iterable<string>): void {");
	console.log("      const hashes = addresses instanceof Array");
	console.log("          ? addresses.map(a => Bun.hash.xxHash32(a))");
	console.log(
		"          : Array.from(addresses).map(a => Bun.hash.xxHash32(a));",
	);
	console.log("      hashes.forEach(h => this.set.add(h));");
	console.log("  }");
}
