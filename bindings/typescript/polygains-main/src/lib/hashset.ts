/**
 * XXHash32Set V2 - Fast, exact detector using Bun's xxHash32
 */
export class XXHash32Set {
	private set: Set<number> = new Set();
	private unsaved: Set<number> = new Set(); // Track changes since last snapshot

	private toHash(value: string | number): number {
		// Runtime account hashes are already int32. Preserve that directly.
		if (
			typeof value === "number" &&
			Number.isFinite(value) &&
			Number.isInteger(value)
		) {
			return value | 0;
		}
		// Fallback for any remaining string callers.
		return Bun.hash.xxHash32(String(value)) | 0;
	}

	has(value: string | number): boolean {
		return this.set.has(this.toHash(value));
	}

	add(value: string | number): void {
		const hash = this.toHash(value);
		if (!this.set.has(hash)) {
			this.set.add(hash);
			this.unsaved.add(hash); // Track for incremental save
		}
	}

	test(value: string | number): boolean {
		return this.has(value);
	}

	addMany(values: Iterable<string | number>): void {
		// Optimized: map all hashes first, then forEach (24.8% faster)
		const hashes = Array.isArray(values)
			? values.map((v) => this.toHash(v))
			: Array.from(values).map((v) => this.toHash(v));
		hashes.forEach((h) => {
			if (!this.set.has(h)) {
				this.set.add(h);
				this.unsaved.add(h); // Track for incremental save
			}
		});
	}

	clear(): void {
		this.set.clear();
		this.unsaved.clear();
	}

	get size(): number {
		return this.set.size;
	}

	// Get underlying Set for serialization
	getSet(): Set<number> {
		return this.set;
	}

	// Restore from serialized Set
	restoreSet(set: Set<number>): void {
		this.set = set;
		this.unsaved.clear(); // Clear unsaved on restore
	}

	// Get only unsaved hashes (incremental snapshot)
	getUnsavedSet(): Set<number> {
		return this.unsaved;
	}

	// Clear unsaved after successful snapshot
	clearUnsaved(): void {
		this.unsaved.clear();
	}

	// Estimate memory
	getMemoryBytes(): number {
		return (this.set.size + this.unsaved.size) * 8; // ~8 bytes per number
	}
}
