import { XXHash32Set } from "@/lib/hashset";

/**
 * Drop-in replacement for BloomFilter with same API
 * Use this to replace existing detectors
 */
export class FastDetector {
	private detector: XXHash32Set;

	constructor() {
		this.detector = new XXHash32Set();
	}

	has(value: string | number): boolean {
		return this.detector.has(value);
	}

	add(value: string | number): void {
		this.detector.add(value);
	}

	addMany(values: Iterable<string | number>): void {
		this.detector.addMany(values);
	}

	getDetector(): XXHash32Set {
		return this.detector;
	}

	// For snapshot compatibility
	getFilter(): XXHash32Set {
		return this.detector;
	}
}

// Drop-in replacements for existing classes
export class InsiderDetector extends FastDetector {}
export class NotInsiderDetector extends FastDetector {}
export { XXHash32Set };
