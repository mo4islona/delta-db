import { Heap } from "heap-js";
import { FIFTEEN_MINUTES } from "@/lib/const";
import type { WindowBufferItem } from "@/lib/types";

// --- 1. Optimized Window Buffer ---
export class WindowBuffer<T extends WindowBufferItem> extends Map<string, T> {
	private minHeap: Heap<T>;
	private getTimestamp: (item: T) => number;
	private getId: (item: T) => string;
	private deletedKeys = new Set<string>();

	constructor(
		private windowSize = FIFTEEN_MINUTES,
		getTimestamp?: (item: T) => number,
		getId?: (item: T) => string,
	) {
		super();

		this.getTimestamp = getTimestamp ?? ((item: T) => item.userStats.firstSeen);
		this.getId = getId ?? ((item: T) => item.id);

		this.minHeap = new Heap(
			(a, b) => this.getTimestamp(a) - this.getTimestamp(b),
		);
		this.minHeap.init([]);
	}

	override set(key: string, value: T): this {
		if (!this.has(key)) {
			this.minHeap.push(value);
		}

		this.deletedKeys.delete(key);
		super.set(key, value);
		return this;
	}

	override delete(key: string): boolean {
		const existed = super.delete(key);
		if (existed) {
			this.deletedKeys.add(key);
		}
		return existed;
	}

	flush(currentTimestamp: number): Record<string, T> {
		const flushed: Record<string, T> = {};

		if (currentTimestamp === undefined || Number.isNaN(currentTimestamp)) {
			return flushed;
		}

		while (
			this.minHeap.length > 0 &&
			currentTimestamp - this.getTimestamp(this.minHeap.peek() as T) >=
				this.windowSize
		) {
			const expiredItem = this.minHeap.pop() as T;
			const key = this.getId(expiredItem);

			if (this.deletedKeys.has(key)) {
				this.deletedKeys.delete(key);
				continue;
			}

			// Verify this is still the active item in the Map
			// If it's not, it's a stale heap entry from a previous set() call
			if (super.get(key) !== expiredItem) {
				continue;
			}

			flushed[key] = expiredItem;
			super.delete(key);
		}

		return flushed;
	}
}
