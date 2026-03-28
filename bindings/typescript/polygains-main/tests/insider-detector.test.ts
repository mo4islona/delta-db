import { describe, expect, test } from "bun:test";
import {
	InsiderDetector,
	NotInsiderDetector,
	XXHash32Set,
} from "@/services/detector";

describe("XXHash32Set", () => {
	test("stores and matches hashes exactly", () => {
		const set = new XXHash32Set();
		const wallet = "0x1234567890abcdef1234567890abcdef12345678";

		expect(set.has(wallet)).toBe(false);
		set.add(wallet);
		expect(set.has(wallet)).toBe(true);
	});

	test("treats uint32 and int32 numeric views as the same hash", () => {
		const set = new XXHash32Set();
		const signed = -1334490829;
		const unsigned = 2960476467;

		set.add(signed);

		expect(set.has(signed)).toBe(true);
		expect(set.has(unsigned)).toBe(true);
	});

	test("supports bulk adds", () => {
		const set = new XXHash32Set();
		const wallets = [
			"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"0xcccccccccccccccccccccccccccccccccccccccc",
		];

		set.addMany(wallets);

		for (const wallet of wallets) {
			expect(set.has(wallet)).toBe(true);
		}
	});
});

describe("Detector v2 wrappers", () => {
	test("keeps insider/non-insider detector state isolated", () => {
		const insider = new InsiderDetector();
		const notInsider = new NotInsiderDetector();
		const wallet = "0x1111111111111111111111111111111111111111";

		insider.add(wallet);
		expect(insider.has(wallet)).toBe(true);
		expect(notInsider.has(wallet)).toBe(false);
	});
});
