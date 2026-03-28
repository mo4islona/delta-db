import { describe, expect, test } from "bun:test";
import type { AlertItem } from "../types/terminal";
import {
	calculatePnL,
	calculateTradeCost,
	calculateWinProfit,
	categoryMatches,
	createAlertKey,
	// State management
	createInitialState,
	// Strategy logic
	didStrategyWin,
	FIXED_STAKE_USD,
	// Filtering
	filterAlerts,
	formatPnL,
	getEntryPrice,
	getROI,
	// Utils
	getWinRate,
	inferInsiderWin,
	isPriceInRange,
	// Price calculations
	normalizePrice,
	normalizePriceRange,
	// Winner resolution
	parseWinnerValue,
	processAlerts,
	resolveClosedAlertWinner,
	settleTrade,
	TARGET_PAYOUT,
	winnerFilterMatches,
} from "./backtest";

// ============================================================================
// FIXTURES
// ============================================================================

const mockAlert = (overrides: Partial<AlertItem> = {}): AlertItem => ({
	price: 0.5,
	user: "test-user",
	volume: 1000,
	alert_time: Date.now() / 1000,
	market_count: 1,
	outcome: "YES",
	winner: null,
	closed: false,
	conditionId: "cond-123",
	question: "Test question?",
	tokenId: "token-456",
	market_price: 0.5,
	walletAddress: "0x123",
	category: "CRYPTO",
	...overrides,
});

// ============================================================================
// WINNER RESOLUTION TESTS
// ============================================================================

describe("parseWinnerValue", () => {
	test("parses boolean values", () => {
		expect(parseWinnerValue(true)).toBe(true);
		expect(parseWinnerValue(false)).toBe(false);
	});

	test("parses numeric values", () => {
		expect(parseWinnerValue(1)).toBe(true);
		expect(parseWinnerValue(0)).toBe(false);
		expect(parseWinnerValue(42)).toBe(true);
	});

	test("parses string values", () => {
		expect(parseWinnerValue("true")).toBe(true);
		expect(parseWinnerValue("TRUE")).toBe(true);
		expect(parseWinnerValue("yes")).toBe(true);
		expect(parseWinnerValue("1")).toBe(true);
		expect(parseWinnerValue("false")).toBe(false);
		expect(parseWinnerValue("FALSE")).toBe(false);
		expect(parseWinnerValue("no")).toBe(false);
		expect(parseWinnerValue("0")).toBe(false);
	});

	test("returns null for unknown values", () => {
		expect(parseWinnerValue("unknown")).toBeNull();
		expect(parseWinnerValue(null)).toBeNull();
		expect(parseWinnerValue(undefined)).toBeNull();
		expect(parseWinnerValue("")).toBeNull();
	});
});

describe("inferInsiderWin", () => {
	test("uses explicit winner when available", () => {
		expect(inferInsiderWin(true, 0.5)).toBe(true);
		expect(inferInsiderWin(false, 0.99)).toBe(false);
		expect(inferInsiderWin("yes", 0.01)).toBe(true);
	});

	test("infers winner from high price", () => {
		expect(inferInsiderWin(null, 0.99)).toBe(true);
		expect(inferInsiderWin(null, 0.98)).toBe(true);
		expect(inferInsiderWin(undefined, 1.0)).toBe(true);
	});

	test("infers loser from low price", () => {
		expect(inferInsiderWin(null, 0.05)).toBe(false);
		expect(inferInsiderWin(null, 0.01)).toBe(false);
		expect(inferInsiderWin(undefined, 0.0)).toBe(false);
	});

	test("returns null for ambiguous prices", () => {
		expect(inferInsiderWin(null, 0.5)).toBeNull();
		expect(inferInsiderWin(null, 0.06)).toBeNull();
		expect(inferInsiderWin(null, 0.97)).toBeNull();
	});

	test("respects allowPriceInference flag", () => {
		expect(inferInsiderWin(null, 0.99, false)).toBeNull();
		expect(inferInsiderWin(null, 0.01, false)).toBeNull();
		expect(inferInsiderWin(true, 0.5, false)).toBe(true);
	});
});

describe("resolveClosedAlertWinner", () => {
	test("uses explicit winner in market-data-only mode", () => {
		expect(resolveClosedAlertWinner(true, 0.5, true)).toBe(true);
		expect(resolveClosedAlertWinner(false, 0.99, true)).toBe(false);
	});

	test("does not infer from price in market-data-only mode", () => {
		expect(resolveClosedAlertWinner(null, 0.99, true)).toBeNull();
		expect(resolveClosedAlertWinner(null, 0.01, true)).toBeNull();
	});

	test("falls back to price inference when allowed", () => {
		expect(resolveClosedAlertWinner(null, 0.99, false)).toBe(true);
		expect(resolveClosedAlertWinner(null, 0.01, false)).toBe(false);
	});
});

// ============================================================================
// PRICE CALCULATION TESTS
// ============================================================================

describe("normalizePrice", () => {
	test("returns value within 0-1 range", () => {
		expect(normalizePrice(0.5)).toBe(0.5);
		expect(normalizePrice(0)).toBe(0);
		expect(normalizePrice(1)).toBe(1);
	});

	test("clamps out-of-range values", () => {
		expect(normalizePrice(-0.5)).toBe(0);
		expect(normalizePrice(1.5)).toBe(1);
	});

	test("uses fallback for invalid inputs", () => {
		expect(normalizePrice(null)).toBe(0.5);
		expect(normalizePrice(undefined)).toBe(0.5);
		expect(normalizePrice(NaN)).toBe(0.5);
		expect(normalizePrice("invalid")).toBe(0.5);
	});
});

describe("normalizePriceRange", () => {
	test("returns normalized range", () => {
		expect(normalizePriceRange(0.1, 0.9)).toEqual({ min: 0.1, max: 0.9 });
	});

	test("swaps inverted ranges", () => {
		expect(normalizePriceRange(0.9, 0.1)).toEqual({ min: 0.1, max: 0.9 });
	});

	test("applies defaults for invalid values", () => {
		expect(normalizePriceRange(NaN, NaN)).toEqual({ min: 0.01, max: 1.0 });
	});
});

describe("getEntryPrice", () => {
	test("returns insider price for follow strategy", () => {
		expect(getEntryPrice(0.3, "follow_insider")).toBe(0.3);
		expect(getEntryPrice(0.7, "follow_insider")).toBe(0.7);
	});

	test("returns inverted price for reverse strategy", () => {
		expect(getEntryPrice(0.3, "reverse_insider")).toBe(0.7);
		expect(getEntryPrice(0.7, "reverse_insider")).toBe(0.3);
	});
});

describe("calculateTradeCost", () => {
	test("returns fixed stake for fixed_stake mode", () => {
		expect(calculateTradeCost(0.1, "fixed_stake")).toBe(FIXED_STAKE_USD);
		expect(calculateTradeCost(0.9, "fixed_stake")).toBe(FIXED_STAKE_USD);
	});

	test("returns variable cost for target_payout mode", () => {
		expect(calculateTradeCost(0.1, "target_payout")).toBe(TARGET_PAYOUT * 0.1);
		expect(calculateTradeCost(0.5, "target_payout")).toBe(TARGET_PAYOUT * 0.5);
		expect(calculateTradeCost(0.9, "target_payout")).toBe(TARGET_PAYOUT * 0.9);
	});
});

describe("calculateWinProfit", () => {
	test("calculates profit for fixed_stake", () => {
		// Bet $10 at price 0.5 -> win $10
		const cost = 10;
		const entryPrice = 0.5;
		expect(calculateWinProfit(cost, entryPrice, "fixed_stake")).toBe(10);

		// Bet $10 at price 0.25 -> win $30
		expect(calculateWinProfit(10, 0.25, "fixed_stake")).toBe(30);
	});

	test("calculates profit for target_payout", () => {
		// Target $10 payout, cost varies by price
		const cost = 5; // At price 0.5
		expect(calculateWinProfit(cost, 0.5, "target_payout")).toBe(
			TARGET_PAYOUT - cost,
		);
	});
});

describe("isPriceInRange", () => {
	test("returns true for prices within range", () => {
		expect(isPriceInRange(0.5, 0.1, 0.9)).toBe(true);
		expect(isPriceInRange(0.1, 0.1, 0.9)).toBe(true);
		expect(isPriceInRange(0.9, 0.1, 0.9)).toBe(true);
	});

	test("returns false for prices outside range", () => {
		expect(isPriceInRange(0.05, 0.1, 0.9)).toBe(false);
		expect(isPriceInRange(0.95, 0.1, 0.9)).toBe(false);
	});
});

// ============================================================================
// STRATEGY LOGIC TESTS
// ============================================================================

describe("didStrategyWin", () => {
	test("follow strategy wins when insider wins", () => {
		expect(didStrategyWin(true, "follow_insider")).toBe(true);
		expect(didStrategyWin(false, "follow_insider")).toBe(false);
	});

	test("reverse strategy wins when insider loses", () => {
		expect(didStrategyWin(true, "reverse_insider")).toBe(false);
		expect(didStrategyWin(false, "reverse_insider")).toBe(true);
	});

	test("returns false for unknown winner", () => {
		expect(didStrategyWin(null, "follow_insider")).toBe(false);
		expect(didStrategyWin(undefined, "reverse_insider")).toBe(false);
	});
});

describe("createAlertKey", () => {
	test("generates unique key from alert data", () => {
		const alert = mockAlert({ user: "user1", alert_time: 123456, volume: 100 });
		const key = createAlertKey(alert);
		expect(key.startsWith("123456")).toBe(true);
		expect(key).toContain("000100000000");
		expect(key).toContain("cond-123");
	});

	test("different alerts produce different keys", () => {
		const alert1 = mockAlert({ volume: 100 });
		const alert2 = mockAlert({ volume: 200 });
		expect(createAlertKey(alert1)).not.toBe(createAlertKey(alert2));
	});
});

// ============================================================================
// FILTERING TESTS
// ============================================================================

describe("categoryMatches", () => {
	test("matches ALL category", () => {
		const alert = mockAlert({ category: "CRYPTO" });
		expect(categoryMatches(alert, "ALL")).toBe(true);
	});

	test("matches specific category", () => {
		const alert = mockAlert({ category: "CRYPTO" });
		expect(categoryMatches(alert, "CRYPTO")).toBe(true);
		expect(categoryMatches(alert, "SPORTS")).toBe(false);
	});
});

describe("winnerFilterMatches", () => {
	test("BOTH matches all", () => {
		const winner = mockAlert({ winner: true });
		const loser = mockAlert({ winner: false });
		expect(winnerFilterMatches(winner, "BOTH")).toBe(true);
		expect(winnerFilterMatches(loser, "BOTH")).toBe(true);
	});

	test("WINNERS only matches winners", () => {
		const winner = mockAlert({ winner: true });
		const loser = mockAlert({ winner: false });
		expect(winnerFilterMatches(winner, "WINNERS")).toBe(true);
		expect(winnerFilterMatches(loser, "WINNERS")).toBe(false);
	});

	test("LOSERS only matches losers", () => {
		const winner = mockAlert({ winner: true });
		const loser = mockAlert({ winner: false });
		expect(winnerFilterMatches(winner, "LOSERS")).toBe(false);
		expect(winnerFilterMatches(loser, "LOSERS")).toBe(true);
	});
});

describe("filterAlerts", () => {
	test("returns empty array for no strategies", () => {
		const alerts = [mockAlert()];
		const settings = {
			strategies: [],
			minPrice: 0.01,
			maxPrice: 1.0,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: false,
			betSizing: "target_payout" as const,
		};
		expect(filterAlerts(alerts, settings)).toEqual([]);
	});

	test("filters by price range", () => {
		const lowPrice = mockAlert({ price: 0.1 }); // entry = 0.1
		const highPrice = mockAlert({ price: 0.9 }); // entry = 0.9
		const alerts = [lowPrice, highPrice];

		const settings = {
			strategies: ["follow_insider" as const],
			minPrice: 0.05,
			maxPrice: 0.5,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: false,
			betSizing: "target_payout" as const,
		};

		const filtered = filterAlerts(alerts, settings);
		expect(filtered).toHaveLength(1);
		expect(filtered[0]?.price).toBe(0.1);
	});
});

// ============================================================================
// STATE MANAGEMENT TESTS
// ============================================================================

describe("createInitialState", () => {
	test("creates empty state", () => {
		const state = createInitialState();
		expect(state.realizedPnL).toBe(0);
		expect(state.totalBet).toBe(0);
		expect(state.trades).toBe(0);
		expect(state.wins).toBe(0);
		expect(state.losses).toBe(0);
		expect(state.pending.size).toBe(0);
		expect(state.processed.size).toBe(0);
	});
});

describe("settleTrade", () => {
	test("records win when strategy wins", () => {
		const state = createInitialState();
		const { state: newState, settled } = settleTrade(
			state,
			10,
			0.5,
			true, // insider won
			"follow_insider",
			"fixed_stake",
		);

		expect(settled).toBe(true);
		expect(newState.wins).toBe(1);
		expect(newState.realizedPnL).toBe(10); // Won $10
	});

	test("records loss when strategy loses", () => {
		const state = createInitialState();
		const { state: newState, settled } = settleTrade(
			state,
			10,
			0.5,
			false, // insider lost
			"follow_insider",
			"fixed_stake",
		);

		expect(settled).toBe(true);
		expect(newState.losses).toBe(1);
		expect(newState.realizedPnL).toBe(-10); // Lost $10
	});

	test("does not settle when winner is unknown", () => {
		const state = createInitialState();
		const { state: newState, settled } = settleTrade(
			state,
			10,
			0.5,
			null, // unknown
			"follow_insider",
			"fixed_stake",
		);

		expect(settled).toBe(false);
		expect(newState.trades).toBe(0);
	});
});

describe("processAlerts", () => {
	test("processes winning trade", () => {
		const alert = mockAlert({
			closed: true,
			winner: true,
			price: 0.3,
		});

		const settings = {
			strategies: ["follow_insider" as const],
			minPrice: 0.01,
			maxPrice: 1.0,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: false,
			betSizing: "fixed_stake" as const,
		};

		const state = processAlerts(createInitialState(), [alert], settings);

		expect(state.trades).toBe(1);
		expect(state.wins).toBe(1);
		expect(state.losses).toBe(0);
		expect(state.realizedPnL).toBeGreaterThan(0);
	});

	test("processes losing trade", () => {
		const alert = mockAlert({
			closed: true,
			winner: false,
			price: 0.3,
		});

		const settings = {
			strategies: ["follow_insider" as const],
			minPrice: 0.01,
			maxPrice: 1.0,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: false,
			betSizing: "fixed_stake" as const,
		};

		const state = processAlerts(createInitialState(), [alert], settings);

		expect(state.trades).toBe(1);
		expect(state.wins).toBe(0);
		expect(state.losses).toBe(1);
		expect(state.realizedPnL).toBeLessThan(0);
	});

	test("adds open markets to pending", () => {
		const alert = mockAlert({
			closed: false,
			price: 0.3,
		});

		const settings = {
			strategies: ["follow_insider" as const],
			minPrice: 0.01,
			maxPrice: 1.0,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: false,
			betSizing: "fixed_stake" as const,
		};

		const state = processAlerts(createInitialState(), [alert], settings);

		expect(state.trades).toBe(1);
		expect(state.pending.size).toBe(1);
		expect(state.openInterest).toBeGreaterThan(0);
	});

	test("CRITICAL BUG FIX: excludes closed markets with unknown winner", () => {
		// This is the critical bug fix - closed markets with no winner
		// should be excluded entirely, not added to pending
		const alert = mockAlert({
			closed: true,
			winner: null, // No winner data
			market_price: 0.5, // Price is ambiguous (not >= 0.98 or <= 0.05)
			price: 0.5,
		});

		const settings = {
			strategies: ["follow_insider" as const],
			minPrice: 0.01,
			maxPrice: 1.0,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: false,
			betSizing: "fixed_stake" as const,
		};

		const state = processAlerts(createInitialState(), [alert], settings);

		// Should NOT be counted in any stats
		expect(state.trades).toBe(0);
		expect(state.totalBet).toBe(0);
		expect(state.pending.size).toBe(0);
		expect(state.realizedPnL).toBe(0);
	});

	test("handles reverse_insider strategy", () => {
		const alert = mockAlert({
			closed: true,
			winner: true, // Insider won
			price: 0.3,
		});

		const settings = {
			strategies: ["reverse_insider" as const], // We bet against insider
			minPrice: 0.01,
			maxPrice: 1.0,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: false,
			betSizing: "fixed_stake" as const,
		};

		const state = processAlerts(createInitialState(), [alert], settings);

		// Reverse strategy loses when insider wins
		expect(state.trades).toBe(1);
		expect(state.wins).toBe(0);
		expect(state.losses).toBe(1);
	});

	test("respects onlyBetOnce constraint", () => {
		const alert1 = mockAlert({
			closed: true,
			winner: true,
			conditionId: "same-cond",
			price: 0.3,
		});
		const alert2 = mockAlert({
			closed: true,
			winner: true,
			conditionId: "same-cond", // Same condition
			price: 0.4,
			alert_time: alert1.alert_time + 1,
		});

		const settings = {
			strategies: ["follow_insider" as const],
			minPrice: 0.01,
			maxPrice: 1.0,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: true, // Only bet once per condition
			betSizing: "fixed_stake" as const,
		};

		let state = createInitialState();
		state = processAlerts(state, [alert1], settings);
		expect(state.trades).toBe(1);

		state = processAlerts(state, [alert2], settings);
		// Second alert should be skipped (same condition already bet)
		expect(state.trades).toBe(1);
	});
});

// ============================================================================
// PnL CALCULATION TESTS
// ============================================================================

describe("calculatePnL", () => {
	test("calculates PnL from alerts", () => {
		const alerts = [
			mockAlert({ closed: true, winner: true, price: 0.3, alert_time: 1000 }),
			mockAlert({ closed: true, winner: false, price: 0.3, alert_time: 2000 }),
		];

		const settings = {
			strategies: ["follow_insider" as const],
			minPrice: 0.01,
			maxPrice: 1.0,
			category: "ALL",
			winnerFilter: "BOTH" as const,
			onlyBetOnce: false,
			betSizing: "fixed_stake" as const,
		};

		const result = calculatePnL(alerts, settings);

		expect(result.trades).toBe(2);
		expect(result.wins).toBe(1);
		expect(result.losses).toBe(1);
	});
});

// ============================================================================
// UTILITY TESTS
// ============================================================================

describe("formatPnL", () => {
	test("formats positive PnL", () => {
		expect(formatPnL(10.5)).toBe("+$10.50");
	});

	test("formats negative PnL", () => {
		expect(formatPnL(-10.5)).toBe("-$10.50");
	});
});

describe("getWinRate", () => {
	test("calculates win rate", () => {
		const state = { ...createInitialState(), trades: 10, wins: 7 };
		expect(getWinRate(state)).toBe(70);
	});

	test("returns 0 for no trades", () => {
		expect(getWinRate(createInitialState())).toBe(0);
	});
});

describe("getROI", () => {
	test("calculates ROI", () => {
		const state = { ...createInitialState(), realizedPnL: 50, totalBet: 100 };
		expect(getROI(state)).toBe(50);
	});

	test("returns 0 for no bets", () => {
		expect(getROI(createInitialState())).toBe(0);
	});
});
