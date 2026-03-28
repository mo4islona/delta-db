import type {
	AlertItem,
	BetSizing,
	PendingAlert,
	StrategyMode,
	WinnerFilter,
} from "../types/terminal";

// ============================================================================
// CONSTANTS
// ============================================================================

export const TARGET_PAYOUT = 10;
export const FIXED_STAKE_USD = 10;

// Price thresholds for winner inference
export const WINNER_THRESHOLD_HIGH = 0.98;
export const WINNER_THRESHOLD_LOW = 0.05;

// ============================================================================
// TYPES
// ============================================================================

export interface FilterSettings {
	strategies: StrategyMode[];
	minPrice: number;
	maxPrice: number;
	category: string;
	winnerFilter: WinnerFilter;
	onlyBetOnce: boolean;
	betSizing: BetSizing;
}

export interface TradeResult {
	cost: number;
	entryPrice: number;
	profit: number;
	won: boolean;
	settled: boolean;
}

export interface PendingTrade extends PendingAlert {
	mode: StrategyMode;
	betSizing: BetSizing;
}

export interface BacktestState {
	realizedPnL: number;
	totalBet: number;
	trades: number;
	wins: number;
	losses: number;
	openInterest: number;
	pending: Map<string, PendingTrade>;
	processed: Set<string>;
	betConditions: Set<string>; // For onlyBetOnce tracking
}

export interface PnLResult {
	realizedPnL: number;
	totalBet: number;
	trades: number;
	wins: number;
	losses: number;
	openInterest: number;
}

export interface ProcessOptions {
	resolveClosedWithMarketDataOnly?: boolean;
	silent?: boolean;
}

// ============================================================================
// PURE FUNCTIONS - Winner Resolution
// ============================================================================

export function parseWinnerValue(value: unknown): boolean | null {
	if (typeof value === "boolean") return value;
	if (typeof value === "number") return value !== 0;
	if (typeof value === "string") {
		const normalized = value.trim().toLowerCase();
		if (normalized === "true" || normalized === "1" || normalized === "yes") {
			return true;
		}
		if (normalized === "false" || normalized === "0" || normalized === "no") {
			return false;
		}
	}
	return null;
}

export function inferInsiderWin(
	rawWinner: unknown,
	rawLastPrice: unknown,
	allowPriceInference = true,
): boolean | null {
	const parsedWinner = parseWinnerValue(rawWinner);
	if (parsedWinner !== null) {
		return parsedWinner;
	}

	if (!allowPriceInference) {
		return null;
	}

	const lastPrice = Number(rawLastPrice);
	if (Number.isFinite(lastPrice)) {
		if (lastPrice >= WINNER_THRESHOLD_HIGH) return true;
		if (lastPrice <= WINNER_THRESHOLD_LOW) return false;
	}

	return null;
}

export function resolveClosedAlertWinner(
	rawWinner: unknown,
	rawLastPrice: unknown,
	resolveClosedWithMarketDataOnly: boolean,
): boolean | null {
	return inferInsiderWin(
		rawWinner,
		rawLastPrice,
		!resolveClosedWithMarketDataOnly,
	);
}

// ============================================================================
// PURE FUNCTIONS - Price Calculations
// ============================================================================

export function normalizePrice(
	rawPrice: number | string | undefined | null,
	fallback = 0.5,
): number {
	if (rawPrice === null || rawPrice === undefined || rawPrice === "") {
		return fallback;
	}
	const value = Number(rawPrice);
	if (!Number.isFinite(value)) return fallback;
	return Math.max(0, Math.min(1, value));
}

export function clampPrice(rawValue: number, fallback: number): number {
	if (!Number.isFinite(rawValue)) return fallback;
	return Math.max(0, Math.min(1, rawValue));
}

export function normalizePriceRange(
	rawMin: number,
	rawMax: number,
): { min: number; max: number } {
	const min = clampPrice(rawMin, 0.01);
	const max = clampPrice(rawMax, 1.0);
	return min <= max ? { min, max } : { min: max, max: min };
}

export function getEntryPrice(
	rawPrice: number | string | undefined | null,
	mode: StrategyMode,
): number {
	const insiderPrice = normalizePrice(rawPrice, 0.5);
	const price = mode === "reverse_insider" ? 1 - insiderPrice : insiderPrice;
	// Round to 6 decimal places to avoid floating point issues
	return Math.round(price * 1000000) / 1000000;
}

export function isPriceInRange(
	price: number,
	minPrice: number,
	maxPrice: number,
): boolean {
	return price >= minPrice && price <= maxPrice;
}

export function calculateTradeCost(
	entryPrice: number,
	betSizing: BetSizing,
): number {
	if (betSizing === "fixed_stake") {
		return FIXED_STAKE_USD;
	}
	return TARGET_PAYOUT * entryPrice;
}

export function calculateWinProfit(
	cost: number,
	entryPrice: number,
	betSizing: BetSizing,
): number {
	if (betSizing === "fixed_stake") {
		const safeEntry = Math.max(entryPrice, 0.0001);
		return cost / safeEntry - cost;
	}
	return TARGET_PAYOUT - cost;
}

// ============================================================================
// PURE FUNCTIONS - Strategy Logic
// ============================================================================

export function didStrategyWin(
	insiderWon: boolean | null | undefined,
	mode: StrategyMode,
): boolean {
	if (insiderWon === null || insiderWon === undefined) return false;
	return mode === "reverse_insider" ? !insiderWon : insiderWon;
}

export function createAlertKey(alert: AlertItem): string {
	const alertTime = Number.isFinite(Number(alert.alert_time))
		? Math.max(0, Math.floor(Number(alert.alert_time)))
		: 0;
	const scaledVolume = Number.isFinite(Number(alert.volume))
		? Math.max(0, Math.round(Number(alert.volume) * 1_000_000))
		: 0;
	const paddedVolume = String(scaledVolume).padStart(12, "0");
	const baseId = `${alertTime}${paddedVolume}`;
	return `${baseId}:${alert.conditionId ?? ""}:${alert.tokenId ?? ""}:${String(alert.outcome ?? "")}`;
}

export function createTradeId(mode: StrategyMode, alertKey: string): string {
	return `${mode}:${alertKey}`;
}

export function createConditionKey(
	mode: StrategyMode,
	conditionId: string | null,
): string | undefined {
	return conditionId ? `${mode}:${conditionId}` : undefined;
}

// ============================================================================
// PURE FUNCTIONS - Filtering
// ============================================================================

export function normalizeCategory(category: string | null | undefined): string {
	const normalized = String(category ?? "").trim();
	if (!normalized) return "ALL";
	if (normalized.toUpperCase() === "ALL") return "ALL";
	return normalized.toUpperCase();
}

export function categoryMatches(
	alert: AlertItem,
	selectedCategory: string,
): boolean {
	if (selectedCategory === "ALL") return true;
	const alertCategory = normalizeCategory(alert.category);
	return alertCategory.toLowerCase() === selectedCategory.toLowerCase();
}

export function winnerFilterMatches(
	alert: AlertItem,
	winnerFilter: WinnerFilter,
): boolean {
	if (winnerFilter === "BOTH") return true;
	const insiderWon = inferInsiderWin(
		alert.winner,
		alert.market_price ?? alert.price,
	);
	if (winnerFilter === "WINNERS") return insiderWon === true;
	if (winnerFilter === "LOSERS") return insiderWon === false;
	return true;
}

export function alertMatchesFilters(
	alert: AlertItem,
	settings: FilterSettings,
): boolean {
	const normalizedCategory = normalizeCategory(settings.category);

	if (!categoryMatches(alert, normalizedCategory)) {
		return false;
	}
	if (!winnerFilterMatches(alert, settings.winnerFilter)) {
		return false;
	}

	const normalizedRange = normalizePriceRange(
		settings.minPrice,
		settings.maxPrice,
	);

	return settings.strategies.some((mode) => {
		const entryPrice = getEntryPrice(alert.price, mode);
		return isPriceInRange(entryPrice, normalizedRange.min, normalizedRange.max);
	});
}

export function filterAlerts(
	alerts: AlertItem[],
	settings: FilterSettings,
): AlertItem[] {
	if (settings.strategies.length === 0) return [];
	return alerts.filter((alert) => alertMatchesFilters(alert, settings));
}

export function sortStrategies(modes: StrategyMode[]): StrategyMode[] {
	const STRATEGY_ORDER: StrategyMode[] = ["follow_insider", "reverse_insider"];
	const unique = Array.from(new Set(modes));
	return STRATEGY_ORDER.filter((mode) => unique.includes(mode));
}

// ============================================================================
// PURE FUNCTIONS - State Management (Reducers)
// ============================================================================

export function createInitialState(): BacktestState {
	return {
		realizedPnL: 0,
		totalBet: 0,
		trades: 0,
		wins: 0,
		losses: 0,
		openInterest: 0,
		pending: new Map(),
		processed: new Set(),
		betConditions: new Set(),
	};
}

export function calculateOpenInterest(state: BacktestState): number {
	let openInterest = 0;
	for (const pending of state.pending.values()) {
		openInterest += pending.cost;
	}
	return openInterest;
}

export function settleTrade(
	state: BacktestState,
	cost: number,
	entryPrice: number,
	insiderWon: boolean | null | undefined,
	mode: StrategyMode,
	betSizing: BetSizing,
): { state: BacktestState; settled: boolean } {
	// If winner is unknown, trade cannot be settled
	if (insiderWon === null || insiderWon === undefined) {
		return { state, settled: false };
	}

	const strategyWon = didStrategyWin(insiderWon, mode);
	const newState = { ...state };

	if (strategyWon) {
		const profit = calculateWinProfit(cost, entryPrice, betSizing);
		newState.realizedPnL += profit;
		newState.wins += 1;
	} else {
		newState.realizedPnL -= cost;
		newState.losses += 1;
	}

	return { state: newState, settled: true };
}

export function processTrade(
	state: BacktestState,
	alert: AlertItem,
	mode: StrategyMode,
	settings: FilterSettings,
	options: ProcessOptions = {},
): BacktestState {
	const { resolveClosedWithMarketDataOnly = false } = options;

	// Create identifiers
	const alertKey = createAlertKey(alert);
	const tradeId = createTradeId(mode, alertKey);
	const conditionKey = createConditionKey(mode, alert.conditionId);

	// Skip if already processed
	if (state.processed.has(tradeId)) {
		// Check if pending and now can be settled
		if (state.pending.has(tradeId) && alert.closed) {
			const pending = state.pending.get(tradeId);
			if (!pending) return state;
			const winner = resolveClosedAlertWinner(
				alert.winner,
				alert.market_price ?? alert.price,
				resolveClosedWithMarketDataOnly,
			);

			const { state: settledState, settled } = settleTrade(
				state,
				pending.cost,
				pending.price,
				winner,
				pending.mode,
				pending.betSizing,
			);

			if (settled) {
				const newPending = new Map(settledState.pending);
				newPending.delete(tradeId);
				return {
					...settledState,
					pending: newPending,
					openInterest: calculateOpenInterest({
						...settledState,
						pending: newPending,
					}),
				};
			}
		}
		return state;
	}

	// Check onlyBetOnce constraint
	if (
		settings.onlyBetOnce &&
		conditionKey &&
		state.betConditions.has(conditionKey)
	) {
		return state;
	}

	// Calculate entry price and cost
	const entryPrice = getEntryPrice(alert.price, mode);
	const normalizedRange = normalizePriceRange(
		settings.minPrice,
		settings.maxPrice,
	);

	if (!isPriceInRange(entryPrice, normalizedRange.min, normalizedRange.max)) {
		return state;
	}

	const cost = calculateTradeCost(entryPrice, settings.betSizing);

	// Mark as processed
	const newProcessed = new Set(state.processed);
	newProcessed.add(tradeId);

	const newBetConditions = new Set(state.betConditions);
	if (conditionKey && settings.onlyBetOnce) {
		newBetConditions.add(conditionKey);
	}

	// Create new state
	let newState: BacktestState = {
		...state,
		processed: newProcessed,
		betConditions: newBetConditions,
		trades: state.trades + 1,
		totalBet: state.totalBet + cost,
	};

	// Handle open vs closed markets
	if (!alert.closed) {
		// Market still open - add to pending
		const pendingTrade: PendingTrade = {
			id: tradeId,
			trader: alert.user,
			detectedAt: Number(alert.alert_time),
			volume: Number(alert.volume || 0),
			conditionId: alert.conditionId,
			tokenId: alert.tokenId,
			user: alert.user,
			alert_time: Number(alert.alert_time),
			outcome: String(alert.outcome ?? ""),
			price: entryPrice,
			marketQuestion: alert.question ?? "",
			cost,
			mode,
			betSizing: settings.betSizing,
		};
		const newPending = new Map(state.pending);
		newPending.set(tradeId, pendingTrade);
		newState = {
			...newState,
			pending: newPending,
			openInterest: calculateOpenInterest({ ...newState, pending: newPending }),
		};
	} else {
		// Market closed - try to settle
		const winner = resolveClosedAlertWinner(
			alert.winner,
			alert.market_price ?? alert.price,
			resolveClosedWithMarketDataOnly,
		);

		// CRITICAL FIX: If market is closed but winner is unknown, SKIP entirely
		// Don't add to pending, don't count toward stats
		if (winner === null) {
			// Closed market with no resolution - exclude from PnL
			return state; // Return unchanged state
		}

		const { state: settledState, settled } = settleTrade(
			newState,
			cost,
			entryPrice,
			winner,
			mode,
			settings.betSizing,
		);

		if (!settled) {
			// Should not happen if winner !== null, but handle gracefully
			const pendingTrade: PendingTrade = {
				id: tradeId,
				trader: alert.user,
				detectedAt: Number(alert.alert_time),
				volume: Number(alert.volume || 0),
				conditionId: alert.conditionId,
				tokenId: alert.tokenId,
				user: alert.user,
				alert_time: Number(alert.alert_time),
				outcome: String(alert.outcome ?? ""),
				price: entryPrice,
				marketQuestion: alert.question ?? "",
				cost,
				mode,
				betSizing: settings.betSizing,
			};
			const newPending = new Map(state.pending);
			newPending.set(tradeId, pendingTrade);
			return {
				...settledState,
				pending: newPending,
				openInterest: calculateOpenInterest({
					...settledState,
					pending: newPending,
				}),
			};
		}

		newState = settledState;
	}

	return {
		...newState,
		openInterest: calculateOpenInterest(newState),
	};
}

export function processAlerts(
	state: BacktestState,
	alerts: AlertItem[],
	settings: FilterSettings,
	options: ProcessOptions = {},
): BacktestState {
	const sortedStrategies = sortStrategies(settings.strategies);

	if (alerts.length === 0 || sortedStrategies.length === 0) {
		return state;
	}

	let currentState = state;

	for (const alert of alerts) {
		// Check category and winner filters first
		if (!categoryMatches(alert, normalizeCategory(settings.category))) {
			continue;
		}
		if (!winnerFilterMatches(alert, settings.winnerFilter)) {
			continue;
		}

		// Process for each strategy
		for (const mode of sortedStrategies) {
			currentState = processTrade(currentState, alert, mode, settings, options);
		}
	}

	return currentState;
}

// ============================================================================
// PURE FUNCTIONS - PnL Calculation
// ============================================================================

export function calculatePnL(
	alerts: AlertItem[],
	settings: FilterSettings,
	options: ProcessOptions = {},
): PnLResult {
	const initialState = createInitialState();
	const finalState = processAlerts(initialState, alerts, settings, options);

	return {
		realizedPnL: finalState.realizedPnL,
		totalBet: finalState.totalBet,
		trades: finalState.trades,
		wins: finalState.wins,
		losses: finalState.losses,
		openInterest: finalState.openInterest,
	};
}

// Calculate PnL from a subset (calcArray pattern)
export function calculateFilteredPnL(
	allAlerts: AlertItem[],
	settings: FilterSettings,
): PnLResult {
	const filtered = filterAlerts(allAlerts, settings);
	return calculatePnL(filtered, settings);
}

// Calculate full sum across all alerts (ignoring some filters)
export function calculateFullSum(
	allAlerts: AlertItem[],
	baseSettings: FilterSettings,
): PnLResult {
	// Full sum uses all strategies but respects price/category filters
	const fullSettings: FilterSettings = {
		...baseSettings,
		strategies: ["follow_insider", "reverse_insider"],
		winnerFilter: "BOTH",
	};
	return calculatePnL(allAlerts, fullSettings);
}

// ============================================================================
// PURE FUNCTIONS - Pending Resolution
// ============================================================================

export function checkPendingResolutions(
	state: BacktestState,
	marketResolutions: Map<
		string,
		{
			closed: boolean;
			outcomes: Array<{ tokenId?: string; outcome?: string; winner?: unknown }>;
		}
	>,
): BacktestState {
	let currentState = state;
	const pendingToRemove: string[] = [];

	for (const [tradeId, pending] of state.pending.entries()) {
		if (!pending.conditionId) continue;

		const market = marketResolutions.get(pending.conditionId);
		if (!market || !market.closed) continue;

		// Find matching outcome
		const resolvedOutcome = market.outcomes.find((outcome) => {
			const pendingTokenId = String(pending.tokenId ?? "");
			const pendingOutcome = String(pending.outcome ?? "").toUpperCase();

			if (pendingTokenId && outcome.tokenId) {
				return outcome.tokenId === pendingTokenId;
			}
			if (!pendingOutcome) return false;
			return String(outcome.outcome ?? "").toUpperCase() === pendingOutcome;
		});

		if (!resolvedOutcome) continue;

		const winner = parseWinnerValue(resolvedOutcome.winner);
		if (winner === null) continue;

		const { state: settledState, settled } = settleTrade(
			currentState,
			pending.cost,
			pending.price,
			winner,
			pending.mode,
			pending.betSizing,
		);

		if (settled) {
			currentState = settledState;
			pendingToRemove.push(tradeId);
		}
	}

	// Remove settled pending trades
	const newPending = new Map(currentState.pending);
	for (const tradeId of pendingToRemove) {
		newPending.delete(tradeId);
	}

	return {
		...currentState,
		pending: newPending,
		openInterest: calculateOpenInterest({
			...currentState,
			pending: newPending,
		}),
	};
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

export function formatPnL(value: number): string {
	const sign = value >= 0 ? "+" : "-";
	const absValue = Math.abs(value);
	return `${sign}$${absValue.toFixed(2)}`;
}

export function getWinRate(state: BacktestState): number {
	if (state.trades === 0) return 0;
	return (state.wins / state.trades) * 100;
}

export function getROI(state: BacktestState): number {
	if (state.totalBet === 0) return 0;
	return (state.realizedPnL / state.totalBet) * 100;
}
