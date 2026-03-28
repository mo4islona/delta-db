import {
	normalizeCategory,
	normalizePriceRange,
	sortStrategies,
} from "@/lib/backtest";
import type { BetSizing, StrategyMode, WinnerFilter } from "@/types/terminal";

export type TradeSide = "YES" | "NO";

export interface TerminalUiState {
	alertsPage: number;
	marketsPage: number;
	minPrice: number;
	maxPrice: number;
	category: string;
	winnerFilter: WinnerFilter;
	strategies: StrategyMode[];
	sides: TradeSide[];
	onlyBetOnce: boolean;
	betSizing: BetSizing;
	autoRefreshEnabled: boolean;
	soundEnabled: boolean;
}

export type TerminalUiAction =
	| { type: "SET_ALERTS_PAGE"; payload: number }
	| { type: "SET_MARKETS_PAGE"; payload: number }
	| { type: "SET_CATEGORY"; payload: string }
	| { type: "SET_WINNER_FILTER"; payload: WinnerFilter }
	| { type: "SET_PRICE_RANGE"; payload: { min: number; max: number } }
	| { type: "TOGGLE_STRATEGY"; payload: { mode: StrategyMode; enabled: boolean } }
	| { type: "TOGGLE_SIDE"; payload: { side: TradeSide; enabled: boolean } }
	| { type: "SET_ONLY_BET_ONCE"; payload: boolean }
	| { type: "SET_BET_SIZING"; payload: BetSizing }
	| { type: "SET_AUTO_REFRESH"; payload: boolean }
	| { type: "SET_SOUND_ENABLED"; payload: boolean };

export const initialTerminalUiState: TerminalUiState = {
	alertsPage: 1,
	marketsPage: 1,
	minPrice: 0.01,
	maxPrice: 1,
	category: "ALL",
	winnerFilter: "BOTH",
	strategies: ["follow_insider"],
	sides: ["YES", "NO"],
	onlyBetOnce: false,
	betSizing: "target_payout",
	autoRefreshEnabled: true,
	soundEnabled: false,
};

function resetToFirstAlertsPage(state: TerminalUiState): TerminalUiState {
	if (state.alertsPage === 1) return state;
	return { ...state, alertsPage: 1 };
}

export function terminalUiReducer(
	state: TerminalUiState,
	action: TerminalUiAction,
): TerminalUiState {
	switch (action.type) {
		case "SET_ALERTS_PAGE": {
			return {
				...state,
				alertsPage: Math.max(1, Math.floor(action.payload)),
			};
		}
		case "SET_MARKETS_PAGE": {
			return {
				...state,
				marketsPage: Math.max(1, Math.floor(action.payload)),
			};
		}
		case "SET_CATEGORY": {
			const next = normalizeCategory(action.payload);
			if (next === state.category) return state;
			return resetToFirstAlertsPage({ ...state, category: next });
		}
		case "SET_WINNER_FILTER": {
			if (action.payload === state.winnerFilter) return state;
			return resetToFirstAlertsPage({ ...state, winnerFilter: action.payload });
		}
		case "SET_PRICE_RANGE": {
			const normalized = normalizePriceRange(
				action.payload.min,
				action.payload.max,
			);
			if (
				normalized.min === state.minPrice &&
				normalized.max === state.maxPrice
			) {
				return state;
			}
			return resetToFirstAlertsPage({
				...state,
				minPrice: normalized.min,
				maxPrice: normalized.max,
			});
		}
		case "TOGGLE_STRATEGY": {
			const nextRaw = action.payload.enabled
				? [...state.strategies, action.payload.mode]
				: state.strategies.filter((mode) => mode !== action.payload.mode);
			const next = sortStrategies(nextRaw);
			if (next.length === 0) return state;
			if (next.join("|") === state.strategies.join("|")) return state;
			return resetToFirstAlertsPage({ ...state, strategies: next });
		}
		case "TOGGLE_SIDE": {
			const nextRaw = action.payload.enabled
				? [...state.sides, action.payload.side]
				: state.sides.filter((side) => side !== action.payload.side);
			const next = Array.from(new Set(nextRaw));
			if (next.length === 0) return state;
			if (next.join("|") === state.sides.join("|")) return state;
			return resetToFirstAlertsPage({
				...state,
				sides: next as TradeSide[],
			});
		}
		case "SET_ONLY_BET_ONCE": {
			if (action.payload === state.onlyBetOnce) return state;
			return { ...state, onlyBetOnce: action.payload };
		}
		case "SET_BET_SIZING": {
			if (action.payload === state.betSizing) return state;
			return { ...state, betSizing: action.payload };
		}
		case "SET_AUTO_REFRESH": {
			if (action.payload === state.autoRefreshEnabled) return state;
			return { ...state, autoRefreshEnabled: action.payload };
		}
		case "SET_SOUND_ENABLED": {
			if (action.payload === state.soundEnabled) return state;
			return { ...state, soundEnabled: action.payload };
		}
		default:
			return state;
	}
}
