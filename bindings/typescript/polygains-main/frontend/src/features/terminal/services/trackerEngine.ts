import {
	categoryMatches,
	calculatePnL,
	createInitialState,
	normalizeCategory,
	processTrade,
	sortStrategies,
	winnerFilterMatches,
	type FilterSettings,
} from "@/lib/backtest";
import type { TerminalUiState } from "@/reducers/terminalUiReducer";
import type { AlertItem, TrackerState } from "@/types/terminal";

const EMPTY_TRACKER: TrackerState = {
	realizedPnL: 0,
	liveTotalBet: 0,
	liveTrades: 0,
	liveWins: 0,
	liveLosses: 0,
	openInterest: 0,
	totalBet: 0,
};

function buildFilterSettings(ui: TerminalUiState): FilterSettings {
	return {
		strategies: ui.strategies,
		minPrice: ui.minPrice,
		maxPrice: ui.maxPrice,
		category: normalizeCategory(ui.category),
		winnerFilter: ui.winnerFilter,
		onlyBetOnce: ui.onlyBetOnce,
		betSizing: ui.betSizing,
	};
}

function applySideFilter(alerts: AlertItem[], ui: TerminalUiState): AlertItem[] {
	const allowed = new Set(ui.sides);
	return alerts.filter((alert) => {
		const outcome = String(alert.outcome ?? "").toUpperCase();
		if (outcome === "YES" || outcome === "NO") {
			return allowed.has(outcome as "YES" | "NO");
		}
		return true;
	});
}

export function computeTrackerFromAlerts(
	alerts: AlertItem[],
	ui: TerminalUiState,
): TrackerState {
	if (ui.strategies.length === 0 || alerts.length === 0) {
		return EMPTY_TRACKER;
	}

	const filtered = applySideFilter(alerts, ui)
		.slice()
		.sort((a, b) => Number(a.alert_time || 0) - Number(b.alert_time || 0));

	const result = calculatePnL(filtered, buildFilterSettings(ui), {
		resolveClosedWithMarketDataOnly: true,
	});

	return {
		realizedPnL: result.realizedPnL,
		liveTotalBet: result.totalBet,
		liveTrades: result.trades,
		liveWins: result.wins,
		liveLosses: result.losses,
		openInterest: result.openInterest,
		totalBet: result.totalBet,
	};
}

export function emptyTracker(): TrackerState {
	return EMPTY_TRACKER;
}

function downsampleSeries(values: number[], maxPoints: number): number[] {
	if (values.length <= maxPoints) {
		return values;
	}
	const step = (values.length - 1) / (maxPoints - 1);
	const sampled: number[] = [];
	for (let i = 0; i < maxPoints; i += 1) {
		const sourceIndex = Math.round(i * step);
		sampled.push(values[sourceIndex]);
	}
	return sampled;
}

export function buildBacktestPnlSeries(
	alerts: AlertItem[],
	ui: TerminalUiState,
	maxPoints = 36,
): number[] {
	if (ui.strategies.length === 0 || alerts.length === 0) {
		return [];
	}

	const filteredAlerts = applySideFilter(alerts, ui)
		.slice()
		.sort((a, b) => Number(a.alert_time || 0) - Number(b.alert_time || 0));
	const settings = buildFilterSettings(ui);
	const normalizedCategory = normalizeCategory(settings.category);
	const sortedStrategies = sortStrategies(settings.strategies);

	let state = createInitialState();
	const series: number[] = [0];

	for (const alert of filteredAlerts) {
		if (!categoryMatches(alert, normalizedCategory)) {
			continue;
		}
		if (!winnerFilterMatches(alert, settings.winnerFilter)) {
			continue;
		}

		for (const mode of sortedStrategies) {
			state = processTrade(state, alert, mode, settings, {
				resolveClosedWithMarketDataOnly: true,
			});
		}

		series.push(state.realizedPnL);
	}

	return downsampleSeries(series, Math.max(8, maxPoints));
}
