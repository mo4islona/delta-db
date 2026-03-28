import type { TerminalDataState } from "@/reducers/terminalDataReducer";
import type { TerminalUiState } from "@/reducers/terminalUiReducer";
import type { AlertItem, TrackerState } from "@/types/terminal";
import { computeTrackerFromAlerts, emptyTracker } from "./trackerEngine";

export interface BacktestStepResult {
	tracker: TrackerState;
	cursor: number;
	nextPageToRequest: number | null;
	done: boolean;
}

export function getChronologicalAlerts(state: TerminalDataState): AlertItem[] {
	const alerts: AlertItem[] = [];

	for (const id of state.historyIds) {
		const alert = state.alertsById[id];
		if (!alert) continue;
		alerts.push(alert);
	}

	alerts.sort((a, b) => Number(a.alert_time || 0) - Number(b.alert_time || 0));
	return alerts;
}

export function runBacktestStep(
	state: TerminalDataState,
	ui: TerminalUiState,
): BacktestStepResult {
	const alerts = getChronologicalAlerts(state);
	if (alerts.length === 0) {
		const hasNext = Boolean(state.alertsHasNextByPage[state.loadedMaxAlertsPage]);
		return {
			tracker: emptyTracker(),
			cursor: 0,
			nextPageToRequest:
				hasNext && state.loadedMaxAlertsPage > 0
					? state.loadedMaxAlertsPage + 1
					: state.loadedMaxAlertsPage === 0
						? 1
						: null,
			done: !hasNext && state.loadedMaxAlertsPage > 0,
		};
	}

	const tracker = computeTrackerFromAlerts(alerts, ui);
	const loadedMaxPage = Math.max(1, state.loadedMaxAlertsPage);
	const hasNext = Boolean(state.alertsHasNextByPage[loadedMaxPage]);
	const nextPageToRequest = hasNext ? loadedMaxPage + 1 : null;

	return {
		tracker,
		cursor: alerts.length,
		nextPageToRequest,
		done: !hasNext,
	};
}
