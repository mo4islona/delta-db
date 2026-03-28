import { createAlertKey } from "@/lib/backtest";
import type {
	AlertItem,
	MarketOutcome,
	Pagination,
	TrackerState,
} from "@/types/terminal";

export type BacktestStatus =
	| "idle"
	| "running"
	| "waiting_more_data"
	| "done"
	| "error";

export interface BacktestRuntimeState {
	runId: number;
	status: BacktestStatus;
	cursor: number;
	canContinue: boolean;
	result: TrackerState;
	error: string | null;
}

export interface TerminalDataState {
	alertsCategory: string;
	alertsById: Record<string, AlertItem>;
	historyIds: string[];
	alertsPages: Record<number, string[]>;
	alertsPaginationByPage: Record<number, Pagination>;
	alertsHasNextByPage: Record<number, boolean>;
	loadedMaxAlertsPage: number;
	pendingPageRequests: number[];
	marketsPages: Record<number, MarketOutcome[]>;
	marketsPaginationByPage: Record<number, Pagination>;
	loadedMaxMarketsPage: number;
	backtestGeneration: number;
	backtest: BacktestRuntimeState;
}

export type TerminalDataAction =
	| { type: "RESET_ALERT_DATA"; payload: { category: string } }
	| {
			type: "ALERTS_PAGE_RECEIVED";
			payload: {
				requestedPage: number;
				alerts: AlertItem[];
				pagination: Pagination;
			};
	  }
	| { type: "REQUEST_ALERTS_PAGE"; payload: { page: number } }
	| { type: "RESOLVE_ALERTS_PAGE_REQUEST"; payload: { page: number } }
	| {
			type: "MARKETS_PAGE_RECEIVED";
			payload: {
				requestedPage: number;
				markets: MarketOutcome[];
				pagination: Pagination;
			};
	  }
	| { type: "INCREMENT_BACKTEST_GENERATION" }
	| { type: "BACKTEST_START"; payload: { runId: number } }
	| {
			type: "BACKTEST_PROGRESS";
			payload: {
				runId: number;
				cursor: number;
				result: TrackerState;
				status?: BacktestStatus;
				canContinue?: boolean;
			};
	  }
	| {
			type: "BACKTEST_REQUIRE_PAGE";
			payload: {
				runId: number;
				page: number;
				result: TrackerState;
				cursor: number;
			};
	  }
	| {
			type: "BACKTEST_COMPLETE";
			payload: { runId: number; cursor: number; result: TrackerState };
	  }
	| { type: "BACKTEST_ERROR"; payload: { runId: number; message: string } }
	| { type: "BACKTEST_RESET" };

const INITIAL_TRACKER_RESULT: TrackerState = {
	realizedPnL: 0,
	liveTotalBet: 0,
	liveTrades: 0,
	liveWins: 0,
	liveLosses: 0,
	openInterest: 0,
	totalBet: 0,
};

function uniqueNumbers(values: number[]): number[] {
	return Array.from(new Set(values)).sort((a, b) => a - b);
}

function removePendingPage(pending: number[], page: number): number[] {
	if (!pending.includes(page)) return pending;
	return pending.filter((value) => value !== page);
}

function normalizeAlertsPagination(
	requestedPage: number,
	pagination: Pagination,
	hasNext: boolean,
): Pagination {
	const totalPages = Math.max(
		Number(pagination.totalPages || 1),
		hasNext ? requestedPage + 1 : requestedPage,
		1,
	);
	return {
		page: requestedPage,
		limit: Number(pagination.limit || 10),
		total: Number(pagination.total || 0),
		totalPages,
		hasPrev: requestedPage > 1,
		hasNext,
	};
}

function normalizeMarketsPagination(
	requestedPage: number,
	pagination: Pagination,
): Pagination {
	const totalPages = Math.max(
		Number(pagination.totalPages || 1),
		requestedPage,
		1,
	);
	return {
		page: requestedPage,
		limit: Number(pagination.limit || 5),
		total: Number(pagination.total || 0),
		totalPages,
		hasPrev: requestedPage > 1,
		hasNext: Boolean(pagination.hasNext),
	};
}

export const initialTerminalDataState: TerminalDataState = {
	alertsCategory: "ALL",
	alertsById: {},
	historyIds: [],
	alertsPages: {},
	alertsPaginationByPage: {},
	alertsHasNextByPage: {},
	loadedMaxAlertsPage: 0,
	pendingPageRequests: [],
	marketsPages: {},
	marketsPaginationByPage: {},
	loadedMaxMarketsPage: 0,
	backtestGeneration: 0,
	backtest: {
		runId: 0,
		status: "idle",
		cursor: 0,
		canContinue: false,
		result: INITIAL_TRACKER_RESULT,
		error: null,
	},
};

export function terminalDataReducer(
	state: TerminalDataState,
	action: TerminalDataAction,
): TerminalDataState {
	switch (action.type) {
		case "RESET_ALERT_DATA": {
			const nextCategory = action.payload.category;
			if (
				state.alertsCategory === nextCategory &&
				Object.keys(state.alertsPages).length === 0
			) {
				return state;
			}
			return {
				...state,
				alertsCategory: nextCategory,
				alertsById: {},
				historyIds: [],
				alertsPages: {},
				alertsPaginationByPage: {},
				alertsHasNextByPage: {},
				loadedMaxAlertsPage: 0,
				pendingPageRequests: [],
				backtest: {
					...state.backtest,
					status: "idle",
					cursor: 0,
					canContinue: false,
					result: INITIAL_TRACKER_RESULT,
					error: null,
				},
				backtestGeneration: state.backtestGeneration + 1,
			};
		}
		case "ALERTS_PAGE_RECEIVED": {
			const { requestedPage, alerts, pagination } = action.payload;
			const page = Math.max(1, requestedPage);
			const previousHistorySet = new Set(state.historyIds);
			const seenInPayload = new Set<string>();
			const nextAlertsById = { ...state.alertsById };
			const nextHistory = [...state.historyIds];
			const pageIds: string[] = [];

			for (const alert of alerts) {
				const id = createAlertKey(alert);
				if (seenInPayload.has(id)) {
					continue;
				}
				seenInPayload.add(id);
				pageIds.push(id);
				nextAlertsById[id] = alert;
				if (!previousHistorySet.has(id)) {
					nextHistory.push(id);
					previousHistorySet.add(id);
				}
			}

			const stagnantPage = false;
			const hasNext = stagnantPage ? false : Boolean(pagination.hasNext);
			const normalizedPagination = normalizeAlertsPagination(
				page,
				pagination,
				hasNext,
			);

			return {
				...state,
				alertsById: nextAlertsById,
				historyIds: nextHistory,
				alertsPages: {
					...state.alertsPages,
					[page]: pageIds,
				},
				alertsPaginationByPage: {
					...state.alertsPaginationByPage,
					[page]: normalizedPagination,
				},
				alertsHasNextByPage: {
					...state.alertsHasNextByPage,
					[page]: hasNext,
				},
				loadedMaxAlertsPage: Math.max(state.loadedMaxAlertsPage, page),
				pendingPageRequests: removePendingPage(state.pendingPageRequests, page),
			};
		}
		case "REQUEST_ALERTS_PAGE": {
			const page = Math.max(1, action.payload.page);
			if (state.alertsPages[page]) {
				return state;
			}
			if (state.pendingPageRequests.includes(page)) {
				return state;
			}
			return {
				...state,
				pendingPageRequests: uniqueNumbers([...state.pendingPageRequests, page]),
			};
		}
		case "RESOLVE_ALERTS_PAGE_REQUEST": {
			const page = Math.max(1, action.payload.page);
			return {
				...state,
				pendingPageRequests: removePendingPage(state.pendingPageRequests, page),
			};
		}
		case "MARKETS_PAGE_RECEIVED": {
			const { requestedPage, markets, pagination } = action.payload;
			const page = Math.max(1, requestedPage);
			return {
				...state,
				marketsPages: {
					...state.marketsPages,
					[page]: markets,
				},
				marketsPaginationByPage: {
					...state.marketsPaginationByPage,
					[page]: normalizeMarketsPagination(page, pagination),
				},
				loadedMaxMarketsPage: Math.max(state.loadedMaxMarketsPage, page),
			};
		}
		case "INCREMENT_BACKTEST_GENERATION": {
			return {
				...state,
				backtestGeneration: state.backtestGeneration + 1,
				backtest: {
					...state.backtest,
					status: "idle",
					cursor: 0,
					canContinue: false,
					error: null,
				},
			};
		}
		case "BACKTEST_START": {
			return {
				...state,
				backtest: {
					...state.backtest,
					runId: action.payload.runId,
					status: "running",
					cursor: 0,
					canContinue: false,
					error: null,
				},
			};
		}
		case "BACKTEST_PROGRESS": {
			if (action.payload.runId !== state.backtest.runId) return state;
			return {
				...state,
				backtest: {
					...state.backtest,
					status: action.payload.status ?? state.backtest.status,
					cursor: action.payload.cursor,
					canContinue: action.payload.canContinue ?? state.backtest.canContinue,
					result: action.payload.result,
					error: null,
				},
			};
		}
		case "BACKTEST_REQUIRE_PAGE": {
			if (action.payload.runId !== state.backtest.runId) return state;
			const page = Math.max(1, action.payload.page);
			return {
				...state,
				pendingPageRequests: state.pendingPageRequests.includes(page)
					? state.pendingPageRequests
					: uniqueNumbers([...state.pendingPageRequests, page]),
				backtest: {
					...state.backtest,
					status: "waiting_more_data",
					cursor: action.payload.cursor,
					canContinue: true,
					result: action.payload.result,
					error: null,
				},
			};
		}
		case "BACKTEST_COMPLETE": {
			if (action.payload.runId !== state.backtest.runId) return state;
			return {
				...state,
				backtest: {
					...state.backtest,
					status: "done",
					cursor: action.payload.cursor,
					canContinue: false,
					result: action.payload.result,
					error: null,
				},
			};
		}
		case "BACKTEST_ERROR": {
			if (action.payload.runId !== state.backtest.runId) return state;
			return {
				...state,
				backtest: {
					...state.backtest,
					status: "error",
					error: action.payload.message,
				},
			};
		}
		case "BACKTEST_RESET": {
			return {
				...state,
				backtest: {
					...state.backtest,
					status: "idle",
					cursor: 0,
					canContinue: false,
					result: INITIAL_TRACKER_RESULT,
					error: null,
				},
			};
		}
		default:
			return state;
	}
}
