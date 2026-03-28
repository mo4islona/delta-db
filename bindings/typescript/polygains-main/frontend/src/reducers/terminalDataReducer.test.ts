import { describe, expect, test } from "bun:test";
import type { AlertItem, Pagination, TrackerState } from "../types/terminal";
import {
	initialTerminalDataState,
	terminalDataReducer,
} from "./terminalDataReducer";

function makeAlert(index: number, overrides: Partial<AlertItem> = {}): AlertItem {
	return {
		price: 0.55,
		user: `wallet-${index}`,
		volume: 1000 + index,
		alert_time: 1_700_000_000 + index,
		market_count: 1,
		outcome: "YES",
		winner: true,
		closed: true,
		conditionId: `cond-${index}`,
		question: `Question ${index}`,
		tokenId: `token-${index}`,
		market_price: 1,
		walletAddress: `0x${index}`,
		category: "CRYPTO",
		...overrides,
	};
}

function makePagination(page: number, hasNext: boolean): Pagination {
	return {
		page,
		limit: 10,
		total: 100,
		totalPages: hasNext ? page + 1 : page,
		hasPrev: page > 1,
		hasNext,
	};
}

const emptyTracker: TrackerState = {
	realizedPnL: 0,
	liveTotalBet: 0,
	liveTrades: 0,
	liveWins: 0,
	liveLosses: 0,
	openInterest: 0,
	totalBet: 0,
};

describe("terminalDataReducer", () => {
	test("ingests alerts pages and keeps history deduped", () => {
		let state = initialTerminalDataState;
		const first = makeAlert(1);
		const second = makeAlert(2);

		state = terminalDataReducer(state, {
			type: "ALERTS_PAGE_RECEIVED",
			payload: {
				requestedPage: 1,
				alerts: [first],
				pagination: makePagination(1, true),
			},
		});

		expect(state.loadedMaxAlertsPage).toBe(1);
		expect(state.historyIds).toHaveLength(1);
		expect(state.alertsPages[1]).toHaveLength(1);

		state = terminalDataReducer(state, {
			type: "ALERTS_PAGE_RECEIVED",
			payload: {
				requestedPage: 2,
				alerts: [first, second],
				pagination: makePagination(2, false),
			},
		});

		expect(state.loadedMaxAlertsPage).toBe(2);
		expect(state.historyIds).toHaveLength(2);
		expect(state.alertsPages[2]).toHaveLength(2);
		expect(state.alertsHasNextByPage[2]).toBe(false);
	});

	test("marks backtest waiting and queues requested page", () => {
		let state = initialTerminalDataState;

		state = terminalDataReducer(state, {
			type: "BACKTEST_START",
			payload: { runId: 99 },
		});

		state = terminalDataReducer(state, {
			type: "BACKTEST_REQUIRE_PAGE",
			payload: {
				runId: 99,
				page: 3,
				cursor: 12,
				result: emptyTracker,
			},
		});

		expect(state.pendingPageRequests).toContain(3);
		expect(state.backtest.status).toBe("waiting_more_data");
		expect(state.backtest.canContinue).toBe(true);
		expect(state.backtest.cursor).toBe(12);

		state = terminalDataReducer(state, {
			type: "RESOLVE_ALERTS_PAGE_REQUEST",
			payload: { page: 3 },
		});

		expect(state.pendingPageRequests).not.toContain(3);
	});
});
