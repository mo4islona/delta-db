import { describe, expect, test } from "bun:test";
import type { AlertItem, Pagination } from "@/types/terminal";
import {
	initialTerminalDataState,
	terminalDataReducer,
} from "@/reducers/terminalDataReducer";
import { initialTerminalUiState } from "@/reducers/terminalUiReducer";
import { runBacktestStep } from "./backtestEngine";

function makeAlert(index: number, overrides: Partial<AlertItem> = {}): AlertItem {
	return {
		price: 0.6,
		user: `u-${index}`,
		volume: 500,
		alert_time: 1_700_000_100 + index,
		market_count: 1,
		outcome: "YES",
		winner: true,
		closed: true,
		conditionId: `c-${index}`,
		question: `Q ${index}`,
		tokenId: `t-${index}`,
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

describe("runBacktestStep", () => {
	test("requests next page when loaded history has more pages", () => {
		let state = initialTerminalDataState;
		state = terminalDataReducer(state, {
			type: "ALERTS_PAGE_RECEIVED",
			payload: {
				requestedPage: 1,
				alerts: [makeAlert(1)],
				pagination: makePagination(1, true),
			},
		});

		const result = runBacktestStep(state, initialTerminalUiState);
		expect(result.cursor).toBe(1);
		expect(result.done).toBe(false);
		expect(result.nextPageToRequest).toBe(2);
	});

	test("completes when no additional alert pages are available", () => {
		let state = initialTerminalDataState;
		state = terminalDataReducer(state, {
			type: "ALERTS_PAGE_RECEIVED",
			payload: {
				requestedPage: 1,
				alerts: [makeAlert(1)],
				pagination: makePagination(1, false),
			},
		});

		const result = runBacktestStep(state, initialTerminalUiState);
		expect(result.cursor).toBe(1);
		expect(result.done).toBe(true);
		expect(result.nextPageToRequest).toBeNull();
	});
});
