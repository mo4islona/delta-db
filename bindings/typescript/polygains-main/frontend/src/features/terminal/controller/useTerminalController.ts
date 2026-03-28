"use client";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
	useTerminalDataDispatch,
	useTerminalDataState,
} from "@/context/TerminalDataContext";
import {
	useTerminalUiDispatch,
	useTerminalUiState,
} from "@/context/TerminalUiContext";
import { createAlertKey } from "@/lib/backtest";
import type { TradeSide } from "@/reducers/terminalUiReducer";
import type { StrategyMode, WinnerFilter } from "@/types/terminal";
import { useAlertsPagesQuery } from "@/hooks/queries/useAlertsQuery";
import { useCategoriesQuery } from "@/hooks/queries/useCategoriesQuery";
import { useHealthQuery } from "@/hooks/queries/useHealthQuery";
import { useMarketsPagesQuery } from "@/hooks/queries/useMarketsQuery";
import {
	useGlobalStatsQuery,
	useInsiderStatsQuery,
} from "@/hooks/queries/useStatsQueries";
import { SWR_REFRESH } from "@/hooks/swr/options";
import { runBacktestStep } from "../services/backtestEngine";
import {
	buildBacktestPnlSeries,
	computeTrackerFromAlerts,
} from "../services/trackerEngine";
import { selectAlertsDisplay } from "../selectors/alerts";
import { groupMarkets } from "../selectors/markets";

const ALERTS_PAGE_SIZE = 10;
const MARKETS_PAGE_SIZE = 5;
const DEFAULT_CATEGORY_OPTIONS = ["ALL"] as const;

function formatNum(value: number | string | undefined | null): string {
	const val = Number(value ?? 0);
	if (!Number.isFinite(val)) return "0.00";
	if (val >= 1_000_000) return `${(val / 1_000_000).toFixed(2)}M`;
	if (val >= 1_000) return `${(val / 1_000).toFixed(2)}K`;
	return val.toLocaleString(undefined, {
		minimumFractionDigits: 2,
		maximumFractionDigits: 2,
	});
}

export function useTerminalController() {
	const ui = useTerminalUiState();
	const uiDispatch = useTerminalUiDispatch();
	const data = useTerminalDataState();
	const dataDispatch = useTerminalDataDispatch();

	// Incrementing block counter - randomly adds 1 every second to the base block
	const [blockOffset, setBlockOffset] = useState(0);
	useEffect(() => {
		const interval = setInterval(() => {
			if (Math.random() < 0.5) {
				setBlockOffset((prev) => prev + 1);
			}
		}, 1000);
		return () => clearInterval(interval);
	}, []);

	const filterSignatureRef = useRef<string>("");
	const ingestedAlertsRef = useRef<Record<number, string>>({});
	const ingestedMarketsRef = useRef<Record<number, string>>({});

	const healthQuery = useHealthQuery({ refreshInterval: SWR_REFRESH.health });
	const insiderStatsQuery = useInsiderStatsQuery({
		refreshInterval: SWR_REFRESH.insiderStats,
	});
	const globalStatsQuery = useGlobalStatsQuery({
		refreshInterval: SWR_REFRESH.globalStats,
	});
	const categoriesQuery = useCategoriesQuery();

	const alertsPagesQuery = useAlertsPagesQuery({
		category: ui.category,
		limit: ALERTS_PAGE_SIZE,
		page: ui.alertsPage,
		refreshInterval: ui.autoRefreshEnabled ? SWR_REFRESH.alerts : 0,
	});

	const marketsPagesQuery = useMarketsPagesQuery({
		limit: MARKETS_PAGE_SIZE,
		page: ui.marketsPage,
		refreshInterval: ui.autoRefreshEnabled ? SWR_REFRESH.markets : 0,
	});

	const alertsDisplay = useMemo(
		() => selectAlertsDisplay(data, ui, ALERTS_PAGE_SIZE),
		[data, ui],
	);

	useEffect(() => {
		if (data.alertsCategory !== ui.category) {
			// Don't reset alert data during an active backtest
			if (data.backtest.status === "running" || data.backtest.status === "waiting_more_data") {
				return;
			}
			ingestedAlertsRef.current = {};
			dataDispatch({
				type: "RESET_ALERT_DATA",
				payload: { category: ui.category },
			});
		}
	}, [data.alertsCategory, ui.category, data.backtest.status, dataDispatch]);

	useEffect(() => {
		for (const payload of alertsPagesQuery.pages) {
			const page = payload.requestedPage;
			const sig = `${payload.pagination.hasNext}|${payload.data
				.map((item) => createAlertKey(item))
				.join(",")}`;
			if (ingestedAlertsRef.current[page] === sig) {
				continue;
			}
			ingestedAlertsRef.current[page] = sig;
			dataDispatch({
				type: "ALERTS_PAGE_RECEIVED",
				payload: {
					requestedPage: page,
					alerts: payload.data,
					pagination: payload.pagination,
				},
			});
		}
	}, [alertsPagesQuery.pages, dataDispatch]);

	useEffect(() => {
		for (const payload of marketsPagesQuery.pages) {
			const page = payload.requestedPage;
			const sig = `${payload.pagination.hasNext}|${payload.markets
				.map(
					(item) =>
						`${item.conditionId ?? ""}:${String(item.outcome)}:${item.last_price}`,
				)
				.join(",")}`;
			if (ingestedMarketsRef.current[page] === sig) {
				continue;
			}
			ingestedMarketsRef.current[page] = sig;
			dataDispatch({
				type: "MARKETS_PAGE_RECEIVED",
				payload: {
					requestedPage: page,
					markets: payload.markets,
					pagination: payload.pagination,
				},
			});
		}
	}, [marketsPagesQuery.pages, dataDispatch]);

	useEffect(() => {
		if (alertsDisplay.nextPageToRequest === null) return;
		if (data.pendingPageRequests.includes(alertsDisplay.nextPageToRequest))
			return;
		if (data.alertsPages[alertsDisplay.nextPageToRequest]) return;
		dataDispatch({
			type: "REQUEST_ALERTS_PAGE",
			payload: { page: alertsDisplay.nextPageToRequest },
		});
	}, [
		alertsDisplay.nextPageToRequest,
		data.pendingPageRequests,
		data.alertsPages,
		dataDispatch,
	]);

	const invalidateBacktest = useCallback(() => {
		dataDispatch({ type: "INCREMENT_BACKTEST_GENERATION" });
	}, [dataDispatch]);

	const filterSignature = useMemo(
		() =>
			JSON.stringify({
				minPrice: ui.minPrice,
				maxPrice: ui.maxPrice,
				category: ui.category,
				winnerFilter: ui.winnerFilter,
				strategies: ui.strategies,
				sides: ui.sides,
				onlyBetOnce: ui.onlyBetOnce,
				betSizing: ui.betSizing,
			}),
		[
			ui.minPrice,
			ui.maxPrice,
			ui.category,
			ui.winnerFilter,
			ui.strategies,
			ui.sides,
			ui.onlyBetOnce,
			ui.betSizing,
		],
	);

	useEffect(() => {
		if (!filterSignatureRef.current) {
			filterSignatureRef.current = filterSignature;
			return;
		}
		if (filterSignatureRef.current !== filterSignature) {
			filterSignatureRef.current = filterSignature;
			// Don't invalidate backtest while it's actively running
			if (data.backtest.status !== "running" && data.backtest.status !== "waiting_more_data") {
				invalidateBacktest();
			}
		}
	}, [filterSignature, invalidateBacktest, data.backtest.status]);

	useEffect(() => {
		if (data.backtest.status !== "running" && data.backtest.status !== "waiting_more_data") return;
		const maxPending = Math.max(...data.pendingPageRequests, 0);
		if (maxPending > 0 && alertsPagesQuery.size < maxPending) {
			void alertsPagesQuery.setSize(maxPending);
		}
	}, [data.pendingPageRequests, data.backtest.status, alertsPagesQuery]);

	useEffect(() => {
		if (
			data.backtest.status !== "running" &&
			data.backtest.status !== "waiting_more_data"
		) {
			return;
		}

		const runId = data.backtest.runId;
		const step = runBacktestStep(data, ui);

		if (step.nextPageToRequest !== null) {
			const alreadyLoaded = Boolean(data.alertsPages[step.nextPageToRequest]);
			const alreadyPending = data.pendingPageRequests.includes(
				step.nextPageToRequest,
			);
			if (!alreadyLoaded && !alreadyPending) {
				dataDispatch({
					type: "BACKTEST_REQUIRE_PAGE",
					payload: {
						runId,
						page: step.nextPageToRequest,
						cursor: step.cursor,
						result: step.tracker,
					},
				});
				return;
			}

			if (data.backtest.status !== "waiting_more_data") {
				dataDispatch({
					type: "BACKTEST_PROGRESS",
					payload: {
						runId,
						cursor: step.cursor,
						result: step.tracker,
						status: "waiting_more_data",
						canContinue: true,
					},
				});
			}
			return;
		}

		if (step.done) {
			dataDispatch({
				type: "BACKTEST_COMPLETE",
				payload: { runId, cursor: step.cursor, result: step.tracker },
			});
			return;
		}

		dataDispatch({
			type: "BACKTEST_PROGRESS",
			payload: {
				runId,
				cursor: step.cursor,
				result: step.tracker,
				status: "running",
				canContinue: false,
			},
		});
	}, [data, ui, dataDispatch]);

	// Use the grouped categories from the query - these are already filtered and sorted
	const categoryOptions = useMemo(() => {
		return categoriesQuery.categoryDetails;
	}, [categoriesQuery.categoryDetails]);

	// Flat (ungrouped) categories for "all" mode in the dropdown
	const allCategoryOptions = useMemo(() => {
		return categoriesQuery.allCategoryDetails;
	}, [categoriesQuery.allCategoryDetails]);

	const marketOutcomes = data.marketsPages[ui.marketsPage] ?? [];
	const groupedMarkets = useMemo(
		() => groupMarkets(marketOutcomes).slice(0, MARKETS_PAGE_SIZE),
		[marketOutcomes],
	);

	const currentMarketsPagination = data.marketsPaginationByPage[
		ui.marketsPage
	] ?? {
		page: ui.marketsPage,
		limit: MARKETS_PAGE_SIZE,
		total: groupedMarkets.length,
		totalPages: Math.max(1, ui.marketsPage),
		hasPrev: ui.marketsPage > 1,
		hasNext: false,
	};

	const allLoadedAlerts = useMemo(() => {
		return data.historyIds
			.map((id) => data.alertsById[id])
			.filter((item): item is NonNullable<typeof item> => Boolean(item));
	}, [data.historyIds, data.alertsById]);

	const liveTracker = useMemo(() => {
		return computeTrackerFromAlerts(allLoadedAlerts, ui);
	}, [allLoadedAlerts, ui]);

	const tracker =
		data.backtest.status === "idle" ? liveTracker : data.backtest.result;
	const introBacktestPnlSeries = useMemo(() => {
		return buildBacktestPnlSeries(allLoadedAlerts, ui, 36);
	}, [allLoadedAlerts, ui]);

	const runBacktest = useCallback(() => {
		const runId = Date.now();
		dataDispatch({ type: "BACKTEST_START", payload: { runId } });
	}, [dataDispatch]);

	const baseBlock =
		insiderStatsQuery.stats?.current_block ?? healthQuery.health?.current_block;
	const currentBlockText = baseBlock
		? String(Number(baseBlock) + blockOffset)
		: "--";

	const syncState = {
		label: healthQuery.error ? "SYNC: ERROR" : "SYNC: ONLINE",
		healthy: !healthQuery.error,
		block: currentBlockText,
	};

	const applyPriceRange = useCallback(
		(min: number, max: number) => {
			uiDispatch({ type: "SET_PRICE_RANGE", payload: { min, max } });
		},
		[uiDispatch],
	);

	const changeAlertsPage = useCallback(
		(delta: number) => {
			const targetPage = Math.max(1, ui.alertsPage + delta);
			uiDispatch({ type: "SET_ALERTS_PAGE", payload: targetPage });
			dataDispatch({
				type: "REQUEST_ALERTS_PAGE",
				payload: { page: targetPage },
			});
		},
		[ui.alertsPage, uiDispatch, dataDispatch],
	);

	const changeMarketsPage = useCallback(
		(delta: number) => {
			const targetPage = Math.max(1, ui.marketsPage + delta);
			uiDispatch({ type: "SET_MARKETS_PAGE", payload: targetPage });
		},
		[ui.marketsPage, uiDispatch],
	);

	const backtestRunning =
		data.backtest.status === "running" ||
		data.backtest.status === "waiting_more_data";

	return {
		syncState,
		currentBlockText,
		alertsRows: alertsDisplay.rows,
		alertsPagination: alertsDisplay.pagination,
		alertsLoading: alertsPagesQuery.isLoading && data.loadedMaxAlertsPage === 0,
		categoryOptions,
		allCategoryOptions,
		selectedCategory: ui.category,
		selectedWinnerFilter: ui.winnerFilter,
		setCategory: (value: string) => {
			uiDispatch({ type: "SET_CATEGORY", payload: value });
		},
		setWinnerFilter: (value: WinnerFilter) => {
			uiDispatch({ type: "SET_WINNER_FILTER", payload: value });
		},
		changeAlertsPage,
		detection: {
			totalInsiders: Number(insiderStatsQuery.stats?.total_insiders || 0),
			yesInsiders: Number(insiderStatsQuery.stats?.yes_insiders || 0),
			noInsiders: Number(insiderStatsQuery.stats?.no_insiders || 0),
			insiderVolume: formatNum(insiderStatsQuery.stats?.total_volume || 0),
		},
		markets: groupedMarkets,
		marketsPagination: currentMarketsPagination,
		marketsLoading:
			marketsPagesQuery.isLoading && !data.marketsPages[ui.marketsPage],
		changeMarketsPage,
		introBacktestPnlSeries,
		globalStats: {
			accounts: formatNum(globalStatsQuery.stats?.total_accounts || 0),
			markets: formatNum(globalStatsQuery.stats?.total_markets || 0),
			trades: formatNum(globalStatsQuery.stats?.total_trades || 0),
			activePositions: formatNum(globalStatsQuery.stats?.active_positions || 0),
		},
		liveControls: {
			minPrice: ui.minPrice,
			maxPrice: ui.maxPrice,
			onlyBetOnce: ui.onlyBetOnce,
			betOneDollarPerTrade: ui.betSizing === "fixed_stake",
			disabled: backtestRunning,
			selectedStrategies: ui.strategies,
			selectedSides: ui.sides,
			onMinPriceChange: (value: number) => applyPriceRange(value, ui.maxPrice),
			onMaxPriceChange: (value: number) => applyPriceRange(ui.minPrice, value),
			onOnlyBetOnceChange: (value: boolean) =>
				uiDispatch({ type: "SET_ONLY_BET_ONCE", payload: value }),
			onBetOneDollarPerTradeChange: (value: boolean) =>
				uiDispatch({
					type: "SET_BET_SIZING",
					payload: value ? "fixed_stake" : "target_payout",
				}),
			onStrategyChange: (mode: StrategyMode, enabled: boolean) =>
				uiDispatch({ type: "TOGGLE_STRATEGY", payload: { mode, enabled } }),
			onSideToggle: (side: string, enabled: boolean) =>
				uiDispatch({
					type: "TOGGLE_SIDE",
					payload: { side: side as TradeSide, enabled },
				}),
		},
		tracker: {
			totalBet: tracker.liveTotalBet,
			openInterest: tracker.openInterest,
			realizedPnL: tracker.realizedPnL,
			liveTrades: tracker.liveTrades,
			liveWins: tracker.liveWins,
			liveLosses: tracker.liveLosses,
			alertsPage: alertsDisplay.pagination.page || ui.alertsPage,
			alertsTotalPages: alertsDisplay.pagination.totalPages || 1,
			alertsFilledThroughPage: alertsDisplay.filledThroughPage,
			backtestCanContinue: data.backtest.canContinue,
			backtestRunning,
			onRunBacktest: runBacktest,
		},
		marketStatsLoadingByCondition: {},
	};
}
