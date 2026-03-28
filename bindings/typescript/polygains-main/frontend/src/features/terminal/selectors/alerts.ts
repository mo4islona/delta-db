import {
	alertMatchesFilters as sharedAlertMatchesFilters,
	createAlertKey,
	inferInsiderWin,
} from "@/lib/backtest";
import type { AlertRowView } from "@/types/api";
import type { AlertItem, Pagination } from "@/types/terminal";
import type { TerminalDataState } from "@/reducers/terminalDataReducer";
import type { TerminalUiState } from "@/reducers/terminalUiReducer";

export interface AlertsDisplayModel {
	alerts: AlertItem[];
	rows: AlertRowView[];
	pagination: Pagination;
	filledThroughPage: number;
	nextPageToRequest: number | null;
}

function mapOutcome(outcome: unknown): { label: string; className: string } {
	if (outcome === null || outcome === undefined || outcome === "") {
		return { label: "N/A", className: "over-under" };
	}
	const text = String(outcome).toUpperCase();
	if (text === "YES" || text === "1") return { label: "YES", className: "yes" };
	if (text === "NO" || text === "0") return { label: "NO", className: "no" };
	return { label: text, className: "over-under" };
}

function alertMatchesUiFilters(alert: AlertItem, ui: TerminalUiState): boolean {
	const outcome = mapOutcome(alert.outcome).label;
	if ((outcome === "YES" || outcome === "NO") && !ui.sides.includes(outcome)) {
		return false;
	}

	return sharedAlertMatchesFilters(alert, {
		strategies: ui.strategies,
		minPrice: ui.minPrice,
		maxPrice: ui.maxPrice,
		category: ui.category,
		winnerFilter: ui.winnerFilter,
		onlyBetOnce: false,
		betSizing: "target_payout",
	});
}

function toRows(alerts: AlertItem[]): AlertRowView[] {
	return alerts.map((alert) => {
		const timestamp = Number(alert.alert_time || 0);
		const dt = new Date(timestamp * 1000);
		const outcome = mapOutcome(alert.outcome);
		const profileAddress = String(alert.walletAddress || alert.user || "");
		const addrShort =
			profileAddress.length > 12
				? `${profileAddress.slice(0, 6)}...${profileAddress.slice(-4)}`
				: profileAddress;
		const winnerStatus = inferInsiderWin(
			alert.winner,
			alert.market_price ?? alert.price,
		);
		const statusBadgeHtml = alert.closed
			? winnerStatus === null
				? '<span class="status-badge pending">PENDING</span>'
				: `<span class="status-badge ${winnerStatus ? "won" : "loss"}">${winnerStatus ? "WON" : "LOSS"}</span>`
			: "";

		return {
			rowId: `alert-${createAlertKey(alert)}`,
			user: alert.user,
			profileAddress,
			addrShort,
			volumeFormatted: Number(alert.volume || 0).toFixed(2),
			outcomeClass: outcome.className,
			outcomeLabel: outcome.label,
			statusBadgeHtml,
			dateText: dt.toLocaleDateString(),
			timeText: dt.toLocaleTimeString(),
			detailHtml: "",
			expanded: false,
			question: alert.question ?? "",
			timestamp,
			conditionId: alert.conditionId ?? "",
			priceFormatted: Number(alert.price || 0).toFixed(2),
			volume: Number(alert.volume || 0),
			price: Number(alert.price || 0),
		};
	});
}

export function selectAlertsDisplay(
	state: TerminalDataState,
	ui: TerminalUiState,
	pageSize = 10,
): AlertsDisplayModel {
	const page = ui.alertsPage;
	const selected: AlertItem[] = [];
	const seen = new Set<string>();

	let scanPage = page;
	let filledThroughPage = page;
	let nextPageToRequest: number | null = null;

	while (selected.length < pageSize) {
		const pageIds = state.alertsPages[scanPage];
		if (!pageIds) {
			if (scanPage <= state.loadedMaxAlertsPage) {
				scanPage += 1;
				continue;
			}
			nextPageToRequest = scanPage;
			break;
		}

		for (const id of pageIds) {
			if (seen.has(id)) continue;
			seen.add(id);
			const alert = state.alertsById[id];
			if (!alert) continue;
			if (!alertMatchesUiFilters(alert, ui)) continue;
			selected.push(alert);
			if (selected.length >= pageSize) {
				break;
			}
		}

		filledThroughPage = scanPage;
		const hasNext = state.alertsHasNextByPage[scanPage] ?? false;
		if (!hasNext || selected.length >= pageSize) {
			break;
		}

		scanPage += 1;
		if (scanPage > state.loadedMaxAlertsPage && !state.alertsPages[scanPage]) {
			nextPageToRequest = scanPage;
			break;
		}
	}

	// Determine hasNext: if we need to fetch more, or if the API said there's more
	// Default to true (optimistic) when we don't have definitive info yet
	const hasNextFromApi = state.alertsHasNextByPage[filledThroughPage];
	const hasNextFromCurrentPage = state.alertsHasNextByPage[page];
	const computedHasNext =
		nextPageToRequest !== null ||
		hasNextFromApi === true ||
		hasNextFromCurrentPage === true;
	// Only disable next button if we explicitly know there's no next page
	const finalHasNext =
		computedHasNext ||
		(hasNextFromApi === undefined && hasNextFromCurrentPage === undefined);

	const basePagination = state.alertsPaginationByPage[page] ?? {
		page,
		limit: pageSize,
		total: selected.length,
		totalPages: Math.max(page, filledThroughPage),
		hasPrev: page > 1,
		hasNext: finalHasNext,
	};

	const pagination: Pagination = {
		...basePagination,
		page,
		hasPrev: page > 1,
		hasNext: finalHasNext,
		totalPages: Math.max(basePagination.totalPages || 1, filledThroughPage),
	};

	const alerts = selected
		.slice()
		.sort((a, b) => {
			const timeDiff = Number(b.alert_time || 0) - Number(a.alert_time || 0);
			if (timeDiff !== 0) return timeDiff;
			const volumeDiff = Number(b.volume || 0) - Number(a.volume || 0);
			if (volumeDiff !== 0) return volumeDiff;
			return createAlertKey(a).localeCompare(createAlertKey(b));
		})
		.slice(0, pageSize);

	return {
		alerts,
		rows: toRows(alerts),
		pagination,
		filledThroughPage,
		nextPageToRequest,
	};
}
