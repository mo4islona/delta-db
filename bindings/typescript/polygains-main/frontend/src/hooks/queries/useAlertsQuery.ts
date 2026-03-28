"use client";
import { useEffect } from "react";
import useSWR from "swr";
import useSWRInfinite from "swr/infinite";
import { fetchAlerts } from "@/api/terminalApi";
import { normalizeCategory } from "@/lib/backtest";
import { swrKeys, type AlertsKey } from "@/hooks/swr/keys";
import { SWR_REFRESH } from "@/hooks/swr/options";
import type { AlertItem, Pagination } from "@/types/terminal";

export interface AlertsPagePayload {
	requestedPage: number;
	data: AlertItem[];
	pagination: Pagination;
}

interface AlertsQueryOptions {
	refreshInterval?: number;
	enabled?: boolean;
}

interface AlertsPagesOptions extends AlertsQueryOptions {
	limit?: number;
	category?: string;
	page?: number;
	initialSize?: number;
}

export function useAlertsQuery(
	page: number,
	limit: number,
	category = "ALL",
	options: AlertsQueryOptions = {},
) {
	const normalizedCategory = normalizeCategory(category);
	const { refreshInterval = SWR_REFRESH.alerts, enabled = true } = options;

	const query = useSWR(
		enabled ? swrKeys.alerts(page, limit, normalizedCategory) : null,
		async ([, params]: AlertsKey): Promise<AlertsPagePayload> => {
			const response = await fetchAlerts(
				params.page,
				params.limit,
				params.category === "ALL" ? undefined : params.category,
			);

			return {
				requestedPage: params.page,
				data: response.data,
				pagination: {
					...response.pagination,
					page: params.page,
					hasPrev: params.page > 1,
				},
			};
		},
		{
			refreshInterval,
			revalidateOnFocus: false,
		},
	);

	return {
		page: query.data,
		...query,
	};
}

export function useAlertsPagesQuery(options: AlertsPagesOptions = {}) {
	const {
		limit = 10,
		category = "ALL",
		page = 1,
		initialSize = 1,
		refreshInterval = SWR_REFRESH.alerts,
		enabled = true,
	} = options;
	const normalizedCategory = normalizeCategory(category);

	const query = useSWRInfinite<AlertsPagePayload>(
		(
			pageIndex: number,
			previousPageData: AlertsPagePayload | null,
		): AlertsKey | null => {
			if (!enabled) return null;
			if (previousPageData && !previousPageData.pagination.hasNext) {
				return null;
			}
			return swrKeys.alerts(pageIndex + 1, limit, normalizedCategory);
		},
		async (key: AlertsKey) => {
			const [, params] = key;
			const response = await fetchAlerts(
				params.page,
				params.limit,
				params.category === "ALL" ? undefined : params.category,
			);
			return {
				requestedPage: params.page,
				data: response.data,
				pagination: {
					...response.pagination,
					page: params.page,
					hasPrev: params.page > 1,
				},
			};
		},
		{
			initialSize,
			refreshInterval,
			revalidateOnFocus: false,
			revalidateFirstPage: true,
		},
	);

	// Sync SWR size with current page to ensure pagination works correctly
	useEffect(() => {
		if (query.size < page) {
			void query.setSize(page);
		}
	}, [page, query]);

	return {
		pages: query.data ?? [],
		...query,
	};
}
