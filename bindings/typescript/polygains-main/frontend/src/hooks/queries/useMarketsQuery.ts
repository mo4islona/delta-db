"use client";
import { useEffect } from "react";
import useSWR from "swr";
import useSWRInfinite from "swr/infinite";
import { fetchMarket, fetchTopLiquidityMarkets } from "@/api/terminalApi";
import { swrKeys, type MarketsKey } from "@/hooks/swr/keys";
import { SWR_REFRESH } from "@/hooks/swr/options";
import type { MarketOutcome, Pagination } from "@/types/terminal";

export interface MarketsPagePayload {
	requestedPage: number;
	markets: MarketOutcome[];
	pagination: Pagination;
}

interface MarketsOptions {
	refreshInterval?: number;
	enabled?: boolean;
}

interface MarketsPagesOptions extends MarketsOptions {
	limit?: number;
	page?: number;
	initialSize?: number;
}

export function useMarketsQuery(
	page: number,
	limit: number,
	options: MarketsOptions = {},
) {
	const { refreshInterval = SWR_REFRESH.markets, enabled = true } = options;

	const query = useSWR(
		enabled ? swrKeys.markets(page, limit, false) : null,
		async ([, params]: MarketsKey): Promise<MarketsPagePayload> => {
			const response = await fetchTopLiquidityMarkets(
				params.page,
				params.limit,
				params.close,
			);
			return {
				requestedPage: params.page,
				markets: response.data,
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

export function useMarketsPagesQuery(options: MarketsPagesOptions = {}) {
	const {
		limit = 5,
		page = 1,
		initialSize = 1,
		refreshInterval = SWR_REFRESH.markets,
		enabled = true,
	} = options;

	const query = useSWRInfinite<MarketsPagePayload>(
		(
			pageIndex: number,
			previousPageData: MarketsPagePayload | null,
		): MarketsKey | null => {
			if (!enabled) return null;
			if (previousPageData && !previousPageData.pagination.hasNext) {
				return null;
			}
			return swrKeys.markets(pageIndex + 1, limit, false);
		},
		async (key: MarketsKey) => {
			const [, params] = key;
			const response = await fetchTopLiquidityMarkets(
				params.page,
				params.limit,
				params.close,
			);
			return {
				requestedPage: params.page,
				markets: response.data,
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

export function useMarketQuery(
	conditionId: string | null,
	options: { refreshInterval?: number; enabled?: boolean } = {},
) {
	const { refreshInterval = 0, enabled = true } = options;
	const query = useSWR(
		conditionId && enabled ? swrKeys.market(conditionId) : null,
		async ([, id]: readonly ["market", string]) => fetchMarket(id),
		{
			refreshInterval,
			revalidateOnFocus: false,
		},
	);

	return {
		market: query.data,
		...query,
	};
}
