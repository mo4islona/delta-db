"use client";
import useSWR from "swr";
import { fetchInsiderTradesPage, fetchInsiders } from "@/api/terminalApi";
import { swrKeys } from "@/hooks/swr/keys";

interface UseInsidersOptions {
	page?: number;
	limit?: number;
	enabled?: boolean;
	refreshInterval?: number;
}

export function useInsidersQuery(options: UseInsidersOptions = {}) {
	const {
		page = 1,
		limit = 10,
		enabled = true,
		refreshInterval = 30_000,
	} = options;

	const query = useSWR(
		enabled ? swrKeys.insiders(page, limit) : null,
		async ([, params]: readonly ["insiders", { page: number; limit: number }]) =>
			fetchInsiders(params.page, params.limit),
		{
			refreshInterval,
			revalidateOnFocus: false,
		},
	);

	return {
		insiders: query.data?.data ?? [],
		pagination: query.data?.pagination,
		...query,
	};
}

interface UseInsiderTradesOptions {
	address: string | null;
	page?: number;
	limit?: number;
	enabled?: boolean;
	refreshInterval?: number;
}

export function useInsiderTradesQuery(options: UseInsiderTradesOptions) {
	const {
		address,
		page = 1,
		limit = 10,
		enabled = true,
		refreshInterval = 30_000,
	} = options;

	const query = useSWR(
		address && enabled ? swrKeys.insiderTrades(address, page, limit) : null,
		async ([, params]: readonly [
			"insiderTrades",
			{ address: string; page: number; limit: number },
		]) => fetchInsiderTradesPage(params.address, params.page, params.limit),
		{
			refreshInterval,
			revalidateOnFocus: false,
		},
	);

	return {
		trades: query.data?.data ?? [],
		pagination: query.data?.pagination,
		...query,
	};
}
