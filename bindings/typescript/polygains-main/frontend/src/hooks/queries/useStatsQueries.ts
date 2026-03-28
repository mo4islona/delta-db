"use client";
import useSWR from "swr";
import { fetchGlobalStats, fetchInsiderStats } from "@/api/terminalApi";
import { swrKeys } from "@/hooks/swr/keys";
import { SWR_REFRESH } from "@/hooks/swr/options";

interface StatsOptions {
	refreshInterval?: number;
	enabled?: boolean;
}

export function useInsiderStatsQuery(options: StatsOptions = {}) {
	const { refreshInterval = SWR_REFRESH.insiderStats, enabled = true } = options;

	const query = useSWR(enabled ? swrKeys.insiderStats() : null, fetchInsiderStats, {
		refreshInterval,
		revalidateOnFocus: false,
	});

	return {
		stats: query.data,
		...query,
	};
}

export function useGlobalStatsQuery(options: StatsOptions = {}) {
	const { refreshInterval = SWR_REFRESH.globalStats, enabled = true } = options;

	const query = useSWR(enabled ? swrKeys.globalStats() : null, fetchGlobalStats, {
		refreshInterval,
		revalidateOnFocus: false,
	});

	return {
		stats: query.data,
		...query,
	};
}
