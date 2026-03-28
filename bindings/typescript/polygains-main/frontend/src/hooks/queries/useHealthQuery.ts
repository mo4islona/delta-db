"use client";
import useSWR from "swr";
import { fetchHealth } from "@/api/terminalApi";
import { SWR_REFRESH } from "@/hooks/swr/options";
import { swrKeys } from "@/hooks/swr/keys";

interface UseHealthOptions {
	refreshInterval?: number;
	enabled?: boolean;
}

export function useHealthQuery(options: UseHealthOptions = {}) {
	const { refreshInterval = SWR_REFRESH.health, enabled = true } = options;

	const query = useSWR(enabled ? swrKeys.health() : null, fetchHealth, {
		refreshInterval,
		revalidateOnFocus: false,
	});

	return {
		health: query.data,
		...query,
	};
}
