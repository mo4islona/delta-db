"use client";
import type { SWRConfiguration } from "swr";
import { swrJsonFetcher } from "@/hooks/swr/fetcher";
import { SWR_DEFAULTS } from "@/hooks/swr/options";

export const appSWRConfig: SWRConfiguration = {
	fetcher: swrJsonFetcher,
	revalidateOnFocus: false,
	revalidateOnReconnect: true,
	shouldRetryOnError: true,
	errorRetryCount: SWR_DEFAULTS.errorRetryCount,
	errorRetryInterval: SWR_DEFAULTS.errorRetryInterval,
	dedupingInterval: SWR_DEFAULTS.dedupingInterval,
	keepPreviousData: true,
};
