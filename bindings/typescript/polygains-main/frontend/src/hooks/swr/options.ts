export const SWR_REFRESH = {
	health: 2_000,
	insiderStats: 2_000,
	globalStats: 5_000,
	alerts: 5_000,
	markets: 5_000,
	categories: 60_000,
} as const;

export const SWR_DEFAULTS = {
	dedupingInterval: 2_000,
	errorRetryCount: 2,
	errorRetryInterval: 2_000,
} as const;
