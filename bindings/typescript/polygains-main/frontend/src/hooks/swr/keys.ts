"use client";
export const swrKeys = {
	health: () => ["health"] as const,
	insiderStats: () => ["stats", "insider"] as const,
	globalStats: () => ["stats", "global"] as const,
	categories: () => ["categories"] as const,
	alerts: (page: number, limit: number, category: string) =>
		["alerts", { page, limit, category }] as const,
	markets: (page: number, limit: number, close: boolean) =>
		["markets", { page, limit, close }] as const,
	market: (conditionId: string) => ["market", conditionId] as const,
	insiders: (page: number, limit: number) =>
		["insiders", { page, limit }] as const,
	insiderTrades: (address: string, page: number, limit: number) =>
		["insiderTrades", { address, page, limit }] as const,
};

export type AlertsKey = ReturnType<typeof swrKeys.alerts>;
export type MarketsKey = ReturnType<typeof swrKeys.markets>;
