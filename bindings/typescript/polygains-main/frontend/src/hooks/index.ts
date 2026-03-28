/**
 * Hooks index - Export all custom hooks
 */

// Query hooks
export { useHealthQuery } from "./queries/useHealthQuery";
export {
	useInsiderStatsQuery,
	useGlobalStatsQuery,
} from "./queries/useStatsQueries";
export { useAlertsQuery, useAlertsPagesQuery } from "./queries/useAlertsQuery";
export {
	useMarketsQuery,
	useMarketsPagesQuery,
	useMarketQuery,
} from "./queries/useMarketsQuery";
export { useCategoriesQuery } from "./queries/useCategoriesQuery";
export {
	useInsidersQuery,
	useInsiderTradesQuery,
} from "./queries/useInsidersQuery";
