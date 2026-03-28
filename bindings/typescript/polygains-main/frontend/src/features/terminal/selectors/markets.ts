import type { GroupedMarket, MarketOutcome } from "@/types/terminal";

function isResolvedMarket(market: GroupedMarket): boolean {
	if (market.closed) return true;
	if (market.outcomes.length === 0) return false;
	return market.outcomes.every((outcome) => Boolean(outcome.closed));
}

export function groupMarkets(outcomes: MarketOutcome[]): GroupedMarket[] {
	const grouped = new Map<string, GroupedMarket>();

	for (const market of outcomes) {
		const key =
			market.conditionId || `${market.question}-${String(market.outcome)}`;
		const existing = grouped.get(key);

		if (!existing) {
			grouped.set(key, {
				conditionId: market.conditionId || key,
				question: market.question,
				totalMarketVol: Number(market.total_market_vol || 0),
				totalMarketTrades: Number(market.total_market_trades || 0),
				hnScore: Number(market.hn_score || 0),
				closed: Boolean(market.closed),
				outcomes: [market],
			});
			continue;
		}

		existing.closed = existing.closed || Boolean(market.closed);
		existing.outcomes.push(market);
	}

	return Array.from(grouped.values())
		.filter((market) => !isResolvedMarket(market))
		.sort((a, b) => {
			const byActivity = (b.hnScore || 0) - (a.hnScore || 0);
			if (byActivity !== 0) return byActivity;

			const byTrades = (b.totalMarketTrades || 0) - (a.totalMarketTrades || 0);
			if (byTrades !== 0) return byTrades;

			return (b.totalMarketVol || 0) - (a.totalMarketVol || 0);
		});
}
