export interface Pagination {
	page: number;
	limit: number;
	total: number;
	totalPages: number;
	hasPrev: boolean;
	hasNext: boolean;
}

export interface InsiderStats {
	total_insiders: number;
	yes_insiders: number;
	no_insiders: number;
	total_volume: number;
	current_block: number;
}

export interface GlobalStats {
	total_accounts: number;
	total_markets: number;
	total_trades: number;
	active_positions: number;
}

export interface AlertItem {
	price: number;
	user: string;
	volume: number;
	alert_time: number;
	market_count: number;
	outcome: string | null;
	winner: boolean | null;
	closed: boolean | null;
	conditionId: string | null;
	question: string | null;
	tokenId: string | null;
	market_price: number;
	walletAddress?: string;
	category: string | null;
}

export interface AlertsResponse {
	data: AlertItem[];
	pagination: Pagination;
}

export interface InsiderTrade {
	position_id: string | null;
	condition_id: string | null;
	volume: number;
	question: string | null;
	outcome: string | null;
	price: number;
}

export interface MarketOutcome {
	conditionId: string | null;
	question: string;
	outcome: string | null;
	tokenId: string | null;
	position_id: string | null;
	total_trades: number;
	volume: number;
	last_price: number;
	total_market_vol: number;
	total_market_trades: number;
	hn_score: number;
	insider_trade_count: number;
	mean: number | null;
	stdDev: number | null;
	p95: number | null;
	closed: boolean | null;
}

export interface MarketsResponse {
	total: number;
	markets: MarketOutcome[];
}

export interface HealthResponse {
	status: string;
	current_block: number;
}

export interface AlertRowView {
	id: string;
	walletAddress: string;
	detectedAt: number;
	volume: string;
	trades: number;
	assetId: string;
	price: string;
	payout: string;
	outcome: string;
}
