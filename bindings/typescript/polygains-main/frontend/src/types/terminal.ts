import type {
	AlertItem as SharedAlertItem,
	GlobalStats as SharedGlobalStats,
	HealthResponse as SharedHealthResponse,
	InsiderStats as SharedInsiderStats,
	InsiderTrade as SharedInsiderTrade,
	MarketOutcome as SharedMarketOutcome,
	Pagination as SharedPagination,
} from "@shared/api";
import type { AlertRowView } from "./api";

export type Pagination = SharedPagination;
export type HealthResponse = SharedHealthResponse;
export type InsiderStats = SharedInsiderStats;
export type GlobalStats = SharedGlobalStats;
export type AlertItem = SharedAlertItem;
export type InsiderTrade = SharedInsiderTrade;
export type MarketOutcome = SharedMarketOutcome;

export interface AlertsResponse {
	data: AlertItem[];
	pagination: Pagination;
}

export interface MarketsResponse {
	data: MarketOutcome[];
	pagination: Pagination;
}

export interface GroupedMarket {
	conditionId: string;
	question: string;
	closed: boolean;
	outcomes: MarketOutcome[];
	totalMarketVol?: number;
	totalMarketTrades?: number;
	hnScore?: number;
}

export interface TrackerState {
	totalBet: number;
	openInterest: number;
	realizedPnL: number;
	liveTrades: number;
	liveWins: number;
	liveLosses: number;
	alertsPage?: number;
	alertsTotalPages?: number;
	alertsFilledThroughPage?: number;
	liveTotalBet: number;
}

export interface PendingAlert {
	id: string;
	trader: string;
	detectedAt: number;
	volume: number;
	outcome: string;
	price: number;
	marketQuestion: string;
	cost: number;
	mode: StrategyMode;
	betSizing: BetSizing;
	conditionId: string | null;
	tokenId: string | null;
	user: string;
	alert_time: number;
}

export type StrategyMode = "reverse_insider" | "follow_insider";
export type BetSizing = "target_payout" | "fixed_stake";
export type WinnerFilter = "BOTH" | "WINNERS" | "LOSERS";

export interface SyncState {
	label: string;
	healthy: boolean;
	block: string;
}

export interface FloatingCash {
	id: number;
	text: string;
	isLoss: boolean;
	offset: number;
}

export const EMPTY_PAGINATION: Pagination = {
	page: 1,
	limit: 10,
	total: 0,
	totalPages: 0,
	hasPrev: false,
	hasNext: false,
};

// ============================================================================
// SECTION PROPS
// ============================================================================

export interface HeaderProps {
	currentBlock: string;
	syncLabel: string;
	syncHealthy: boolean;
}

export interface TerminalIntroProps {
	text: string;
	totalInsiders?: number;
	yesInsiders?: number;
	noInsiders?: number;
	insiderVolume?: string;
	backtestPnl?: number;
	backtestTotalBet?: number;
	backtestTrades?: number;
	backtestWins?: number;
	backtestLosses?: number;
	backtestSeries?: number[];
}

export interface LiveTrackerControlsProps {
	minPrice: number;
	maxPrice: number;
	onlyBetOnce: boolean;
	betOneDollarPerTrade: boolean;
	disabled?: boolean;
	selectedStrategies: Array<"reverse_insider" | "follow_insider">;
	selectedSides: string[];
	onMinPriceChange: (value: number) => void;
	onMaxPriceChange: (value: number) => void;
	onOnlyBetOnceChange: (value: boolean) => void;
	onBetOneDollarPerTradeChange: (value: boolean) => void;
	onStrategyChange: (
		mode: "reverse_insider" | "follow_insider",
		enabled: boolean,
	) => void;
	onSideToggle: (side: string, enabled: boolean) => void;
}

export interface LiveTrackerCardsProps {
	totalBet: number;
	openInterest: number;
	realizedPnL: number;
	liveTrades: number;
	liveWins: number;
	liveLosses: number;
	alertsPage: number;
	alertsTotalPages: number;
	alertsFilledThroughPage: number;
	backtestCanContinue: boolean;
	backtestRunning: boolean;
	onRunBacktest: () => void;
}

export interface CategoryOption {
	name: string;
	count: number;
	enabled: boolean;
	displayName: string;
}

export interface AlertsSectionProps {
	rows: AlertRowView[];
	pagination: Pagination;
	selectedCategory: string;
	selectedWinnerFilter: "BOTH" | "WINNERS" | "LOSERS";
	categoryOptions: CategoryOption[];
	allCategoryOptions: CategoryOption[];
	isLoading?: boolean;
	onPrev: () => void;
	onNext: () => void;
	onCategoryChange: (value: string) => void;
	onWinnerFilterChange: (value: "BOTH" | "WINNERS" | "LOSERS") => void;
}

export interface DetectionSectionProps {
	totalInsiders: number;
	yesInsiders: number;
	noInsiders: number;
	insiderVolume: string;
}

export interface MarketsSectionProps {
	markets: GroupedMarket[];
	pagination: Pagination;
	isLoading?: boolean;
	marketStatsLoadingByCondition?: Record<string, boolean>;
	onPrev: () => void;
	onNext: () => void;
}

export interface GlobalStatsSectionProps {
	accounts: string;
	markets: string;
	trades: string;
	activePositions: string;
}

export interface BannerProps {
	currentBlock: string;
}
