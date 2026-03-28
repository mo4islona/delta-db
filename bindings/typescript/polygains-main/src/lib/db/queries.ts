import { and, desc, eq, inArray, isNotNull, sql } from "drizzle-orm";
import {
	accountWalletMap,
	checkpoint,
	insiderPositions,
	markets as marketsTable,
	marketTokens,
	tokenMarketLookup,
	tokenStats,
	vBaseTokenMarketInfo,
	vInsidersEnriched,
	vMarketSummary,
} from "@/lib/db/schema";
import { db } from "./init";

const parseCursorNumber = (value: unknown): number | undefined => {
	if (typeof value === "number" && Number.isFinite(value) && value > 0) {
		return value;
	}

	if (typeof value === "string") {
		const parsed = Number.parseInt(value, 10);
		if (Number.isFinite(parsed) && parsed > 0) {
			return parsed;
		}
		return undefined;
	}

	if (value && typeof value === "object" && "number" in value) {
		return parseCursorNumber((value as { number?: unknown }).number);
	}

	return undefined;
};

const parseStatOrNull = (value: unknown): number | null => {
	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : null;
};

const parsePositiveStatOrNull = (value: unknown): number | null => {
	const parsed = parseStatOrNull(value);
	if (parsed === null || parsed <= 0) {
		return null;
	}
	return parsed;
};

const parseInt32HashLookup = (value: string): number | null => {
	let normalized = value.trim();

	// Support "u32=1234" format if needed, but primarily handle direct numbers
	if (normalized.startsWith("u32=")) {
		normalized = normalized.slice(4);
	}

	// Handle signed and unsigned decimal strings
	if (!/^-?\d+$/.test(normalized)) {
		return null;
	}

	try {
		const parsed = BigInt(normalized);
		// Validate range (allow both signed int32 and unsigned uint32 ranges)
		if (parsed < -2147483648n || parsed > 4294967295n) {
			return null;
		}
		// Always return as signed 32-bit integer for DB compatibility
		return Number(BigInt.asIntN(32, parsed));
	} catch {
		return null;
	}
};

const normalizeWalletAddress = (value: string): string | null => {
	const normalized = value.trim().toLowerCase();
	if (!/^0x[a-f0-9]{40}$/.test(normalized)) {
		return null;
	}
	return normalized;
};

const parseCategoryFromTags = (
	rawTags: string | null | undefined,
): string | null => {
	if (!rawTags) return null;
	const tags = rawTags
		.split(",")
		.map((tag) => tag.trim())
		.filter((tag) => tag.length > 0);
	if (tags.length === 0) return null;
	const nonAll = tags.find((tag) => tag.toLowerCase() !== "all");
	return nonAll ?? tags[0] ?? null;
};

const ALERT_MAX_TRIGGER_PRICE = 0.95;
const ALERT_PRICE_EPSILON = 0.000001;

const parseWinnerValue = (value: unknown): boolean | null => {
	if (typeof value === "boolean") return value;
	if (typeof value === "number") return value !== 0;
	if (typeof value === "string") {
		const normalized = value.trim().toLowerCase();
		if (normalized === "true" || normalized === "1" || normalized === "yes") {
			return true;
		}
		if (normalized === "false" || normalized === "0" || normalized === "no") {
			return false;
		}
	}
	return null;
};

const inferResolvedWinner = (
	rawResolvedWinner: unknown,
	rawLastPrice: unknown,
): boolean | null => {
	const parsedWinner = parseWinnerValue(rawResolvedWinner);
	if (parsedWinner !== null) {
		return parsedWinner;
	}

	const lastPrice = Number(rawLastPrice);
	if (Number.isFinite(lastPrice)) {
		if (lastPrice >= 0.98) return true;
		if (lastPrice <= 0.05) return false;
	}
	return null;
};

const getStateFileBlock = async (): Promise<number> => {
	try {
		const stateFile = Bun.file("state.json");
		if (!(await stateFile.exists())) return 0;

		const content = await stateFile.text();
		if (!content.trim()) return 0;

		try {
			const parsed = JSON.parse(content);
			return parseCursorNumber(parsed) ?? 0;
		} catch {
			return parseCursorNumber(content.trim()) ?? 0;
		}
	} catch {
		return 0;
	}
};

// --- INSIDER QUERIES ---

export async function getCurrentBlock() {
	const row = await db
		.select({
			currentNumber: sql<number>`CAST(${checkpoint.currentNumber} AS DOUBLE PRECISION)`,
		})
		.from(checkpoint)
		.orderBy(desc(checkpoint.currentNumber))
		.limit(1);

	const dbBlock = row[0]?.currentNumber;
	if (typeof dbBlock === "number" && Number.isFinite(dbBlock) && dbBlock > 0) {
		return dbBlock;
	}

	return await getStateFileBlock();
}

export async function getInsiderStats() {
	const currentBlock = await getCurrentBlock();

	const statsResult = await db
		.select({
			total_insiders: sql<number>`CAST(count(distinct ${vInsidersEnriched.account}) AS DOUBLE PRECISION)`,
			yes_insiders: sql<number>`CAST(count(distinct CASE WHEN ${vInsidersEnriched.outcome} = 'Yes' THEN ${vInsidersEnriched.account} END) AS DOUBLE PRECISION)`,
			no_insiders: sql<number>`CAST(count(distinct CASE WHEN ${vInsidersEnriched.outcome} = 'No' THEN ${vInsidersEnriched.account} END) AS DOUBLE PRECISION)`,
			total_volume: sql<number>`CAST(coalesce(sum(${vInsidersEnriched.volume}), 0) AS DOUBLE PRECISION)`,
		})
		.from(vInsidersEnriched);

	const stats = statsResult[0] || {
		total_insiders: 0,
		yes_insiders: 0,
		no_insiders: 0,
		total_volume: 0,
	};

	return {
		total_insiders: Number(stats.total_insiders),
		yes_insiders: Number(stats.yes_insiders),
		no_insiders: Number(stats.no_insiders),
		total_volume: Number(stats.total_volume),
		current_block: currentBlock,
	};
}

export async function getInsiderAlerts(
	limit: number,
	offset: number,
	category?: string,
) {
	const resolvedWinnerByCondition = db
		.select({
			conditionId: marketTokens.marketConditionId,
			winningTokenId: sql<
				string | null
			>`max(case when ${marketTokens.winner} then ${marketTokens.tokenId}::text end)`.as(
				"winning_token_id",
			),
			winnerCount:
				sql<number>`sum(case when ${marketTokens.winner} then 1 else 0 end)`.as(
					"winner_count",
				),
		})
		.from(marketTokens)
		.groupBy(marketTokens.marketConditionId)
		.as("resolved_winner_by_condition");

	const triggerPriceFilter = sql`coalesce(${vInsidersEnriched.alertPrice}, ${vInsidersEnriched.lastPrice}, 0) <= ${ALERT_MAX_TRIGGER_PRICE + ALERT_PRICE_EPSILON}`;
	const normalizedCategory = category?.trim();
	const categoryFilter = normalizedCategory
		? sql`coalesce(${vInsidersEnriched.outcomeTags}, '') ilike ${`%${normalizedCategory}%`}`
		: undefined;
	const combinedFilter = categoryFilter
		? and(triggerPriceFilter, categoryFilter)
		: triggerPriceFilter;

	const countBase = db
		.select({ count: sql<number>`CAST(count(*) AS DOUBLE PRECISION)` })
		.from(vInsidersEnriched);
	const countResult = await countBase.where(combinedFilter);
	const total = Number(countResult[0]?.count || 0);

	const insidersBase = db
		.select({
			account: vInsidersEnriched.account,
			volume: vInsidersEnriched.volume,
			detectedAt: sql<
				number | null
			>`CAST(${vInsidersEnriched.detectedAt} AS DOUBLE PRECISION)`,
			marketCount: vInsidersEnriched.marketCount,
			outcome: vInsidersEnriched.outcome,
			winner: vInsidersEnriched.winner,
			closed: vInsidersEnriched.closed,
			conditionId: vInsidersEnriched.conditionId,
			question: vInsidersEnriched.question,
			tokenId: vInsidersEnriched.tokenId,
			alertPrice: vInsidersEnriched.alertPrice,
			lastPrice: vInsidersEnriched.lastPrice,
			resolvedWinner: sql<boolean | null>`
					case
						when ${vInsidersEnriched.closed} is true
							and ${resolvedWinnerByCondition.winnerCount} = 1
							and ${resolvedWinnerByCondition.winningTokenId} is not null
							then (${vInsidersEnriched.tokenId}::text = ${resolvedWinnerByCondition.winningTokenId})
						else null
					end
				`,
			walletAddress: accountWalletMap.walletAddress,
			marketTags: vInsidersEnriched.outcomeTags,
		})
		.from(vInsidersEnriched)
		.leftJoin(
			accountWalletMap,
			eq(vInsidersEnriched.account, accountWalletMap.accountHash),
		)
		.leftJoin(
			resolvedWinnerByCondition,
			eq(vInsidersEnriched.conditionId, resolvedWinnerByCondition.conditionId),
		);
	const scopedInsidersBase = insidersBase.where(combinedFilter);
	const insiders = await scopedInsidersBase
		.limit(limit)
		.offset(offset)
		.orderBy(desc(vInsidersEnriched.detectedAt));

	const alerts = insiders.map((insider) => ({
		price: Number((insider.alertPrice ?? insider.lastPrice) || 0),
		user: String(insider.walletAddress || insider.account),
		volume: Number(insider.volume || 0),
		alert_time: insider.detectedAt ? Number(insider.detectedAt) / 1000 : 0,
		market_count: Number(insider.marketCount || 0),
		outcome: insider.outcome,
		winner: inferResolvedWinner(insider.resolvedWinner, insider.lastPrice),
		closed: insider.closed,
		conditionId: insider.conditionId,
		question: insider.question,
		tokenId: insider.tokenId,
		market_price: Number(insider.lastPrice || 0),
		walletAddress: insider.walletAddress || undefined,
		category: parseCategoryFromTags(insider.marketTags),
	}));

	return { total, alerts };
}

export async function getInsiderTrades(address: string) {
	let accountHash = parseInt32HashLookup(address);
	if (accountHash === null) {
		const walletAddress = normalizeWalletAddress(address);
		if (!walletAddress) {
			return [];
		}

		const accountMapping = await db
			.select({
				accountHash: accountWalletMap.accountHash,
			})
			.from(accountWalletMap)
			.where(sql`lower(${accountWalletMap.walletAddress}) = ${walletAddress}`)
			.limit(1);
		accountHash = Number(accountMapping[0]?.accountHash);
	}
	if (accountHash === null || !Number.isFinite(accountHash)) return [];

	const insiders = await db
		.select({
			tokenId: vInsidersEnriched.tokenId,
			conditionId: vInsidersEnriched.conditionId,
			volume: vInsidersEnriched.volume,
			question: vInsidersEnriched.question,
			outcome: vInsidersEnriched.outcome,
			alertPrice: vInsidersEnriched.alertPrice,
			lastPrice: vInsidersEnriched.lastPrice,
		})
		.from(vInsidersEnriched)
		.where(eq(vInsidersEnriched.account, accountHash | 0));

	return insiders.map((insider) => ({
		position_id: insider.tokenId,
		condition_id: insider.conditionId,
		volume: Number(insider.volume || 0),
		question: insider.question,
		outcome: insider.outcome,
		price: Number((insider.alertPrice ?? insider.lastPrice) || 0),
	}));
}

export async function getInsidersList() {
	const insiders = await db
		.select({
			account: vInsidersEnriched.account,
			volume: vInsidersEnriched.volume,
			detectedAt: sql<
				number | null
			>`CAST(${vInsidersEnriched.detectedAt} AS DOUBLE PRECISION)`,
			tokenId: vInsidersEnriched.tokenId,
			conditionId: vInsidersEnriched.conditionId,
			lastPrice: vInsidersEnriched.lastPrice,
			marketTotalVolume: vInsidersEnriched.marketTotalVolume,
		})
		.from(vInsidersEnriched)
		.limit(50)
		.orderBy(desc(vInsidersEnriched.detectedAt));

	return insiders.map((insider) => ({
		account: String(insider.account),
		insider_volume: Number(insider.volume || 0),
		detected_at: insider.detectedAt
			? new Date(Number(insider.detectedAt))
					.toISOString()
					.replace("T", " ")
					.split(".")[0]
			: null,
		token_id: insider.tokenId,
		condition_id: insider.conditionId,
		market_price: Number(insider.lastPrice || 0),
		market_total_volume: Number(insider.marketTotalVolume || 0),
	}));
}

// --- MARKET QUERIES ---

export async function getMarkets(
	limit: number,
	offset: number,
	closed?: boolean,
) {
	const now = Date.now();
	const closedFilter =
		closed === undefined ? undefined : eq(vMarketSummary.closed, closed);
	const activeFilter = eq(marketsTable.active, true);
	const marketFilter = closedFilter
		? and(activeFilter, closedFilter)
		: activeFilter;
	const marketVolumesBase = db
		.select({
			conditionId: vMarketSummary.conditionId,
			totalMarketVol: sql<number>`CAST(sum(coalesce(${vMarketSummary.totalVol}, 0)::double precision) AS DOUBLE PRECISION)`,
			totalMarketTrades: sql<number>`CAST(sum(coalesce(${vMarketSummary.totalTrades}, 0)) AS DOUBLE PRECISION)`,
			hnScore: sql<number>`
					(sum(coalesce(${vMarketSummary.totalVol}, 0)::double precision) - 1.0) /
					power(
						((CAST(${now} AS DOUBLE PRECISION) - MIN(${vMarketSummary.createdAt})::double precision) / 3600000.0) + 2.0,
						1.8
					)
				`.as("hn_score"),
		})
		.from(vMarketSummary)
		.leftJoin(
			marketsTable,
			eq(vMarketSummary.conditionId, marketsTable.conditionId),
		)
		.where(marketFilter);
	const totalCountBase = db
		.select({
			count: sql<number>`CAST(count(distinct ${vMarketSummary.conditionId}) AS DOUBLE PRECISION)`,
		})
		.from(vMarketSummary)
		.leftJoin(
			marketsTable,
			eq(vMarketSummary.conditionId, marketsTable.conditionId),
		)
		.where(marketFilter);
	const [marketVolumes, totalResult] = await Promise.all([
		marketVolumesBase
			.groupBy(vMarketSummary.conditionId)
			.orderBy(desc(sql`hn_score`))
			.limit(limit)
			.offset(offset),
		totalCountBase,
	]);
	const total = Number(totalResult[0]?.count || 0);

	if (marketVolumes.length === 0) {
		return { total, markets: [] };
	}

	const conditionIds = marketVolumes
		.map((m) => m.conditionId)
		.filter((id): id is string => id !== null);

	const conditionFilter = inArray(vMarketSummary.conditionId, conditionIds);
	const allOutcomesFilter = closedFilter
		? and(conditionFilter, closedFilter)
		: conditionFilter;

	const allOutcomes = await db
		.select({
			conditionId: vMarketSummary.conditionId,
			question: vMarketSummary.question,
			outcome: vMarketSummary.outcome,
			tokenId: vMarketSummary.tokenId,
			totalTrades: sql<number>`CAST(coalesce(${vMarketSummary.totalTrades}, 0) AS DOUBLE PRECISION)`,
			totalVol: sql<number>`CAST(coalesce(${vMarketSummary.totalVol}, 0) AS DOUBLE PRECISION)`,
			lastPrice: sql<number>`CAST(coalesce(${vMarketSummary.lastPrice}, 0) AS DOUBLE PRECISION)`,
			mean: sql<
				number | null
			>`CAST(${vMarketSummary.mean} AS DOUBLE PRECISION)`,
			stdDev: sql<
				number | null
			>`CAST(${vMarketSummary.stdDev} AS DOUBLE PRECISION)`,
			p95: sql<number | null>`CAST(${vMarketSummary.p95} AS DOUBLE PRECISION)`,
			closed: vMarketSummary.closed,
		})
		.from(vMarketSummary)
		.where(allOutcomesFilter);

	const tokenIds = Array.from(
		new Set(
			allOutcomes
				.map((outcome) => outcome.tokenId)
				.filter((id): id is string => id !== null),
		),
	);
	const insiderTradeCounts = tokenIds.length
		? await db
				.select({
					tokenId: insiderPositions.tokenId,
					insiderTradeCount: sql<number>`CAST(coalesce(sum(${insiderPositions.tradeCount}), 0) AS DOUBLE PRECISION)`,
				})
				.from(insiderPositions)
				.where(inArray(insiderPositions.tokenId, tokenIds))
				.groupBy(insiderPositions.tokenId)
		: [];
	const insiderTradeCountByToken = new Map(
		insiderTradeCounts.map((entry) => [
			String(entry.tokenId),
			Number(entry.insiderTradeCount || 0),
		]),
	);

	const markets = allOutcomes.map((outcome) => {
		const marketTotal = marketVolumes.find(
			(mv) => mv.conditionId === outcome.conditionId,
		);
		const tokenIdKey = outcome.tokenId !== null ? String(outcome.tokenId) : "";
		return {
			conditionId: outcome.conditionId,
			question: outcome.question || outcome.conditionId,
			outcome: outcome.outcome,
			tokenId: outcome.tokenId,
			position_id: outcome.tokenId,
			total_trades: Number(outcome.totalTrades || 0),
			volume: Number(outcome.totalVol || 0),
			last_price: Number(outcome.lastPrice || 0),
			total_market_vol: Number(marketTotal?.totalMarketVol || 0),
			total_market_trades: Number(marketTotal?.totalMarketTrades || 0),
			hn_score: Number(marketTotal?.hnScore || 0),
			insider_trade_count: insiderTradeCountByToken.get(tokenIdKey) ?? 0,
			mean: outcome.mean !== null ? Number(outcome.mean) : null,
			stdDev: outcome.stdDev !== null ? Number(outcome.stdDev) : null,
			p95: parsePositiveStatOrNull(outcome.p95),
			closed: outcome.closed,
		};
	});

	return { total, markets };
}

export async function getMarketByCondition(conditionId: string) {
	const allOutcomes = await db
		.select({
			conditionId: vBaseTokenMarketInfo.conditionId,
			question: vBaseTokenMarketInfo.question,
			closed: vBaseTokenMarketInfo.closed,
			tokenId: vBaseTokenMarketInfo.tokenId,
			outcome: vBaseTokenMarketInfo.outcome,
			winner: vBaseTokenMarketInfo.winner,
			lastPrice: sql<number>`CAST(coalesce(${tokenStats.lastPrice}, 0) AS DOUBLE PRECISION)`,
			totalTrades: sql<number>`CAST(coalesce(${tokenStats.totalTrades}, 0) AS DOUBLE PRECISION)`,
			totalVol: sql<number>`CAST(coalesce(${tokenStats.totalVol}, 0) AS DOUBLE PRECISION)`,
			mean: sql<number | null>`CAST(${tokenStats.mean} AS DOUBLE PRECISION)`,
			stdDev: sql<
				number | null
			>`CAST(${tokenStats.stdDev} AS DOUBLE PRECISION)`,
			p95: sql<number | null>`CAST(${tokenStats.p95} AS DOUBLE PRECISION)`,
		})
		.from(vBaseTokenMarketInfo)
		.leftJoin(tokenStats, eq(vBaseTokenMarketInfo.tokenId, tokenStats.token))
		.where(eq(vBaseTokenMarketInfo.conditionId, conditionId));

	if (allOutcomes.length === 0) return null;

	const first = allOutcomes[0];
	if (!first) return null;
	return {
		conditionId: first.conditionId,
		question: first.question,
		closed: first.closed,
		outcomes: allOutcomes.map((o) => ({
			tokenId: o.tokenId,
			outcome: o.outcome,
			winner: o.winner ?? null,
			lastPrice: Number(o.lastPrice || 0),
			totalTrades: Number(o.totalTrades || 0),
			volume: Number(o.totalVol || 0),
			mean: o.mean !== null ? Number(o.mean) : null,
			stdDev: o.stdDev !== null ? Number(o.stdDev) : null,
			p95: parsePositiveStatOrNull(o.p95),
		})),
	};
}

export async function getGlobalStats() {
	const accountsResult = await db
		.select({
			count: sql<number>`CAST(count(distinct ${insiderPositions.accountHash}) AS DOUBLE PRECISION)`,
		})
		.from(insiderPositions);
	const total_accounts = Number(accountsResult[0]?.count || 0);

	const marketsResult = await db
		.select({
			count: sql<number>`CAST(count(distinct ${tokenMarketLookup.conditionId}) AS DOUBLE PRECISION)`,
		})
		.from(tokenMarketLookup)
		.where(isNotNull(tokenMarketLookup.conditionId));
	const total_markets = Number(marketsResult[0]?.count || 0);

	const tokenStatsResult = await db
		.select({
			total_trades: sql<number>`CAST(sum(coalesce(${tokenStats.totalTrades}, 0)) AS DOUBLE PRECISION)`,
		})
		.from(tokenStats)
		.where(sql`${tokenStats.totalTrades} > 0`);

	const total_trades = Number(tokenStatsResult[0]?.total_trades || 0);

	const activePositionsResult = await db
		.select({ count: sql<number>`CAST(count(*) AS DOUBLE PRECISION)` })
		.from(insiderPositions);
	const active_positions = Number(activePositionsResult[0]?.count || 0);

	return {
		total_accounts,
		total_markets,
		total_trades,
		active_positions,
	};
}

export async function getCategories() {
	const result = await db
		.selectDistinct({ tags: marketsTable.outcomeTags })
		.from(marketsTable)
		.where(isNotNull(marketsTable.outcomeTags));

	const categories = new Set<string>();
	categories.add("ALL");

	for (const row of result) {
		if (!row.tags) continue;
		const tags = row.tags.split(",");
		for (const tag of tags) {
			const cleanTag = tag.trim().toUpperCase();
			if (cleanTag && cleanTag !== "ALL") {
				categories.add(cleanTag);
			}
		}
	}

	return Array.from(categories).sort((a, b) => {
		if (a === "ALL") return -1;
		if (b === "ALL") return 1;
		return a.localeCompare(b);
	});
}

// Category to priority group mapping
const CATEGORY_GROUPS: Record<string, string> = {
	// Politics group
	POLITICS: "Politics",
	POLITICAL: "Politics",
	ELECTION: "Politics",
	ELECTIONS: "Politics",
	GOVERNMENT: "Politics",
	TRUMP: "Politics",
	BIDEN: "Politics",
	CONGRESS: "Politics",
	SENATE: "Politics",
	HOUSE: "Politics",
	VOTE: "Politics",
	VOTING: "Politics",
	CAMPAIGN: "Politics",
	// Sports group
	SPORTS: "Sports",
	SPORT: "Sports",
	NFL: "Sports",
	NBA: "Sports",
	MLB: "Sports",
	NHL: "Sports",
	SOCCER: "Sports",
	FOOTBALL: "Sports",
	BASEBALL: "Sports",
	BASKETBALL: "Sports",
	TENNIS: "Sports",
	GOLF: "Sports",
	UFC: "Sports",
	MMA: "Sports",
	BOXING: "Sports",
	OLYMPICS: "Sports",
	// Crypto group
	CRYPTO: "Crypto",
	CRYPTOCURRENCY: "Crypto",
	BITCOIN: "Crypto",
	ETHEREUM: "Crypto",
	ETH: "Crypto",
	BTC: "Crypto",
	BLOCKCHAIN: "Crypto",
	DEFI: "Crypto",
	NFT: "Crypto",
	NFTS: "Crypto",
	WEB3: "Crypto",
	// Finance group
	FINANCE: "Finance",
	FINANCIAL: "Finance",
	STOCKS: "Finance",
	STOCK: "Finance",
	EQUITY: "Finance",
	EQUITIES: "Finance",
	TRADING: "Finance",
	INVESTMENT: "Finance",
	INVESTING: "Finance",
	BANKING: "Finance",
	BANK: "Finance",
	// Geopolitics group
	GEOPOLITICS: "Geopolitics",
	GEOPOLITICAL: "Geopolitics",
	INTERNATIONAL: "Geopolitics",
	FOREIGN: "Geopolitics",
	DIPLOMACY: "Geopolitics",
	WAR: "Geopolitics",
	CONFLICT: "Geopolitics",
	MILITARY: "Geopolitics",
	// Earnings group
	EARNINGS: "Earnings",
	EARNINGS_REPORT: "Earnings",
	PROFIT: "Earnings",
	REVENUE: "Earnings",
	QUARTERLY: "Earnings",
	QUARTER: "Earnings",
	Q1: "Earnings",
	Q2: "Earnings",
	Q3: "Earnings",
	Q4: "Earnings",
	// Tech group
	TECH: "Tech",
	TECHNOLOGY: "Tech",
	AI: "Tech",
	ARTIFICIAL_INTELLIGENCE: "Tech",
	MACHINE_LEARNING: "Tech",
	SOFTWARE: "Tech",
	HARDWARE: "Tech",
	SEMICONDUCTOR: "Tech",
	CHIP: "Tech",
	CHIPS: "Tech",
	TESLA: "Tech",
	APPLE: "Tech",
	GOOGLE: "Tech",
	AMAZON: "Tech",
	MICROSOFT: "Tech",
	// Culture group
	CULTURE: "Culture",
	ENTERTAINMENT: "Culture",
	MUSIC: "Culture",
	MOVIE: "Culture",
	MOVIES: "Culture",
	TV: "Culture",
	CELEBRITY: "Culture",
	CELEBRITIES: "Culture",
	AWARDS: "Culture",
	OSCAR: "Culture",
	OSCARS: "Culture",
	GRAMMY: "Culture",
	GRAMMYS: "Culture",
	// World group
	WORLD: "World",
	GLOBAL: "World",
	INTERNATIONAL_NEWS: "World",
	FOREIGN_AFFAIRS: "World",
	// Economy group
	ECONOMY: "Economy",
	ECONOMIC: "Economy",
	GDP: "Economy",
	INFLATION: "Economy",
	RECESSION: "Economy",
	FED: "Economy",
	FEDERAL_RESERVE: "Economy",
	INTEREST_RATES: "Economy",
	UNEMPLOYMENT: "Economy",
	JOBS: "Economy",
	LABOR: "Economy",
};

export interface CategoryCount {
	name: string;
	count: number;
	enabled: boolean;
	group?: string;
}

export async function getCategoriesWithCounts(): Promise<CategoryCount[]> {
	// Get all distinct tags with their counts from markets
	const result = await db
		.select({ tags: marketsTable.outcomeTags })
		.from(marketsTable)
		.where(isNotNull(marketsTable.outcomeTags));

	const categoryCounts = new Map<string, number>();
	const categoryGroups = new Map<string, string>();

	for (const row of result) {
		if (!row.tags) continue;
		const tags = row.tags.split(",");
		for (const tag of tags) {
			const cleanTag = tag.trim().toUpperCase();
			if (cleanTag && cleanTag !== "ALL") {
				categoryCounts.set(cleanTag, (categoryCounts.get(cleanTag) || 0) + 1);
				// Map to group if exists
				const group = CATEGORY_GROUPS[cleanTag];
				if (group) {
					categoryGroups.set(cleanTag, group);
				}
			}
		}
	}

	// Build result with counts and enabled status
	const categories: CategoryCount[] = [];
	
	// Add ALL first
	categories.push({ name: "ALL", count: 0, enabled: true });

	// Add categories with counts
	for (const [name, count] of categoryCounts) {
		categories.push({
			name,
			count,
			enabled: count > 0,
			group: categoryGroups.get(name),
		});
	}

	return categories.sort((a, b) => {
		if (a.name === "ALL") return -1;
		if (b.name === "ALL") return 1;
		return a.name.localeCompare(b.name);
	});
}

// --- OPTIMIZED QUERIES ---

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

/**
 * Optimized alerts query with pagination.
 */
export async function getInsiderAlertsOptimized(
	page = 1,
	limit = 10,
	category?: string,
): Promise<{ total: number; alerts: AlertItem[] }> {
	const safePage = Number.isFinite(page) ? Math.max(1, Math.floor(page)) : 1;
	const safeLimit = Number.isFinite(limit)
		? Math.max(1, Math.floor(limit))
		: 10;
	const offset = (safePage - 1) * safeLimit;
	const normalizedCategory = category?.trim();
	const triggerPriceFilter = sql`coalesce(${vInsidersEnriched.alertPrice}, ${vInsidersEnriched.lastPrice}, 0) <= ${ALERT_MAX_TRIGGER_PRICE + ALERT_PRICE_EPSILON}`;
	const categoryFilter = normalizedCategory
		? sql`coalesce(${vInsidersEnriched.outcomeTags}, '') ilike ${`%${normalizedCategory}%`}`
		: undefined;
	const combinedFilter = categoryFilter
		? and(triggerPriceFilter, categoryFilter)
		: triggerPriceFilter;

	const countPromise = db
		.select({ count: sql<number>`CAST(count(*) AS DOUBLE PRECISION)` })
		.from(vInsidersEnriched)
		.where(combinedFilter);

	const alertsPromise = db
		.select({
			account: vInsidersEnriched.account,
			volume: vInsidersEnriched.volume,
			detectedAt: sql<
				number | null
			>`CAST(${vInsidersEnriched.detectedAt} AS DOUBLE PRECISION)`,
			marketCount: vInsidersEnriched.marketCount,
			outcome: vInsidersEnriched.outcome,
			winner: vInsidersEnriched.winner,
			closed: vInsidersEnriched.closed,
			conditionId: vInsidersEnriched.conditionId,
			question: vInsidersEnriched.question,
			tokenId: vInsidersEnriched.tokenId,
			alertPrice: vInsidersEnriched.alertPrice,
			lastPrice: vInsidersEnriched.lastPrice,
			marketTags: vInsidersEnriched.outcomeTags,
			walletAddress: accountWalletMap.walletAddress,
		})
		.from(vInsidersEnriched)
		.leftJoin(
			accountWalletMap,
			eq(vInsidersEnriched.account, accountWalletMap.accountHash),
		)
		.where(combinedFilter)
		.orderBy(
			desc(vInsidersEnriched.detectedAt),
			desc(vInsidersEnriched.volume),
			desc(vInsidersEnriched.account),
		)
		.offset(offset)
		.limit(safeLimit);

	const [countResult, insiders] = await Promise.all([
		countPromise,
		alertsPromise,
	]);
	const total = Number(countResult[0]?.count || 0);

	const conditionIds = insiders
		.map((i) => i.conditionId)
		.filter((id): id is string => id !== null);

	let resolvedWinners: Map<
		string,
		{ tokenId: string | null; winnerCount: number }
	> = new Map();
	if (conditionIds.length > 0) {
		const winnerResult = await db
			.select({
				conditionId: marketTokens.marketConditionId,
				winningTokenId: sql<
					string | null
				>`max(case when ${marketTokens.winner} then ${marketTokens.tokenId}::text end)`,
				winnerCount: sql<number>`sum(case when ${marketTokens.winner} then 1 else 0 end)`,
			})
			.from(marketTokens)
			.where(inArray(marketTokens.marketConditionId, conditionIds))
			.groupBy(marketTokens.marketConditionId);

		resolvedWinners = new Map(
			winnerResult.map((r) => [
				r.conditionId,
				{ tokenId: r.winningTokenId, winnerCount: r.winnerCount },
			]),
		);
	}

	const alerts: AlertItem[] = insiders.map((insider) => {
		const resolvedWinnerInfo = insider.conditionId
			? resolvedWinners.get(insider.conditionId)
			: undefined;

		let winner: boolean | null = null;
		if (
			insider.closed &&
			resolvedWinnerInfo &&
			resolvedWinnerInfo.winnerCount === 1 &&
			resolvedWinnerInfo.tokenId &&
			insider.tokenId
		) {
			winner = insider.tokenId === resolvedWinnerInfo.tokenId;
		} else if (insider.lastPrice !== null) {
			const lp = Number(insider.lastPrice);
			if (lp >= 0.98) winner = true;
			else if (lp <= 0.05) winner = false;
		}

		return {
			price: Number((insider.alertPrice ?? insider.lastPrice) || 0),
			user: String(insider.walletAddress || insider.account),
			volume: Number(insider.volume || 0),
			alert_time: insider.detectedAt ? Number(insider.detectedAt) / 1000 : 0,
			market_count: Number(insider.marketCount || 0),
			outcome: insider.outcome,
			winner: winner,
			closed: insider.closed,
			conditionId: insider.conditionId,
			question: insider.question,
			tokenId: insider.tokenId,
			market_price: Number(insider.lastPrice || 0),
			walletAddress: insider.walletAddress || undefined,
			category: parseCategoryFromTags(insider.marketTags),
		};
	});

	return { total, alerts };
}

export interface OptimizedMarketOutcome {
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

/**
 * Optimized markets query with pagination and volume pre-filter.
 */
export async function getMarketsOptimized(
	page = 1,
	limit = 10,
	closed?: boolean,
): Promise<{ total: number; markets: OptimizedMarketOutcome[] }> {
	const safePage = Number.isFinite(page) ? Math.max(1, Math.floor(page)) : 1;
	const safeLimit = Number.isFinite(limit)
		? Math.max(1, Math.floor(limit))
		: 10;
	const offset = (safePage - 1) * safeLimit;
	const now = Date.now();
	const closedFilter =
		closed === undefined ? undefined : eq(vMarketSummary.closed, closed);
	const activeFilter = eq(marketsTable.active, true);
	const marketFilter = closedFilter
		? and(activeFilter, closedFilter)
		: activeFilter;

	const topMarketsSubquery = db
		.select({
			conditionId: vMarketSummary.conditionId,
			totalMarketVol:
				sql<number>`sum(coalesce(${vMarketSummary.totalVol}, 0))`.as(
					"total_market_vol",
				),
			totalMarketTrades:
				sql<number>`sum(coalesce(${vMarketSummary.totalTrades}, 0))`.as(
					"total_market_trades",
				),
			minCreatedAt: sql<number>`min(${vMarketSummary.createdAt})`.as(
				"min_created_at",
			),
		})
		.from(vMarketSummary)
		.leftJoin(
			marketsTable,
			eq(vMarketSummary.conditionId, marketsTable.conditionId),
		)
		.where(and(marketFilter, sql`${vMarketSummary.totalVol} > 0`))
		.groupBy(vMarketSummary.conditionId)
		.as("top_markets_subquery");

	const hnScoreSql = sql<number>`
		(${topMarketsSubquery.totalMarketVol} - 1.0) /
		POWER(
			((CAST(${now} AS DOUBLE PRECISION) - CAST(${topMarketsSubquery.minCreatedAt} AS DOUBLE PRECISION)) / 3600000.0) + 2.0,
			1.8
		)
	`.as("hn_score");

	const marketVolumes = await db
		.select({
			conditionId: topMarketsSubquery.conditionId,
			totalMarketVol: topMarketsSubquery.totalMarketVol,
			totalMarketTrades: topMarketsSubquery.totalMarketTrades,
			hnScore: hnScoreSql,
		})
		.from(topMarketsSubquery)
		.orderBy(desc(hnScoreSql))
		.offset(offset)
		.limit(safeLimit);

	const totalResult = await db
		.select({
			count: sql<number>`CAST(count(distinct ${vMarketSummary.conditionId}) AS DOUBLE PRECISION)`,
		})
		.from(vMarketSummary)
		.leftJoin(
			marketsTable,
			eq(vMarketSummary.conditionId, marketsTable.conditionId),
		)
		.where(marketFilter);
	const total = Number(totalResult[0]?.count || 0);

	if (marketVolumes.length === 0) {
		return { total, markets: [] };
	}

	const conditionIds = marketVolumes
		.map((m) => m.conditionId)
		.filter((id): id is string => id !== null);

	const conditionFilter = inArray(vMarketSummary.conditionId, conditionIds);
	const allOutcomesFilter = closedFilter
		? and(conditionFilter, closedFilter)
		: conditionFilter;

	const allOutcomes = await db
		.select({
			conditionId: vMarketSummary.conditionId,
			question: vMarketSummary.question,
			outcome: vMarketSummary.outcome,
			tokenId: vMarketSummary.tokenId,
			totalTrades: sql<number>`CAST(coalesce(${vMarketSummary.totalTrades}, 0) AS DOUBLE PRECISION)`,
			totalVol: sql<number>`CAST(coalesce(${vMarketSummary.totalVol}, 0) AS DOUBLE PRECISION)`,
			lastPrice: sql<number>`CAST(coalesce(${vMarketSummary.lastPrice}, 0) AS DOUBLE PRECISION)`,
			mean: sql<
				number | null
			>`CAST(${vMarketSummary.mean} AS DOUBLE PRECISION)`,
			stdDev: sql<
				number | null
			>`CAST(${vMarketSummary.stdDev} AS DOUBLE PRECISION)`,
			p95: sql<number | null>`CAST(${vMarketSummary.p95} AS DOUBLE PRECISION)`,
			closed: vMarketSummary.closed,
		})
		.from(vMarketSummary)
		.where(allOutcomesFilter);

	const tokenIds = Array.from(
		new Set(
			allOutcomes
				.map((outcome) => outcome.tokenId)
				.filter((id): id is string => id !== null),
		),
	);

	const insiderTradeCounts = tokenIds.length
		? await db
				.select({
					tokenId: insiderPositions.tokenId,
					insiderTradeCount: sql<number>`CAST(coalesce(sum(${insiderPositions.tradeCount}), 0) AS DOUBLE PRECISION)`,
				})
				.from(insiderPositions)
				.where(inArray(insiderPositions.tokenId, tokenIds))
				.groupBy(insiderPositions.tokenId)
		: [];

	const insiderTradeCountByToken = new Map(
		insiderTradeCounts.map((entry) => [
			String(entry.tokenId),
			Number(entry.insiderTradeCount || 0),
		]),
	);

	const hnScoreByCondition = new Map(
		marketVolumes.map((m) => [m.conditionId, Number(m.hnScore || 0)]),
	);

	const markets: OptimizedMarketOutcome[] = allOutcomes.map((outcome) => {
		const marketTotal = marketVolumes.find(
			(mv) => mv.conditionId === outcome.conditionId,
		);
		const tokenIdKey = outcome.tokenId !== null ? String(outcome.tokenId) : "";
		return {
			conditionId: outcome.conditionId,
			question: outcome.question || outcome.conditionId || "",
			outcome: outcome.outcome,
			tokenId: outcome.tokenId,
			position_id: outcome.tokenId,
			total_trades: Number(outcome.totalTrades || 0),
			volume: Number(outcome.totalVol || 0),
			last_price: Number(outcome.lastPrice || 0),
			total_market_vol: Number(marketTotal?.totalMarketVol || 0),
			total_market_trades: Number(marketTotal?.totalMarketTrades || 0),
			hn_score: hnScoreByCondition.get(outcome.conditionId) ?? 0,
			insider_trade_count: insiderTradeCountByToken.get(tokenIdKey) ?? 0,
			mean: outcome.mean !== null ? Number(outcome.mean) : null,
			stdDev: outcome.stdDev !== null ? Number(outcome.stdDev) : null,
			p95: parsePositiveStatOrNull(outcome.p95),
			closed: outcome.closed,
		};
	});

	markets.sort((a, b) => {
		const hnScoreDiff = b.hn_score - a.hn_score;
		if (hnScoreDiff !== 0) return hnScoreDiff;
		const conditionDiff = String(a.conditionId ?? "").localeCompare(
			String(b.conditionId ?? ""),
		);
		if (conditionDiff !== 0) return conditionDiff;
		return String(a.outcome ?? "").localeCompare(String(b.outcome ?? ""));
	});

	return { total, markets };
}
