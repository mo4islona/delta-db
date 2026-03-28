import { eq, isNotNull, relations, sql } from "drizzle-orm";
import {
	bigint,
	boolean,
	customType,
	foreignKey,
	index,
	integer,
	numeric,
	pgTable,
	pgView,
	real,
	text,
} from "drizzle-orm/pg-core";

const bytea = customType<{
	data: Buffer;
	driverData: Buffer | Uint8Array | string;
}>({
	dataType() {
		return "bytea";
	},
	fromDriver(value) {
		if (typeof value === "string") {
			const hex = value.startsWith("\\x") ? value.slice(2) : value;
			return Buffer.from(hex, "hex");
		}
		return Buffer.isBuffer(value) ? value : Buffer.from(value);
	},
});

// Checkpoint for stream processing
export const checkpoint = pgTable("checkpoint", {
	id: text("id").primaryKey(),
	currentNumber: bigint("current_number", { mode: "number" }).notNull(),
	currentHash: text("current_hash").notNull(),
	currentTimestamp: bigint("current_timestamp", { mode: "number" }),
	finalized: text("finalized"), // JSON string
	rollbackChain: text("rollback_chain"), // JSON string
});

// BloomFilter snapshots for reconstruction (legacy - kept for migration/rollback)
export const bloomfilterSnapshots = pgTable(
	"bloomfilter_snapshots",
	{
		id: text("id").primaryKey(), // e.g., "insider" or "notinsider"
		buckets: bytea("buckets").notNull(), // Binary data of Int32Array buckets
		bits: integer("bits").notNull(), // Number of bits (e.g., 819200)
		hashes: integer("hashes").notNull(), // Number of hash functions (e.g., 4)
		itemCount: integer("item_count").default(0), // Approximate number of items in filter
		updatedAt: bigint("updated_at", { mode: "number" }).notNull(), // Timestamp of last update
		blockNumber: bigint("block_number", { mode: "number" }), // Block number when snapshot was taken
		blockHash: text("block_hash"), // Block hash when snapshot was taken
		blockTimestamp: bigint("block_timestamp", { mode: "number" }), // Block timestamp when snapshot was taken
	},
	(table) => [
		index("idx_bloomfilter_updated").on(table.updatedAt),
		index("idx_bloomfilter_block").on(table.blockNumber),
	],
);

// XXHash32Set detector snapshots (new - incremental saves)
export const detectorSnapshots = pgTable(
	"detector_snapshots",
	{
		id: text("id").primaryKey(), // e.g., "insider" or "notinsider"
		dataSet: integer("data_set").array().notNull(), // INCREMENTAL: Only unsaved hashes since last snapshot
		unsavedCount: integer("unsaved_count").default(0), // Track size of this incremental snapshot
		itemCount: integer("item_count").default(0).notNull(), // Total count (cumulative across all snapshots)
		updatedAt: bigint("updated_at", { mode: "number" }).notNull(), // Timestamp of this snapshot
		blockNumber: bigint("block_number", { mode: "number" }), // Block number when snapshot was taken
	},
	(table) => [
		index("idx_detector_updated").on(table.updatedAt),
		index("idx_detector_block").on(table.blockNumber),
	],
);

// Account statistics
export const accountStats = pgTable(
	"account_stats",
	{
		account: text("account").primaryKey(),
		totalTrades: integer("total_trades").default(0),
		totalVol: real("total_vol").default(0),
		lastTradeTime: bigint("last_trade_time", { mode: "number" }),
	},
	(table) => [index("idx_account_stats_vol").on(table.totalVol)],
);

// Detected insider traders
export const detectedInsiders = pgTable(
	"detected_insiders",
	{
		account: text("account").primaryKey(),
		detectedAt: bigint("detected_at", { mode: "number" }),
		volume: real("volume"),
		tokenId: numeric("token_id", { precision: 78, scale: 0 }),
		alertPrice: real("alert_price"),
	},
	(table) => [index("idx_insiders_vol").on(table.volume)],
);

// Persisted insider positions (hashed account key only)
export const insiderPositions = pgTable(
	"insider_positions",
	{
		id: text("id").primaryKey(), // `${accountHash}-${tokenId}`
		accountHash: integer("account_hash").notNull(),
		tokenId: numeric("token_id", { precision: 78, scale: 0 }).notNull(),
		totalVolume: real("total_volume").notNull().default(0),
		tradeCount: integer("trade_count").notNull().default(0),
		avgPrice: real("avg_price").notNull().default(0),
		sumPrice: real("sum_price").notNull().default(0),
		sumPriceSq: real("sum_price_sq").notNull().default(0),
		firstSeen: bigint("first_seen", { mode: "number" }),
		lastSeen: bigint("last_seen", { mode: "number" }),
		detectedAt: bigint("detected_at", { mode: "number" }),
	},
	(table) => [
		index("idx_insider_positions_account").on(table.accountHash),
		index("idx_insider_positions_token").on(table.tokenId),
		index("idx_insider_positions_detected").on(table.detectedAt),
	],
);

// Hash -> wallet address mapping (persisted on eviction/classification)
export const accountWalletMap = pgTable(
	"account_wallet_map",
	{
		accountHash: integer("account_hash").primaryKey(),
		walletAddress: text("wallet_address").notNull(),
		firstSeen: bigint("first_seen", { mode: "number" }),
		lastSeen: bigint("last_seen", { mode: "number" }),
	},
	(table) => [
		index("idx_account_wallet_wallet").on(table.walletAddress),
		index("idx_account_wallet_last_seen").on(table.lastSeen),
	],
);

// Token statistics
export const tokenStats = pgTable(
	"token_stats",
	{
		token: numeric("token", { precision: 78, scale: 0 }).primaryKey(),
		totalTrades: integer("total_trades").default(0),
		totalVol: real("total_vol").default(0),
		totalInsiders: integer("total_insiders").default(0),
		totalInsidersVol: real("total_insiders_vol").default(0),
		lastPrice: real("last_price").default(0),
		sumPrice: real("sum_price").default(0),
		sumPriceSq: real("sum_price_sq").default(0),
		mean: real("mean").default(0),
		stdDev: real("std_dev").default(0),
		p95: real("p95").default(0),
	},
	(table) => [
		index("idx_token_stats_vol").on(table.totalVol),
		index("idx_token_stats_insiders").on(table.totalInsiders),
	],
);

// Token to market lookup
export const tokenMarketLookup = pgTable(
	"token_market_lookup",
	{
		tokenId: numeric("token_id", { precision: 78, scale: 0 }).primaryKey(),
		conditionId: text("condition_id"),
		createdAt: bigint("created_at", { mode: "number" }),
	},
	(table) => [
		index("idx_token_lookup_condition").on(table.conditionId),
		index("idx_token_lookup_condition_token").on(
			table.conditionId,
			table.tokenId,
		),
	],
);

// Polymarket markets info
export const markets = pgTable(
	"markets",
	{
		conditionId: text("conditionId").primaryKey(),
		question: text("question").notNull(),
		description: text("description"),
		outcomeTags: text("outcomeTags"),
		slug: text("slug"),
		active: boolean("active").default(true),
		closed: boolean("closed").default(false),
		updatedAt: bigint("updatedAt", { mode: "number" }),
	},
	(table) => [
		index("idx_markets_closed_condition").on(table.closed, table.conditionId),
	],
);

// Exploded market tokens (outcomes)
export const marketTokens = pgTable(
	"market_tokens",
	{
		tokenId: numeric("token_id", { precision: 78, scale: 0 }).primaryKey(),
		marketConditionId: text("market_condition_id").notNull(),
		outcome: text("outcome"),
		tokenIndex: integer("token_index"),
		outcomeIndex: integer("outcome_index"),
		winner: boolean("winner").default(false),
	},
	(table) => [
		index("idx_market_tokens_condition").on(table.marketConditionId),
		index("idx_market_tokens_condition_winner_token").on(
			table.marketConditionId,
			table.winner,
			table.tokenId,
		),
		foreignKey({
			columns: [table.marketConditionId],
			foreignColumns: [markets.conditionId],
		}),
	],
);

// Views - Token with Market Info
export const vBaseTokenMarketInfo = pgView("v_base_token_market_info").as(
	(qb) =>
		qb
			.select({
				tokenId: tokenMarketLookup.tokenId,
				conditionId: tokenMarketLookup.conditionId,
				createdAt: tokenMarketLookup.createdAt,
				question: markets.question,
				description: markets.description,
				slug: markets.slug,
				outcome: marketTokens.outcome,
				tokenIndex: marketTokens.tokenIndex,
				outcomeIndex: marketTokens.outcomeIndex,
				winner: marketTokens.winner,
				closed: markets.closed,
			})
			.from(tokenMarketLookup)
			.leftJoin(markets, eq(tokenMarketLookup.conditionId, markets.conditionId))
			.leftJoin(
				marketTokens,
				eq(tokenMarketLookup.tokenId, marketTokens.tokenId),
			),
);

// Views - Token Stats Enriched
export const vTokenStatsEnriched = pgView("v_token_stats_enriched").as((qb) =>
	qb
		.select({
			token: tokenStats.token,
			totalTrades: tokenStats.totalTrades,
			totalVol: tokenStats.totalVol,
			totalInsiders: tokenStats.totalInsiders,
			totalInsidersVol: tokenStats.totalInsidersVol,
			lastPrice: tokenStats.lastPrice,
			mean: tokenStats.mean,
			stdDev: tokenStats.stdDev,
			p95: tokenStats.p95,
			conditionId: vBaseTokenMarketInfo.conditionId,
			question: vBaseTokenMarketInfo.question,
			outcome: vBaseTokenMarketInfo.outcome,
			slug: vBaseTokenMarketInfo.slug,
			winner: vBaseTokenMarketInfo.winner,
			closed: vBaseTokenMarketInfo.closed,
		})
		.from(tokenStats)
		.leftJoin(
			vBaseTokenMarketInfo,
			eq(tokenStats.token, vBaseTokenMarketInfo.tokenId),
		),
);

// Views - Insiders Enriched
export const vInsidersEnriched = pgView("v_insiders_enriched").as((qb) =>
	qb
		.select({
			account: insiderPositions.accountHash,
			detectedAt: insiderPositions.detectedAt,
			volume: insiderPositions.totalVolume,
			tokenId: insiderPositions.tokenId,
			alertPrice: insiderPositions.avgPrice,
			outcome: marketTokens.outcome,
			marketCount: sql<number>`1`.as("market_count"),
			conditionId: tokenMarketLookup.conditionId,
			question: markets.question,
			slug: markets.slug,
			outcomeTags: markets.outcomeTags, // Added this
			lastPrice: tokenStats.lastPrice,
			marketTotalVolume: tokenStats.totalVol,
			winner: marketTokens.winner,
			closed: markets.closed,
		})
		.from(insiderPositions)
		.leftJoin(
			tokenMarketLookup,
			eq(insiderPositions.tokenId, tokenMarketLookup.tokenId),
		)
		.leftJoin(markets, eq(tokenMarketLookup.conditionId, markets.conditionId))
		.leftJoin(marketTokens, eq(insiderPositions.tokenId, marketTokens.tokenId))
		.leftJoin(tokenStats, eq(insiderPositions.tokenId, tokenStats.token)),
);

// Views - Market Summary
export const vMarketSummary = pgView("v_market_summary").as((qb) =>
	qb
		.select({
			conditionId: vBaseTokenMarketInfo.conditionId,
			createdAt: vBaseTokenMarketInfo.createdAt,
			question: vBaseTokenMarketInfo.question,
			slug: vBaseTokenMarketInfo.slug,
			outcome: vBaseTokenMarketInfo.outcome,
			tokenId: vBaseTokenMarketInfo.tokenId,
			totalTrades: tokenStats.totalTrades,
			totalVol: tokenStats.totalVol,
			lastPrice: tokenStats.lastPrice,
			totalInsiders: tokenStats.totalInsiders,
			mean: tokenStats.mean,
			stdDev: tokenStats.stdDev,
			p95: tokenStats.p95,
			closed: vBaseTokenMarketInfo.closed,
		})
		.from(vBaseTokenMarketInfo)
		.leftJoin(tokenStats, eq(vBaseTokenMarketInfo.tokenId, tokenStats.token))
		.where(isNotNull(vBaseTokenMarketInfo.conditionId)),
);

// Relations
export const tokenStatsRelations = relations(tokenStats, ({ one }) => ({
	tokenMarketLookup: one(tokenMarketLookup, {
		fields: [tokenStats.token],
		references: [tokenMarketLookup.tokenId],
	}),
}));

export const tokenMarketLookupRelations = relations(
	tokenMarketLookup,
	({ one }) => ({
		market: one(markets, {
			fields: [tokenMarketLookup.conditionId],
			references: [markets.conditionId],
		}),
		tokenStats: one(tokenStats, {
			fields: [tokenMarketLookup.tokenId],
			references: [tokenStats.token],
		}),
	}),
);

export const marketsRelations = relations(markets, ({ many }) => ({
	tokenLookups: many(tokenMarketLookup),
	marketTokens: many(marketTokens),
}));

export const marketTokensRelations = relations(marketTokens, ({ one }) => ({
	market: one(markets, {
		fields: [marketTokens.marketConditionId],
		references: [markets.conditionId],
	}),
}));

export const detectedInsidersRelations = relations(
	detectedInsiders,
	({ one }) => ({
		tokenLookup: one(tokenMarketLookup, {
			fields: [detectedInsiders.tokenId],
			references: [tokenMarketLookup.tokenId],
		}),
	}),
);

export const insiderPositionsRelations = relations(
	insiderPositions,
	({ one }) => ({
		tokenLookup: one(tokenMarketLookup, {
			fields: [insiderPositions.tokenId],
			references: [tokenMarketLookup.tokenId],
		}),
	}),
);
