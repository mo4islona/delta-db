CREATE TABLE "account_stats" (
	"account" text PRIMARY KEY NOT NULL,
	"total_trades" integer DEFAULT 0,
	"total_vol" real DEFAULT 0,
	"last_trade_time" bigint
);
--> statement-breakpoint
CREATE TABLE "account_wallet_map" (
	"account_hash" text PRIMARY KEY NOT NULL,
	"wallet_address" text NOT NULL,
	"first_seen" bigint,
	"last_seen" bigint
);
--> statement-breakpoint
CREATE TABLE "bloomfilter_snapshots" (
	"id" text PRIMARY KEY NOT NULL,
	"buckets" "bytea" NOT NULL,
	"bits" integer NOT NULL,
	"hashes" integer NOT NULL,
	"item_count" integer DEFAULT 0,
	"updated_at" bigint NOT NULL,
	"block_number" bigint,
	"block_hash" text,
	"block_timestamp" bigint
);
--> statement-breakpoint
CREATE TABLE "checkpoint" (
	"id" text PRIMARY KEY NOT NULL,
	"current_number" bigint NOT NULL,
	"current_hash" text NOT NULL,
	"current_timestamp" bigint,
	"finalized" text,
	"rollback_chain" text
);
--> statement-breakpoint
CREATE TABLE "detected_insiders" (
	"account" text PRIMARY KEY NOT NULL,
	"detected_at" bigint,
	"volume" real,
	"token_id" numeric(78, 0),
	"alert_price" real
);
--> statement-breakpoint
CREATE TABLE "detector_snapshots" (
	"id" text PRIMARY KEY NOT NULL,
	"data_set" integer[] NOT NULL,
	"unsaved_count" integer DEFAULT 0,
	"item_count" integer DEFAULT 0 NOT NULL,
	"updated_at" bigint NOT NULL,
	"block_number" bigint
);
--> statement-breakpoint
CREATE TABLE "insider_positions" (
	"id" text PRIMARY KEY NOT NULL,
	"account_hash" text NOT NULL,
	"token_id" numeric(78, 0) NOT NULL,
	"total_volume" real DEFAULT 0 NOT NULL,
	"trade_count" integer DEFAULT 0 NOT NULL,
	"avg_price" real DEFAULT 0 NOT NULL,
	"sum_price" real DEFAULT 0 NOT NULL,
	"sum_price_sq" real DEFAULT 0 NOT NULL,
	"first_seen" bigint,
	"last_seen" bigint,
	"detected_at" bigint
);
--> statement-breakpoint
CREATE TABLE "market_tokens" (
	"token_id" numeric(78, 0) PRIMARY KEY NOT NULL,
	"market_condition_id" text NOT NULL,
	"outcome" text,
	"token_index" integer,
	"outcome_index" integer,
	"winner" boolean DEFAULT false
);
--> statement-breakpoint
CREATE TABLE "markets" (
	"conditionId" text PRIMARY KEY NOT NULL,
	"question" text NOT NULL,
	"description" text,
	"outcomeTags" text,
	"slug" text,
	"active" boolean DEFAULT true,
	"closed" boolean DEFAULT false,
	"updatedAt" bigint
);
--> statement-breakpoint
CREATE TABLE "token_market_lookup" (
	"token_id" numeric(78, 0) PRIMARY KEY NOT NULL,
	"condition_id" text,
	"created_at" bigint
);
--> statement-breakpoint
CREATE TABLE "token_stats" (
	"token" numeric(78, 0) PRIMARY KEY NOT NULL,
	"total_trades" integer DEFAULT 0,
	"total_vol" real DEFAULT 0,
	"total_insiders" integer DEFAULT 0,
	"total_insiders_vol" real DEFAULT 0,
	"last_price" real DEFAULT 0,
	"sum_price" real DEFAULT 0,
	"sum_price_sq" real DEFAULT 0,
	"mean" real DEFAULT 0,
	"std_dev" real DEFAULT 0,
	"p95" real DEFAULT 0
);
--> statement-breakpoint
ALTER TABLE "market_tokens" ADD CONSTRAINT "market_tokens_market_condition_id_markets_conditionId_fk" FOREIGN KEY ("market_condition_id") REFERENCES "public"."markets"("conditionId") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "idx_account_stats_vol" ON "account_stats" USING btree ("total_vol");--> statement-breakpoint
CREATE INDEX "idx_account_wallet_wallet" ON "account_wallet_map" USING btree ("wallet_address");--> statement-breakpoint
CREATE INDEX "idx_account_wallet_last_seen" ON "account_wallet_map" USING btree ("last_seen");--> statement-breakpoint
CREATE INDEX "idx_bloomfilter_updated" ON "bloomfilter_snapshots" USING btree ("updated_at");--> statement-breakpoint
CREATE INDEX "idx_bloomfilter_block" ON "bloomfilter_snapshots" USING btree ("block_number");--> statement-breakpoint
CREATE INDEX "idx_insiders_vol" ON "detected_insiders" USING btree ("volume");--> statement-breakpoint
CREATE INDEX "idx_detector_updated" ON "detector_snapshots" USING btree ("updated_at");--> statement-breakpoint
CREATE INDEX "idx_detector_block" ON "detector_snapshots" USING btree ("block_number");--> statement-breakpoint
CREATE INDEX "idx_insider_positions_account" ON "insider_positions" USING btree ("account_hash");--> statement-breakpoint
CREATE INDEX "idx_insider_positions_token" ON "insider_positions" USING btree ("token_id");--> statement-breakpoint
CREATE INDEX "idx_insider_positions_detected" ON "insider_positions" USING btree ("detected_at");--> statement-breakpoint
CREATE INDEX "idx_market_tokens_condition" ON "market_tokens" USING btree ("market_condition_id");--> statement-breakpoint
CREATE INDEX "idx_market_tokens_condition_winner_token" ON "market_tokens" USING btree ("market_condition_id","winner","token_id");--> statement-breakpoint
CREATE INDEX "idx_markets_closed_condition" ON "markets" USING btree ("closed","conditionId");--> statement-breakpoint
CREATE INDEX "idx_token_lookup_condition" ON "token_market_lookup" USING btree ("condition_id");--> statement-breakpoint
CREATE INDEX "idx_token_lookup_condition_token" ON "token_market_lookup" USING btree ("condition_id","token_id");--> statement-breakpoint
CREATE INDEX "idx_token_stats_vol" ON "token_stats" USING btree ("total_vol");--> statement-breakpoint
CREATE INDEX "idx_token_stats_insiders" ON "token_stats" USING btree ("total_insiders");--> statement-breakpoint
CREATE VIEW "public"."v_base_token_market_info" AS (select "token_market_lookup"."token_id", "token_market_lookup"."condition_id", "token_market_lookup"."created_at", "markets"."question", "markets"."description", "markets"."slug", "market_tokens"."outcome", "market_tokens"."token_index", "market_tokens"."outcome_index", "market_tokens"."winner", "markets"."closed" from "token_market_lookup" left join "markets" on "token_market_lookup"."condition_id" = "markets"."conditionId" left join "market_tokens" on "token_market_lookup"."token_id" = "market_tokens"."token_id");--> statement-breakpoint
CREATE VIEW "public"."v_insiders_enriched" AS (select "insider_positions"."account_hash", "insider_positions"."detected_at", "insider_positions"."total_volume", "insider_positions"."token_id", "insider_positions"."avg_price", "market_tokens"."outcome", 1 as "market_count", "token_market_lookup"."condition_id", "markets"."question", "markets"."slug", "token_stats"."last_price", "token_stats"."total_vol", "market_tokens"."winner", "markets"."closed" from "insider_positions" left join "token_market_lookup" on "insider_positions"."token_id" = "token_market_lookup"."token_id" left join "markets" on "token_market_lookup"."condition_id" = "markets"."conditionId" left join "market_tokens" on "insider_positions"."token_id" = "market_tokens"."token_id" left join "token_stats" on "insider_positions"."token_id" = "token_stats"."token");--> statement-breakpoint
CREATE VIEW "public"."v_market_summary" AS (select "v_base_token_market_info"."condition_id", "v_base_token_market_info"."created_at", "v_base_token_market_info"."question", "v_base_token_market_info"."slug", "v_base_token_market_info"."outcome", "v_base_token_market_info"."token_id", "token_stats"."total_trades", "token_stats"."total_vol", "token_stats"."last_price", "token_stats"."total_insiders", "token_stats"."mean", "token_stats"."std_dev", "token_stats"."p95", "v_base_token_market_info"."closed" from "v_base_token_market_info" left join "token_stats" on "v_base_token_market_info"."token_id" = "token_stats"."token" where "v_base_token_market_info"."condition_id" is not null);--> statement-breakpoint
CREATE VIEW "public"."v_token_stats_enriched" AS (select "token_stats"."token", "token_stats"."total_trades", "token_stats"."total_vol", "token_stats"."total_insiders", "token_stats"."total_insiders_vol", "token_stats"."last_price", "token_stats"."mean", "token_stats"."std_dev", "token_stats"."p95", "v_base_token_market_info"."condition_id", "v_base_token_market_info"."question", "v_base_token_market_info"."outcome", "v_base_token_market_info"."slug", "v_base_token_market_info"."winner", "v_base_token_market_info"."closed" from "token_stats" left join "v_base_token_market_info" on "token_stats"."token" = "v_base_token_market_info"."token_id");