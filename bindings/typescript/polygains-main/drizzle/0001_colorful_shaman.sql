DROP VIEW IF EXISTS "public"."v_market_summary";--> statement-breakpoint
DROP VIEW IF EXISTS "public"."v_token_stats_enriched";--> statement-breakpoint
DROP VIEW IF EXISTS "public"."v_insiders_enriched";--> statement-breakpoint
DROP VIEW IF EXISTS "public"."v_base_token_market_info";--> statement-breakpoint

CREATE TEMP TABLE "__tmp_account_wallet_map" AS
SELECT
	CASE
		WHEN "account_hash"::bigint > 2147483647
			THEN ("account_hash"::bigint - 4294967296)::integer
		ELSE "account_hash"::integer
	END AS "account_hash",
	(
		array_agg(
			"wallet_address"
			ORDER BY "last_seen" DESC NULLS LAST, "first_seen" ASC NULLS LAST, "wallet_address" ASC
		)
	)[1] AS "wallet_address",
	MIN("first_seen") AS "first_seen",
	MAX("last_seen") AS "last_seen"
FROM "account_wallet_map"
GROUP BY 1;--> statement-breakpoint

CREATE TEMP TABLE "__tmp_insider_positions" AS
SELECT
	CASE
		WHEN "account_hash"::bigint > 2147483647
			THEN ("account_hash"::bigint - 4294967296)::integer
		ELSE "account_hash"::integer
	END AS "account_hash",
	"token_id",
	SUM("total_volume") AS "total_volume",
	SUM("trade_count") AS "trade_count",
	SUM("sum_price") AS "sum_price",
	SUM("sum_price_sq") AS "sum_price_sq",
	MIN("first_seen") AS "first_seen",
	MAX("last_seen") AS "last_seen",
	MIN("detected_at") AS "detected_at"
FROM "insider_positions"
GROUP BY 1, 2;--> statement-breakpoint

TRUNCATE TABLE "account_wallet_map";--> statement-breakpoint
TRUNCATE TABLE "insider_positions";--> statement-breakpoint

ALTER TABLE "account_wallet_map"
ALTER COLUMN "account_hash"
SET DATA TYPE integer
USING "account_hash"::integer;--> statement-breakpoint

ALTER TABLE "insider_positions"
ALTER COLUMN "account_hash"
SET DATA TYPE integer
USING "account_hash"::integer;--> statement-breakpoint

INSERT INTO "account_wallet_map" ("account_hash", "wallet_address", "first_seen", "last_seen")
SELECT
	"account_hash",
	"wallet_address",
	"first_seen",
	"last_seen"
FROM "__tmp_account_wallet_map";--> statement-breakpoint

INSERT INTO "insider_positions" (
	"id",
	"account_hash",
	"token_id",
	"total_volume",
	"trade_count",
	"avg_price",
	"sum_price",
	"sum_price_sq",
	"first_seen",
	"last_seen",
	"detected_at"
)
SELECT
	("account_hash"::text || '-' || "token_id"::text) AS "id",
	"account_hash",
	"token_id",
	"total_volume",
	"trade_count",
	CASE
		WHEN "trade_count" > 0 THEN "sum_price" / "trade_count"
		ELSE 0
	END AS "avg_price",
	"sum_price",
	"sum_price_sq",
	"first_seen",
	"last_seen",
	"detected_at"
FROM "__tmp_insider_positions";

--> statement-breakpoint
CREATE VIEW "public"."v_base_token_market_info" AS (
	select
		"token_market_lookup"."token_id",
		"token_market_lookup"."condition_id",
		"token_market_lookup"."created_at",
		"markets"."question",
		"markets"."description",
		"markets"."slug",
		"market_tokens"."outcome",
		"market_tokens"."token_index",
		"market_tokens"."outcome_index",
		"market_tokens"."winner",
		"markets"."closed"
	from "token_market_lookup"
	left join "markets"
		on "token_market_lookup"."condition_id" = "markets"."conditionId"
	left join "market_tokens"
		on "token_market_lookup"."token_id" = "market_tokens"."token_id"
);--> statement-breakpoint

CREATE VIEW "public"."v_insiders_enriched" AS (
	select
		"insider_positions"."account_hash",
		"insider_positions"."detected_at",
		"insider_positions"."total_volume",
		"insider_positions"."token_id",
		"insider_positions"."avg_price",
		"market_tokens"."outcome",
		1 as "market_count",
		"token_market_lookup"."condition_id",
		"markets"."question",
		"markets"."slug",
		"token_stats"."last_price",
		"token_stats"."total_vol",
		"market_tokens"."winner",
		"markets"."closed"
	from "insider_positions"
	left join "token_market_lookup"
		on "insider_positions"."token_id" = "token_market_lookup"."token_id"
	left join "markets"
		on "token_market_lookup"."condition_id" = "markets"."conditionId"
	left join "market_tokens"
		on "insider_positions"."token_id" = "market_tokens"."token_id"
	left join "token_stats"
		on "insider_positions"."token_id" = "token_stats"."token"
);--> statement-breakpoint

CREATE VIEW "public"."v_market_summary" AS (
	select
		"v_base_token_market_info"."condition_id",
		"v_base_token_market_info"."created_at",
		"v_base_token_market_info"."question",
		"v_base_token_market_info"."slug",
		"v_base_token_market_info"."outcome",
		"v_base_token_market_info"."token_id",
		"token_stats"."total_trades",
		"token_stats"."total_vol",
		"token_stats"."last_price",
		"token_stats"."total_insiders",
		"token_stats"."mean",
		"token_stats"."std_dev",
		"token_stats"."p95",
		"v_base_token_market_info"."closed"
	from "v_base_token_market_info"
	left join "token_stats"
		on "v_base_token_market_info"."token_id" = "token_stats"."token"
	where "v_base_token_market_info"."condition_id" is not null
);--> statement-breakpoint

CREATE VIEW "public"."v_token_stats_enriched" AS (
	select
		"token_stats"."token",
		"token_stats"."total_trades",
		"token_stats"."total_vol",
		"token_stats"."total_insiders",
		"token_stats"."total_insiders_vol",
		"token_stats"."last_price",
		"token_stats"."mean",
		"token_stats"."std_dev",
		"token_stats"."p95",
		"v_base_token_market_info"."condition_id",
		"v_base_token_market_info"."question",
		"v_base_token_market_info"."outcome",
		"v_base_token_market_info"."slug",
		"v_base_token_market_info"."winner",
		"v_base_token_market_info"."closed"
	from "token_stats"
	left join "v_base_token_market_info"
		on "token_stats"."token" = "v_base_token_market_info"."token_id"
);
