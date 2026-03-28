import { beforeAll, beforeEach } from "bun:test";
import { drizzle } from "drizzle-orm/bun-sql";
import {
	quotePgIdentifier,
	resolveDbConnectionConfig,
} from "@/lib/db/connection";

const RESET_TABLES = [
	"markets",
	"market_tokens",
	"token_market_lookup",
	"token_stats",
	"insider_positions",
	"account_wallet_map",
	"detected_insiders",
	"account_stats",
	"checkpoint",
];

let setupPromise: Promise<void> | null = null;
let bootstrapDb: ReturnType<typeof drizzle> | null = null;
let testDb: ReturnType<typeof drizzle> | null = null;

async function ensureTestingSchema(): Promise<void> {
	const config = resolveDbConnectionConfig();
	if (config.schema !== "testing") {
		throw new Error(
			`Integration tests must run on DB_SCHEMA=testing (resolved ${config.schema})`,
		);
	}

	if (!setupPromise) {
		setupPromise = (async () => {
			if (!bootstrapDb) {
				bootstrapDb = drizzle(config.baseDatabaseUrl);
			}
			await bootstrapDb.execute(
				`CREATE SCHEMA IF NOT EXISTS ${quotePgIdentifier(config.schema)}`,
			);

			const tableNames = RESET_TABLES.map((table) => `'${table}'`).join(", ");

			await bootstrapDb.execute(`
				DO $$
				DECLARE
					t_name text;
				BEGIN
					FOREACH t_name IN ARRAY ARRAY[${tableNames}]
					LOOP
						IF to_regclass(format('public.%I', t_name)) IS NOT NULL THEN
							EXECUTE format(
								'CREATE TABLE IF NOT EXISTS ${quotePgIdentifier(config.schema)}.%I (LIKE public.%I INCLUDING ALL)',
								t_name,
								t_name
							);
						END IF;
					END LOOP;
				END
				$$;
			`);

			await bootstrapDb.execute(`
				CREATE OR REPLACE VIEW ${quotePgIdentifier(config.schema)}."v_base_token_market_info" AS
				SELECT
					tml.token_id,
					tml.condition_id,
					tml.created_at,
					m.question,
					m.description,
					m.slug,
					mt.outcome,
					mt.token_index,
					mt.outcome_index,
					mt.winner,
					m.closed
				FROM ${quotePgIdentifier(config.schema)}.token_market_lookup tml
				LEFT JOIN ${quotePgIdentifier(config.schema)}.markets m
					ON tml.condition_id = m."conditionId"
				LEFT JOIN ${quotePgIdentifier(config.schema)}.market_tokens mt
					ON tml.token_id = mt.token_id;
			`);

			await bootstrapDb.execute(`
				CREATE OR REPLACE VIEW ${quotePgIdentifier(config.schema)}."v_insiders_enriched" AS
				SELECT
					ip.account_hash,
					ip.detected_at,
					ip.total_volume,
					ip.token_id,
					ip.avg_price,
					mt.outcome,
					1 AS market_count,
					tml.condition_id,
					m.question,
					m.slug,
					m."outcomeTags",
					ts.last_price,
					ts.total_vol,
					mt.winner,
					m.closed
				FROM ${quotePgIdentifier(config.schema)}.insider_positions ip
				LEFT JOIN ${quotePgIdentifier(config.schema)}.token_market_lookup tml
					ON ip.token_id = tml.token_id
				LEFT JOIN ${quotePgIdentifier(config.schema)}.markets m
					ON tml.condition_id = m."conditionId"
				LEFT JOIN ${quotePgIdentifier(config.schema)}.market_tokens mt
					ON ip.token_id = mt.token_id
				LEFT JOIN ${quotePgIdentifier(config.schema)}.token_stats ts
					ON ip.token_id = ts.token;
			`);

			await bootstrapDb.execute(`
				CREATE OR REPLACE VIEW ${quotePgIdentifier(config.schema)}."v_market_summary" AS
				SELECT
					base.condition_id,
					base.created_at,
					base.question,
					base.slug,
					base.outcome,
					base.token_id,
					ts.total_trades,
					ts.total_vol,
					ts.last_price,
					ts.total_insiders,
					ts.mean,
					ts.std_dev,
					ts.p95,
					base.closed
				FROM ${quotePgIdentifier(config.schema)}."v_base_token_market_info" base
				LEFT JOIN ${quotePgIdentifier(config.schema)}.token_stats ts
					ON base.token_id = ts.token
				WHERE base.condition_id IS NOT NULL;
			`);
		})();
	}

	await setupPromise;
}

async function resetTestingSchemaTables(): Promise<void> {
	const config = resolveDbConnectionConfig();
	if (!testDb) {
		testDb = drizzle(config.databaseUrl);
	}
	const truncateSql = RESET_TABLES.map((table) => {
		const qualified = `${quotePgIdentifier(config.schema)}.${quotePgIdentifier(table)}`;
		const regClass = `${config.schema}.${table}`;
		return `
			IF to_regclass('${regClass}') IS NOT NULL THEN
				EXECUTE 'TRUNCATE TABLE ${qualified} RESTART IDENTITY CASCADE';
			END IF;
		`;
	}).join("\n");

	await testDb.execute(`
		DO $$
		BEGIN
			${truncateSql}
		END
		$$;
	`);
}

export function setupTestingSchema(): void {
	beforeAll(async () => {
		await ensureTestingSchema();
	});

	beforeEach(async () => {
		await resetTestingSchemaTables();
	});
}
