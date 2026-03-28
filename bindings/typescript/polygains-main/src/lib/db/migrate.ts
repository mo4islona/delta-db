import { drizzle } from "drizzle-orm/bun-sql";
import { migrate } from "drizzle-orm/bun-sql/migrator";
import {
	quotePgIdentifier,
	resolveDbConnectionConfig,
} from "@/lib/db/connection";

const dbConfig = resolveDbConnectionConfig();
const DATABASE_URL = dbConfig.databaseUrl;

export async function runMigrations() {
	console.log("[Migration] Starting database migration via bun-sql...");
	console.log(`[Migration] Target schema: ${dbConfig.schema}`);

	// Ensure non-public schema exists before running unqualified migrations.
	if (dbConfig.schema !== "public") {
		const bootstrapDb = drizzle(dbConfig.baseDatabaseUrl);
		await bootstrapDb.execute(
			`CREATE SCHEMA IF NOT EXISTS ${quotePgIdentifier(dbConfig.schema)}`,
		);
	}

	const db = drizzle(DATABASE_URL);

	console.log("[Migration] Running migrations from ./drizzle...");
	await migrate(db, { migrationsFolder: "./drizzle" });

	console.log("[Migration] Migration completed successfully!");
}

if (import.meta.main) {
	runMigrations().catch((error) => {
		console.error("[Migration] Migration failed:", error);
		process.exit(1);
	});
}
