import { drizzle } from "drizzle-orm/bun-sql";
import * as schema from "@/lib/db/schema";
import { resolveDbConnectionConfig } from "@/lib/db/connection";

const dbConfig = resolveDbConnectionConfig();
const DATABASE_URL = dbConfig.databaseUrl;
export const DB_SCHEMA = dbConfig.schema;

// Initialize Drizzle with bun-sql driver
export const db = drizzle(DATABASE_URL, { schema, casing: "snake_case" });

// Factory function to create a fresh db connection (for respawn)
export const createDb = () => {
	return drizzle(DATABASE_URL, { schema, casing: "snake_case" });
};

export const initDb = async (retries = 5, delay = 2000) => {
	console.log("[DB] Initializing PostgreSQL connection via bun-sql...");

	for (let i = 0; i < retries; i++) {
		try {
			// Test connection by running a simple query
			await db.execute("SELECT 1");
			console.log("[DB] PostgreSQL connection established successfully");
				console.log(
					`[DB] Connected to: ${DATABASE_URL.replace(/:[^:]*@/, ":***@")}`,
				);
				console.log(`[DB] Active schema: ${DB_SCHEMA}`);
				return;
			} catch (error) {
			console.error(
				`[DB] Connection attempt ${i + 1}/${retries} failed:`,
				error instanceof Error ? error.message : String(error),
			);
			if (i < retries - 1) {
				console.log(`[DB] Retrying in ${delay}ms...`);
				await new Promise((resolve) => setTimeout(resolve, delay));
			} else {
				console.error("[DB] All connection attempts failed.");
				throw error;
			}
		}
	}
};
