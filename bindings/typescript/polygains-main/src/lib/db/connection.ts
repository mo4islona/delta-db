const DEFAULT_DATABASE_URL =
	"postgresql://postgres:postgres@127.0.0.1:5469/postgres";

export interface DbConnectionConfig {
	baseDatabaseUrl: string;
	databaseUrl: string;
	schema: string;
	isTestRuntime: boolean;
}

function getRuntimeArgv(): string[] {
	if (typeof Bun !== "undefined" && Array.isArray(Bun.argv)) {
		return Bun.argv;
	}
	return process.argv;
}

function detectTestRuntime(): boolean {
	if (process.env.BUN_TEST === "1" || process.env.NODE_ENV === "test") {
		return true;
	}
	return getRuntimeArgv().includes("test");
}

function appendSearchPathOption(databaseUrl: string, schema: string): string {
	const trimmedSchema = schema.trim();
	if (!trimmedSchema || trimmedSchema === "public") {
		return databaseUrl;
	}

	const parsed = new URL(databaseUrl);
	const currentOptions = parsed.searchParams.get("options") ?? "";

	if (currentOptions.includes("search_path=")) {
		return parsed.toString();
	}

	const searchPathOption = `-c search_path=${trimmedSchema}`;
	parsed.searchParams.set(
		"options",
		currentOptions ? `${currentOptions} ${searchPathOption}` : searchPathOption,
	);
	return parsed.toString();
}

export function quotePgIdentifier(value: string): string {
	return `"${value.replace(/"/g, "\"\"")}"`;
}

export function resolveDbConnectionConfig(): DbConnectionConfig {
	const baseDatabaseUrl = process.env.DATABASE_URL || DEFAULT_DATABASE_URL;
	const isTestRuntime = detectTestRuntime();
	const schema = (process.env.DB_SCHEMA || (isTestRuntime ? "testing" : "public"))
		.trim()
		.toLowerCase();
	const databaseUrl = appendSearchPathOption(baseDatabaseUrl, schema);

	return {
		baseDatabaseUrl,
		databaseUrl,
		schema,
		isTestRuntime,
	};
}

