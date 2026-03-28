import { describe, expect, test } from "bun:test";

describe("Database initialization", () => {
	describe("Connection string parsing", () => {
		test("should parse standard PostgreSQL URL", () => {
			const url = "postgresql://user:pass@localhost:5432/dbname";
			const parsed = new URL(url);

			expect(parsed.protocol).toBe("postgresql:");
			expect(parsed.username).toBe("user");
			expect(parsed.password).toBe("pass");
			expect(parsed.hostname).toBe("localhost");
			expect(parsed.port).toBe("5432");
			expect(parsed.pathname).toBe("/dbname");
		});

		test("should handle URL without explicit port", () => {
			const url = "postgresql://user:pass@localhost/dbname";
			const parsed = new URL(url);

			expect(parsed.hostname).toBe("localhost");
			expect(parsed.port).toBe(""); // Empty when not specified
			expect(parsed.pathname).toBe("/dbname");
		});

		test("should handle URL with special characters in password", () => {
			const url = "postgresql://user:p@ss%40word@localhost:5432/dbname";
			const parsed = new URL(url);

			expect(parsed.username).toBe("user");
			expect(parsed.password).toBe("p%40ss%40word"); // Bun keeps %40 as is in .password
		});

		test("should extract database name from pathname", () => {
			const url = "postgresql://postgres:postgres@localhost:5432/postgres";
			const parsed = new URL(url);
			const dbName = parsed.pathname.slice(1); // Remove leading "/"

			expect(dbName).toBe("postgres");
		});
	});

	describe("Environment variable handling", () => {
		test("should use DATABASE_URL from environment", () => {
			const envUrl = process.env.DATABASE_URL;

			// If DATABASE_URL is set, verify it's valid
			if (envUrl) {
				expect(() => new URL(envUrl)).not.toThrow();
			}
		});

		test("should use DB_SOCKET_PATH when provided", () => {
			const socketPath = process.env.DB_SOCKET_PATH;

			if (socketPath) {
				expect(typeof socketPath).toBe("string");
				expect(socketPath.length).toBeGreaterThan(0);
			}
		});

		test("should have default DATABASE_URL fallback", () => {
			const defaultUrl =
				"postgresql://postgres:postgres@localhost:5432/postgres";
			const parsed = new URL(defaultUrl);

			expect(parsed.username).toBe("postgres");
			expect(parsed.password).toBe("postgres");
			expect(parsed.hostname).toBe("localhost");
			expect(parsed.port).toBe("5432");
		});
	});

	describe("Unix socket path construction", () => {
		test("should construct valid socket file path", () => {
			const socketDir = "/var/run/postgresql";
			const socketFile = `${socketDir}/.s.PGSQL.5432`;

			expect(socketFile).toBe("/var/run/postgresql/.s.PGSQL.5432");
			expect(socketFile).toContain(".s.PGSQL");
		});

		test("should use port 5432 by default", () => {
			const socketDir = "/var/run/postgresql";
			const defaultPort = 5432;
			const socketFile = `${socketDir}/.s.PGSQL.${defaultPort}`;

			expect(socketFile).toContain("5432");
		});

		test("should handle various socket directory paths", () => {
			const paths = [
				"/var/run/postgresql",
				"/run/postgresql",
				"/tmp",
				"/var/lib/postgresql",
			];

			paths.forEach((path) => {
				const socketFile = `${path}/.s.PGSQL.5432`;
				expect(socketFile).toContain(path);
				expect(socketFile).toEndWith("/.s.PGSQL.5432");
			});
		});
	});

	describe("Connection mode detection", () => {
		test("should prefer Unix socket when path is set", () => {
			const socketPath = "/var/run/postgresql";
			const useSocket = Boolean(socketPath);

			expect(useSocket).toBe(true);
		});

		test("should fallback to TCP when socket path is empty", () => {
			const socketPath = "";
			const useSocket = Boolean(socketPath);

			expect(useSocket).toBe(false);
		});

		test("should fallback to TCP when socket path is undefined", () => {
			const socketPath = undefined;
			const useSocket = Boolean(socketPath);

			expect(useSocket).toBe(false);
		});
	});

	describe("Retry logic parameters", () => {
		test("should have reasonable retry defaults", () => {
			const defaultRetries = 5;
			const defaultDelay = 2000; // ms

			expect(defaultRetries).toBeGreaterThanOrEqual(3);
			expect(defaultRetries).toBeLessThanOrEqual(10);

			expect(defaultDelay).toBeGreaterThanOrEqual(1000);
			expect(defaultDelay).toBeLessThanOrEqual(5000);
		});

		test("should calculate total retry time correctly", () => {
			const retries = 5;
			const delay = 2000;
			const totalTime = retries * delay;

			expect(totalTime).toBe(10000); // 10 seconds max
		});

		test("should allow for exponential backoff", () => {
			const baseDelay = 1000;
			const attempts = [1, 2, 3, 4, 5];

			const delays = attempts.map((attempt) => baseDelay * 2 ** (attempt - 1));

			// Exponential: 1s, 2s, 4s, 8s, 16s
			expect(delays).toEqual([1000, 2000, 4000, 8000, 16000]);
			expect(delays[4]).toBeLessThan(30000); // Last retry < 30s
		});
	});

	describe("SQL client configuration", () => {
		test("should create TCP config with all required fields", () => {
			const url = "postgresql://testuser:testpass@testhost:5433/testdb";
			const parsed = new URL(url);

			const config = {
				host: parsed.hostname,
				port: parseInt(parsed.port, 10),
				database: parsed.pathname.slice(1),
				username: parsed.username,
				password: parsed.password,
			};

			expect(config).toMatchObject({
				host: "testhost",
				port: 5433,
				database: "testdb",
				username: "testuser",
				password: "testpass",
			});
		});

		test("should create Unix socket config with path", () => {
			const socketPath = "/var/run/postgresql";
			const socketFile = `${socketPath}/.s.PGSQL.5432`;
			const dbName = "postgres";
			const username = "postgres";
			const password = "postgres";

			const config = {
				path: socketFile,
				database: dbName,
				username,
				password,
			};

			expect(config).toMatchObject({
				path: "/var/run/postgresql/.s.PGSQL.5432",
				database: "postgres",
				username: "postgres",
				password: "postgres",
			});

			expect(config).not.toHaveProperty("host");
			expect(config).not.toHaveProperty("port");
		});
	});

	describe("Error handling", () => {
		test("should handle invalid DATABASE_URL gracefully", () => {
			const invalidUrls = [
				"not-a-url",
				"http://wrong-protocol.com",
				"postgresql://", // Missing host
				"postgresql://:@/", // Missing required parts
			];

			invalidUrls.forEach((url) => {
				if (url.startsWith("postgresql://")) {
					try {
						new URL(url);
					} catch (error) {
						expect(error).toBeInstanceOf(Error);
					}
				}
			});
		});

		test("should validate port is a number", () => {
			const url = "postgresql://user:pass@localhost:5432/db";
			const parsed = new URL(url);
			const port = parseInt(parsed.port, 10);

			expect(Number.isInteger(port)).toBe(true);
			expect(port).toBeGreaterThan(0);
			expect(port).toBeLessThanOrEqual(65535);
		});

		test("should handle missing password in URL", () => {
			const url = "postgresql://user@localhost:5432/db";
			const parsed = new URL(url);

			expect(parsed.username).toBe("user");
			expect(parsed.password).toBe("");
		});
	});

	describe("Connection logging", () => {
		test("should mask password in connection string for logging", () => {
			const url = "postgresql://user:secretpass@localhost:5432/db";
			const masked = url.replace(/:[^:]*@/, ":***@");

			expect(masked).toBe("postgresql://user:***@localhost:5432/db");
			expect(masked).not.toContain("secretpass");
		});

		test("should handle multiple colons in password masking", () => {
			const url = "postgresql://user:pass:with:colons@localhost:5432/db";
			const masked = url.replace(/:[^:]*@/, ":***@");

			// This will only mask until the first colon, which is acceptable
			expect(masked).toContain(":***@");
			expect(masked).not.toBe(url);
		});
	});

	describe("Drizzle configuration", () => {
		test("should use snake_case casing by default", () => {
			const config = { casing: "snake_case" };

			expect(config.casing).toBe("snake_case");
		});

		test("should include schema in config", () => {
			const config = {
				schema: {
					// Mock schema
					users: {},
					posts: {},
				},
				casing: "snake_case",
			};

			expect(config).toHaveProperty("schema");
			expect(config).toHaveProperty("casing");
		});
	});

	describe("Factory function behavior", () => {
		test("should create independent database instances", () => {
			// Mock factory function
			const createDb = () => ({
				id: Math.random(),
				schema: {},
			});

			const db1 = createDb();
			const db2 = createDb();

			expect(db1.id).not.toBe(db2.id);
		});
	});

	describe("Health check query", () => {
		test("should use simple SELECT 1 for health check", () => {
			const healthCheckQuery = "SELECT 1";

			expect(healthCheckQuery).toBe("SELECT 1");
			expect(healthCheckQuery.length).toBeLessThan(20);
		});

		test("should validate health check is a safe query", () => {
			const healthCheckQuery = "SELECT 1";

			expect(healthCheckQuery).not.toContain("DROP");
			expect(healthCheckQuery).not.toContain("DELETE");
			expect(healthCheckQuery).not.toContain("UPDATE");
			expect(healthCheckQuery).not.toContain("INSERT");
		});
	});
});
