import {
	getCategories,
	getCategoriesWithCounts,
	getCurrentBlock,
	getGlobalStats,
	getInsiderAlertsOptimized,
	getInsiderStats,
	getInsidersList,
	getInsiderTrades,
	getMarketByCondition,
	getMarketsOptimized,
} from "@/lib/db/queries";
import { generateCacheKey, getCache, setCache } from "@/lib/file-cache";
import {
	parseOptionalBoolean,
	parseOptionalString,
	parsePositiveInt,
	readEnv,
	readPort,
} from "@/lib/utils";

const DEFAULT_PAGE = 1;
const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 100;
const DEFAULT_IDLE_TIMEOUT_SEC = 60;
const CACHE_TTL_MS = 30_000;

const getCorsHeaders = (req: Request): Record<string, string> => {
	const origin = req.headers.get("origin") ?? "";

	// Base allowed origins
	const allowedOrigins = [
		"https://polygains.com",
		"https://www.polygains.com",
		"https://staging.polygains.com",
		"https://api.polygains.com",
		"http://localhost:4033",
		"http://127.0.0.1:4033",
		"http://localhost:4033",
		"http://127.0.0.1:4033",
	];

	// Add custom origins from env var (comma-separated)
	const customOrigins =
		process.env.CORS_ORIGINS?.split(",")
			.map((s) => s.trim())
			.filter(Boolean) ?? [];
	allowedOrigins.push(...customOrigins);

	// Allow if origin matches or if no origin (same-origin requests)
	const allowOrigin =
		!origin || allowedOrigins.includes(origin)
			? origin || "*"
			: allowedOrigins[0];

	return {
		"Access-Control-Allow-Origin": allowOrigin,
		"Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
		"Access-Control-Allow-Headers": "Content-Type, Authorization",
		"Access-Control-Allow-Credentials": "true",
	};
};

const json = (
	body: unknown,
	status = 200,
	cacheGeneration?: number,
	req?: Request,
): Response => {
	const headers: Record<string, string> = {
		"Content-Type": "application/json",
		...getCorsHeaders(req || new Request("http://localhost")),
	};
	// Add cache headers for cacheable responses
	if (cacheGeneration !== undefined) {
		headers["X-Cache-Generation"] = String(cacheGeneration);
		headers["Cache-Control"] = "public, max-age=5";
	}
	return new Response(JSON.stringify(body), { status, headers });
};

const makePagination = (page: number, limit: number, total: number) => {
	const totalPages = Math.max(1, Math.ceil(total / limit));
	const safePage = Math.min(Math.max(page, 1), totalPages);

	return {
		page: safePage,
		limit,
		total,
		totalPages,
		hasPrev: safePage > 1,
		hasNext: safePage < totalPages,
	};
};

const toOffset = (page: number, limit: number): number =>
	Math.max(0, (page - 1) * limit);

export function createServer() {
	const host = readEnv("API_HOST", "HOST");
	const port = readPort("API_PORT", "PORT");
	const idleTimeout = process.env.IDLE_TIMEOUT_SEC
		? Number.parseInt(process.env.IDLE_TIMEOUT_SEC, 10)
		: DEFAULT_IDLE_TIMEOUT_SEC;

	const server = Bun.serve({
		hostname: host,
		port,
		idleTimeout,
		async fetch(req) {
			const url = new URL(req.url);

			// HTTPS redirect (for production behind reverse proxy)
			const proto = req.headers.get("x-forwarded-proto");
			if (proto === "http") {
				return Response.redirect(
					`https://${url.host}${url.pathname}${url.search}`,
					301,
				);
			}

			// Add OPTIONS handling for CORS
			if (req.method === "OPTIONS") {
				return new Response(null, { headers: getCorsHeaders(req) });
			}

			if (url.pathname === "/health" || url.pathname === "/api/health") {
				return json({ status: "ok" }, 200, undefined, req);
			}

			if (url.pathname === "/stats" || url.pathname === "/api/stats") {
				const stats = await getInsiderStats();
				return json(stats, 200, undefined, req);
			}

			if (
				url.pathname === "/global-stats" ||
				url.pathname === "/api/global-stats"
			) {
				const stats = await getGlobalStats();
				return json(stats, 200, undefined, req);
			}

			if (
				url.pathname === "/categories" ||
				url.pathname === "/api/categories"
			) {
				const categories = await getCategories();
				return json(categories, 200, undefined, req);
			}

			if (
				url.pathname === "/categories-with-counts" ||
				url.pathname === "/api/categories-with-counts"
			) {
				const categories = await getCategoriesWithCounts();
				return json(categories, 200, undefined, req);
			}

			if (
				url.pathname === "/api/markets" ||
				url.pathname === "/markets" ||
				url.pathname === "/api/top-liquidity-markets" ||
				url.pathname === "/top-liquidity-markets"
			) {
				const cacheKey = generateCacheKey(url.toString());
				const cached = await getCache<{ data: unknown; pagination: unknown }>(
					cacheKey,
				);
				if (cached) return json(cached, 200, undefined, req);

				const page = parsePositiveInt(
					url.searchParams.get("page"),
					DEFAULT_PAGE,
				);
				const limit = Math.min(
					parsePositiveInt(url.searchParams.get("limit"), DEFAULT_LIMIT),
					MAX_LIMIT,
				);
				const closed = parseOptionalBoolean(url.searchParams.get("close"));
				const { markets, total } = await getMarketsOptimized(
					page,
					limit,
					closed,
				);
				const pagination = makePagination(page, limit, total);
				const responseData = { data: markets, pagination };
				await setCache(cacheKey, responseData, CACHE_TTL_MS);
				return json(responseData, 200, undefined, req);
			}

			if (
				url.pathname.startsWith("/api/market/") ||
				url.pathname.startsWith("/api/markets/") ||
				url.pathname.startsWith("/market/")
			) {
				const conditionId = url.pathname.split("/").pop();
				if (!conditionId)
					return json({ error: "Missing conditionId" }, 400, undefined, req);

				const market = await getMarketByCondition(conditionId);
				if (!market)
					return json({ error: "Market not found" }, 404, undefined, req);

				return json(market, 200, undefined, req);
			}

			if (url.pathname === "/api/insiders" || url.pathname === "/insiders") {
				const page = parsePositiveInt(
					url.searchParams.get("page"),
					DEFAULT_PAGE,
				);
				const limit = Math.min(
					parsePositiveInt(url.searchParams.get("limit"), DEFAULT_LIMIT),
					MAX_LIMIT,
				);
				const offset = toOffset(page, limit);

				const insiders = await getInsidersList();
				const total = insiders.length;
				const pagedInsiders = insiders.slice(offset, offset + limit);
				const pagination = makePagination(page, limit, total);
				return json({ data: pagedInsiders, pagination }, 200, undefined, req);
			}

			if (
				url.pathname.startsWith("/api/insider-trades/") ||
				url.pathname.startsWith("/insider-trades/")
			) {
				const address = url.pathname.split("/").pop();
				if (!address)
					return json({ error: "Missing address" }, 400, undefined, req);

				const page = parsePositiveInt(
					url.searchParams.get("page"),
					DEFAULT_PAGE,
				);
				const limit = Math.min(
					parsePositiveInt(url.searchParams.get("limit"), DEFAULT_LIMIT),
					MAX_LIMIT,
				);
				const offset = toOffset(page, limit);

				const trades = await getInsiderTrades(address);
				const total = trades.length;
				const pagedTrades = trades.slice(offset, offset + limit);
				const pagination = makePagination(page, limit, total);

				return json({ data: pagedTrades, pagination }, 200, undefined, req);
			}

			if (
				url.pathname.startsWith("/api/insiders/") ||
				url.pathname.startsWith("/insiders/")
			) {
				const parts = url.pathname.split("/");
				const isApiRoute = parts[1] === "api";
				const address = isApiRoute ? parts[3] : parts[2];

				if (!address)
					return json({ error: "Missing address" }, 400, undefined, req);

				if (url.pathname.endsWith("/stats")) {
					const stats = await getInsiderStats();
					return json(stats, 200, undefined, req);
				}

				if (url.pathname.endsWith("/trades")) {
					const page = parsePositiveInt(
						url.searchParams.get("page"),
						DEFAULT_PAGE,
					);
					const limit = Math.min(
						parsePositiveInt(url.searchParams.get("limit"), DEFAULT_LIMIT),
						MAX_LIMIT,
					);
					const offset = toOffset(page, limit);

					const trades = await getInsiderTrades(address);
					const total = trades.length;
					const pagedTrades = trades.slice(offset, offset + limit);
					const pagination = makePagination(page, limit, total);
					return json({ data: pagedTrades, pagination }, 200, undefined, req);
				}
			}

			if (url.pathname === "/api/alerts" || url.pathname === "/alerts") {
				const cacheKey = generateCacheKey(url.toString());
				const cached = await getCache<{ data: unknown; pagination: unknown }>(
					cacheKey,
				);
				if (cached) return json(cached, 200, undefined, req);

				const page = parsePositiveInt(
					url.searchParams.get("page"),
					DEFAULT_PAGE,
				);
				const limit = Math.min(
					parsePositiveInt(url.searchParams.get("limit"), DEFAULT_LIMIT),
					MAX_LIMIT,
				);
				const category = parseOptionalString(url.searchParams.get("category"));
				const { alerts, total } = await getInsiderAlertsOptimized(
					page,
					limit,
					category,
				);
				const pagination = makePagination(page, limit, total);
				const responseData = { data: alerts, pagination };
				await setCache(cacheKey, responseData, CACHE_TTL_MS);
				return json(responseData, 200, undefined, req);
			}

			if (url.pathname === "/api/block" || url.pathname === "/block") {
				const block = await getCurrentBlock();
				return json({ block }, 200, undefined, req);
			}

			if (url.pathname === "/api/signup" || url.pathname === "/signup") {
				if (req.method !== "POST") {
					return json({ error: "Method not allowed" }, 405, undefined, req);
				}

				try {
					const body = await req.json();
					const email = body?.email?.trim();

					// Validate email
					const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
					if (!email || !emailRegex.test(email)) {
						return json({ error: "Invalid email address" }, 400, undefined, req);
					}

					// Forward to Google Apps Script
					const response = await fetch(
						"https://script.google.com/macros/s/AKfycbwP10TDgJrd8n44PJdawZPy6gWjyIfOIrx__A1sRV3YoO6eWIpxe2h_5kRlAXgpcdwQ_g/exec",
						{
							method: "POST",
							headers: { "Content-Type": "application/json" },
							body: JSON.stringify({ mail: email }),
						},
					);

					if (!response.ok) {
						throw new Error(`Google Script returned ${response.status}`);
					}

					return json({ success: true, message: "Subscribed successfully" }, 200, undefined, req);
				} catch (error) {
					console.error("[Signup] Error:", error);
					return json(
						{ error: "Failed to process signup" },
						500,
						undefined,
						req,
					);
				}
			}

			// API-only server - no static file serving
			// Return 404 for non-API routes
			return json({ error: "Not Found" }, 404, undefined, req);
		},
	});

	console.log(
		`[API] Server running at http://${server.hostname}:${server.port}`,
	);
	return server;
}

if (import.meta.main) {
	createServer();
}
