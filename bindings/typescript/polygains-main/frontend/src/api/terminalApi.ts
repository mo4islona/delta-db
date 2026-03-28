import type {
	AlertsResponse,
	GlobalStats,
	HealthResponse,
	InsiderStats,
	InsiderTrade,
	MarketsResponse,
	Pagination,
} from "../types/terminal";

const DEFAULT_PAGINATION: Pagination = {
	page: 1,
	limit: 6,
	totalPages: 1,
	total: 0,
	hasPrev: false,
	hasNext: false,
};

export interface PaginatedResult<T> {
	data: T[];
	pagination: Pagination;
}

function getConfiguredApiBase(): string {
	const envBase =
		(typeof process !== "undefined"
			? process.env?.BUN_PUBLIC_API_BASE_URL
			: undefined) ?? "";
	const raw = envBase.trim();

	if (typeof window !== "undefined" && window.location?.hostname) {
		const hostname = window.location.hostname;
		// Production domains use api.polygains.com
		if (hostname === "polygains.com" || hostname === "www.polygains.com" || hostname === "staging.polygains.com") {
			return "https://api.polygains.com";
		}
		// Local dev server (port 4033) should call API on port 4069
		if (hostname === "localhost" || hostname === "127.0.0.1") {
			return "http://127.0.0.1:4069";
		}
	}

	if (!raw && typeof window !== "undefined" && window.location?.origin) {
		return window.location.origin;
	}
	if (!raw) throw new Error("Missing BUN_PUBLIC_API_BASE_URL");

	if (/^https?:\/\//i.test(raw)) {
		return raw;
	}

	return `http://${raw}`;
}

function buildApiUrl(
	pathname: string,
	query?: Record<string, string | number | boolean | undefined>,
): string {
	const normalizedPath = pathname.startsWith("/") ? pathname : `/${pathname}`;
	const candidateBase = getConfiguredApiBase();

	const finalize = (base: string) => {
		const url = new URL(normalizedPath, base.endsWith("/") ? base : `${base}/`);
		if (query) {
			for (const [key, value] of Object.entries(query)) {
				if (value === undefined) continue;
				url.searchParams.set(key, String(value));
			}
		}
		return url.toString();
	};

	try {
		return finalize(candidateBase);
	} catch {
		if (typeof window === "undefined" || !window.location?.origin) {
			throw new Error(
				"Cannot build API URL without browser origin or BUN_PUBLIC_API_BASE_URL",
			);
		}
		const fallbackBase = window.location.origin;
		return finalize(fallbackBase);
	}
}

async function getJson<T>(
	pathname: string,
	query?: Record<string, string | number | boolean | undefined>,
): Promise<T> {
	const response = await fetch(buildApiUrl(pathname, query));
	if (!response.ok) {
		throw new Error(`${response.status} ${response.statusText}`);
	}

	const text = await response.text();
	try {
		return JSON.parse(text) as T;
	} catch {
		throw new Error(
			`Expected JSON response but received: ${text.slice(0, 120)}`,
		);
	}
}

function normalizePagination(raw: unknown): Pagination {
	const value = (raw ?? {}) as Partial<Pagination>;
	return {
		page: Number.isFinite(Number(value.page)) ? Number(value.page) : 1,
		limit: Number.isFinite(Number(value.limit)) ? Number(value.limit) : 10,
		totalPages: Number.isFinite(Number(value.totalPages))
			? Number(value.totalPages)
			: 1,
		total: Number.isFinite(Number(value.total)) ? Number(value.total) : 0,
		hasPrev: Boolean(value.hasPrev),
		hasNext: Boolean(value.hasNext),
	};
}

export async function fetchHealth(): Promise<HealthResponse> {
	try {
		return await getJson<HealthResponse>("/health");
	} catch {
		return { status: "error", current_block: 0 };
	}
}

export async function fetchInsiderStats(): Promise<InsiderStats> {
	try {
		return await getJson<InsiderStats>("/stats");
	} catch {
		return {
			total_insiders: 0,
			yes_insiders: 0,
			no_insiders: 0,
			total_volume: 0,
			current_block: 0,
		};
	}
}

export async function fetchGlobalStats(): Promise<GlobalStats> {
	try {
		return await getJson<GlobalStats>("/global-stats");
	} catch {
		return {
			total_accounts: 0,
			total_markets: 0,
			total_trades: 0,
			active_positions: 0,
		};
	}
}

export interface CategoryCount {
	name: string;
	count: number;
	enabled: boolean;
	group?: string;
	displayName?: string;
}

export async function fetchCategories(): Promise<string[]> {
	try {
		const payload = await getJson<string[]>("/categories");
		return Array.isArray(payload) ? payload : ["ALL"];
	} catch {
		return ["ALL"];
	}
}

// Default fallback categories when API fails
const FALLBACK_CATEGORIES: CategoryCount[] = [
	{ name: "ALL", count: 0, enabled: true },
	{ name: "CRYPTO", count: 0, enabled: true },
	{ name: "SPORTS", count: 0, enabled: true },
	{ name: "POLITICS", count: 0, enabled: true },
];

export async function fetchCategoriesWithCounts(): Promise<CategoryCount[]> {
	try {
		const payload = await getJson<CategoryCount[]>("/categories-with-counts");
		if (Array.isArray(payload) && payload.length > 0) {
			return payload;
		}
		return FALLBACK_CATEGORIES;
	} catch (err) {
		console.warn("Failed to fetch categories with counts, using fallback:", err);
		return FALLBACK_CATEGORIES;
	}
}

export async function fetchAlerts(
	page = 1,
	limit = 10,
	category?: string,
): Promise<AlertsResponse> {
	const payload = await getJson<unknown>("/alerts", {
		page,
		limit,
		category,
	});

	if (Array.isArray(payload)) {
		return {
			data: payload,
			pagination: {
				...DEFAULT_PAGINATION,
				total: payload.length,
			},
		} as AlertsResponse;
	}

	const structured = payload as Partial<AlertsResponse>;
	return {
		data: Array.isArray(structured.data) ? structured.data : [],
		pagination: normalizePagination(structured.pagination),
	};
}

export async function fetchInsiderTrades(
	address: string,
): Promise<InsiderTrade[]> {
	const payload = await fetchInsiderTradesPage(address, 1, 100);
	return payload.data;
}

export async function fetchInsiders(
	page = 1,
	limit = 10,
): Promise<PaginatedResult<Record<string, unknown>>> {
	const payload = await getJson<unknown>("/insiders", { page, limit });

	if (Array.isArray(payload)) {
		return {
			data: payload as Record<string, unknown>[],
			pagination: {
				...DEFAULT_PAGINATION,
				page,
				limit,
				total: payload.length,
				totalPages: 1,
			},
		};
	}

	const structured = payload as Partial<PaginatedResult<Record<string, unknown>>>;
	return {
		data: Array.isArray(structured.data)
			? (structured.data as Record<string, unknown>[])
			: [],
		pagination: normalizePagination(structured.pagination),
	};
}

export async function fetchInsiderTradesPage(
	address: string,
	page = 1,
	limit = 10,
): Promise<PaginatedResult<InsiderTrade>> {
	try {
		const payload = await getJson<unknown>(
			`/insider-trades/${encodeURIComponent(address)}`,
			{ page, limit },
		);

		if (Array.isArray(payload)) {
			return {
				data: payload as InsiderTrade[],
				pagination: {
					...DEFAULT_PAGINATION,
					page,
					limit,
					total: payload.length,
					totalPages: 1,
				},
			};
		}

		const structured = payload as Partial<PaginatedResult<InsiderTrade>>;
		return {
			data: Array.isArray(structured.data)
				? (structured.data as InsiderTrade[])
				: [],
			pagination: normalizePagination(structured.pagination),
		};
	} catch {
		return {
			data: [],
			pagination: {
				...DEFAULT_PAGINATION,
				page,
				limit,
			},
		};
	}
}

export async function fetchMarkets(
	page = 1,
	limit = 10,
	close?: boolean,
): Promise<MarketsResponse> {
	const payload = await getJson<unknown>("/markets", {
		page,
		limit,
		close,
	});

	if (Array.isArray(payload)) {
		return {
			data: payload,
			pagination: {
				...DEFAULT_PAGINATION,
				total: payload.length,
			},
		} as MarketsResponse;
	}

	const structured = payload as Partial<MarketsResponse>;
	return {
		data: Array.isArray(structured.data) ? structured.data : [],
		pagination: normalizePagination(structured.pagination),
	};
}

export async function fetchTopLiquidityMarkets(
	page = 1,
	limit = 10,
	close?: boolean,
): Promise<MarketsResponse> {
	try {
		const payload = await getJson<unknown>("/top-liquidity-markets", {
			page,
			limit,
			close,
		});

		if (Array.isArray(payload)) {
			return {
				data: payload,
				pagination: {
					...DEFAULT_PAGINATION,
					total: payload.length,
				},
			} as MarketsResponse;
		}

		const structured = payload as Partial<MarketsResponse>;
		return {
			data: Array.isArray(structured.data) ? structured.data : [],
			pagination: normalizePagination(structured.pagination),
		};
	} catch {
		return fetchMarkets(page, limit, close);
	}
}

export async function fetchMarket(
	conditionId: string,
): Promise<Record<string, unknown> | null> {
	try {
		return await getJson<Record<string, unknown>>(
			`/market/${encodeURIComponent(conditionId)}`,
		);
	} catch {
		return null;
	}
}

export interface SignupResponse {
	success: boolean;
	message?: string;
	error?: string;
}

export async function postSignup(email: string): Promise<SignupResponse> {
	const response = await fetch(buildApiUrl("/signup"), {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ email }),
	});

	const text = await response.text();
	let data: SignupResponse;
	try {
		data = JSON.parse(text) as SignupResponse;
	} catch {
		data = { success: false, error: "Invalid response from server" };
	}

	if (!response.ok) {
		return {
			success: false,
			error: data.error || `Error ${response.status}: ${response.statusText}`,
		};
	}

	return data;
}
