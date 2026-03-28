"use client";
function resolveApiBase(): string {
	const raw =
		(typeof process !== "undefined"
			? process.env?.BUN_PUBLIC_API_BASE_URL
			: undefined) ?? "";
	const trimmed = raw.trim();

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
		if (!trimmed && window.location.origin) {
			return window.location.origin;
		}
	}

	if (!trimmed) {
		return "";
	}
	if (/^https?:\/\//i.test(trimmed)) {
		return trimmed;
	}
	return `http://${trimmed}`;
}

export async function swrJsonFetcher<T>(
	resource: string,
	init?: RequestInit,
): Promise<T> {
	let requestUrl = resource;
	if (resource.startsWith("/")) {
		const base = resolveApiBase();
		if (base) {
			requestUrl = new URL(resource, base.endsWith("/") ? base : `${base}/`).toString();
		}
	}

	const response = await fetch(requestUrl, init);
	if (!response.ok) {
		throw new Error(`${response.status} ${response.statusText}`);
	}

	const text = await response.text();
	if (!text) {
		return {} as T;
	}

	try {
		return JSON.parse(text) as T;
	} catch {
		throw new Error(`Expected JSON but received: ${text.slice(0, 120)}`);
	}
}
