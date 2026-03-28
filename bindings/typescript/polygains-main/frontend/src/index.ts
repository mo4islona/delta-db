import { serve } from "bun";
import index from "./index.html";
import { join } from "path";

const API_BASE_URL =
	process.env.BUN_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:4069";
const PORT = parseInt(process.env.FRONTEND_PORT ?? "4033", 10);
const PUBLIC_DIR = join(import.meta.dir, "..", "public");

// Serve static files from public directory
async function serveStatic(pathname: string): Promise<Response | null> {
	// URL decode the pathname to handle spaces and special characters
	const decodedPath = decodeURIComponent(pathname);
	const filePath = join(PUBLIC_DIR, decodedPath);
	const file = Bun.file(filePath);
	const exists = await file.exists();
	if (exists) {
		return new Response(file);
	}
	return null;
}

// Simple API proxy for local development
async function proxyToApi(req: Request, pathname: string): Promise<Response> {
	const target = new URL(pathname + new URL(req.url).search, API_BASE_URL);

	const headers = new Headers();
	req.headers.forEach((value, key) => {
		if (!["connection", "keep-alive", "upgrade"].includes(key.toLowerCase())) {
			headers.set(key, value);
		}
	});

	try {
		return await fetch(target, {
			method: req.method,
			headers,
			body:
				req.method !== "GET" && req.method !== "HEAD"
					? await req.blob()
					: undefined,
		});
	} catch (error) {
		return Response.json(
			{ error: "api_unreachable", detail: String(error) },
			{ status: 502 },
		);
	}
}

const server = serve({
	port: PORT,
	routes: {
		// Static routes first
		"/": index,
		"/mainv2": index,
		"/legacy": index,
		"/terminal/:id": index,
		// API routes - must be before catch-all
		"/health": async (req) => proxyToApi(req, "/health"),
		"/stats": async (req) => proxyToApi(req, "/stats"),
		"/global-stats": async (req) => proxyToApi(req, "/global-stats"),
		"/categories": async (req) => proxyToApi(req, "/categories"),
		"/markets": async (req) => proxyToApi(req, "/markets"),
		"/top-liquidity-markets": async (req) =>
			proxyToApi(req, "/top-liquidity-markets"),
		"/insiders": async (req) => proxyToApi(req, "/insiders"),
		"/alerts": async (req) => proxyToApi(req, "/alerts"),
		"/block": async (req) => proxyToApi(req, "/block"),
		"/market/:id": async (req) => proxyToApi(req, `/market/${req.params.id}`),
		"/insider-trades/:id": async (req) =>
			proxyToApi(req, `/insider-trades/${req.params.id}`),
		// Static assets - must be before catch-all
		"/assets/*": async (req) => {
			const pathname = new URL(req.url).pathname;
			const response = await serveStatic(pathname);
			if (response) return response;
			return new Response("Not found", { status: 404 });
		},
		// Catch-all LAST (Bun matches in order, not by specificity)
		"/api/*": async (req) => {
			const pathname = new URL(req.url).pathname.replace(/^\/api/, "");
			return proxyToApi(req, pathname || "/");
		},
		"/*": async (req) => {
			// For non-API routes, serve the index (SPA behavior)
			const pathname = new URL(req.url).pathname;
			// If it looks like an API path, proxy it
			if (pathname.startsWith("/")) {
				return proxyToApi(req, pathname);
			}
			return index;
		},
	},
	development: {
		hmr: true,
		console: true,
	},
});

console.log(`🚀 Server running at ${server.url}`);
console.log(`📡 API proxy: ${API_BASE_URL}`);
