import { expect, test } from "@playwright/test";

test.describe("Frontend E2E Tests", () => {
	test("should load the homepage", async ({ page }) => {
		await page.goto("/");

		// Wait for page to load
		await page.waitForLoadState("networkidle");

		// Check title
		await expect(page).toHaveTitle(/POLY INSIDER/i);

		// Check that the root element exists
		const root = page.locator("#root");
		await expect(root).toBeVisible();
	});

	test("should have working API health endpoint", async ({ request }) => {
		// Test the API health endpoint directly
		const apiBaseUrl = process.env.API_BASE_URL || "http://localhost:4000";
		const response = await request.get(`${apiBaseUrl}/health`);

		expect(response.ok()).toBeTruthy();
		expect(response.status()).toBe(200);

		const data = await response.json();
		expect(data).toHaveProperty("status");
		expect(data.status).toBe("ok");
	});

	test("should load global stats from API", async ({ request }) => {
		const apiBaseUrl = process.env.API_BASE_URL || "http://localhost:4000";
		const response = await request.get(`${apiBaseUrl}/global-stats`);

		expect(response.ok()).toBeTruthy();
		const data = await response.json();

		// Verify stats structure
		expect(data).toHaveProperty("totalInsiders");
		expect(data).toHaveProperty("totalVolume");
		expect(data).toHaveProperty("totalTrades");
	});

	test("should load markets from API", async ({ request }) => {
		const apiBaseUrl = process.env.API_BASE_URL || "http://localhost:4000";
		const response = await request.get(
			`${apiBaseUrl}/markets?page=1&limit=10`,
		);

		expect(response.ok()).toBeTruthy();
		const data = await response.json();

		// Verify pagination structure
		expect(data).toHaveProperty("data");
		expect(data).toHaveProperty("pagination");
		expect(Array.isArray(data.data)).toBeTruthy();

		// Verify pagination fields
		expect(data.pagination).toHaveProperty("page");
		expect(data.pagination).toHaveProperty("limit");
		expect(data.pagination).toHaveProperty("total");
	});

	test("should have CORS headers", async ({ request }) => {
		const apiBaseUrl = process.env.API_BASE_URL || "http://localhost:4000";
		const response = await request.get(`${apiBaseUrl}/health`);

		const headers = response.headers();
		expect(headers["access-control-allow-origin"]).toBeDefined();
	});

	test("should handle 404 for non-existent routes", async ({ request }) => {
		const apiBaseUrl = process.env.API_BASE_URL || "http://localhost:4000";
		const response = await request.get(
			`${apiBaseUrl}/this-route-does-not-exist`,
		);

		expect(response.status()).toBe(404);
	});

	test("should navigate through the app", async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");

		// Check if app is interactive
		const root = page.locator("#root");
		await expect(root).toBeVisible();

		// TODO: Add more navigation tests once the frontend routes are defined
	});
});
