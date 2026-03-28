import { test, expect } from "@playwright/test";

/**
 * Integration test to verify pagination returns different data for different pages
 * This ensures the file cache bug is fixed and pages are properly distinct
 */

test.describe("Pagination - Different Pages Return Different Data", () => {
	test("markets endpoint returns different data for page 1 vs page 2", async ({
		request,
	}) => {
		const baseUrl = process.env.API_BASE_URL || "http://127.0.0.1:4069";

		// Fetch page 1 and page 2
		const page1Response = await request.get(
			`${baseUrl}/top-liquidity-markets?page=1&limit=5`,
		);
		const page2Response = await request.get(
			`${baseUrl}/top-liquidity-markets?page=2&limit=5`,
		);

		expect(page1Response.ok()).toBeTruthy();
		expect(page2Response.ok()).toBeTruthy();

		const page1Data = await page1Response.json();
		const page2Data = await page2Response.json();

		// Verify pagination metadata
		expect(page1Data.pagination.page).toBe(1);
		expect(page2Data.pagination.page).toBe(2);
		expect(page1Data.pagination.limit).toBe(5);
		expect(page2Data.pagination.limit).toBe(5);

		// Verify we got data arrays (markets endpoint returns outcomes - 2 per market)
		expect(Array.isArray(page1Data.data)).toBeTruthy();
		expect(Array.isArray(page2Data.data)).toBeTruthy();
		expect(page1Data.data.length).toBeGreaterThan(0);
		expect(page2Data.data.length).toBeGreaterThan(0);

		// Extract unique conditionIds (markets) for comparison
		const page1ConditionIds = [...new Set(page1Data.data.map((m: { conditionId?: string }) => m.conditionId))];
		const page2ConditionIds = [...new Set(page2Data.data.map((m: { conditionId?: string }) => m.conditionId))];

		// Should have approximately 'limit' number of markets (each with 2 outcomes)
		expect(page1ConditionIds.length).toBeLessThanOrEqual(5);
		expect(page2ConditionIds.length).toBeLessThanOrEqual(5);

		// CRITICAL: No overlap between page 1 and page 2 market conditionIds
		const overlap = page1ConditionIds.filter((id: string) => page2ConditionIds.includes(id));
		expect(
			overlap.length,
			`Pages 1 and 2 should have different markets, but found overlap: ${overlap.join(", ")}`,
		).toBe(0);

		console.log(`✓ Markets: Page 1 has ${page1ConditionIds.length} markets, Page 2 has ${page2ConditionIds.length} markets`);
		console.log(`✓ No overlapping markets between pages`);
	});

	test("alerts endpoint returns different data for page 1 vs page 2", async ({
		request,
	}) => {
		const baseUrl = process.env.API_BASE_URL || "http://127.0.0.1:4069";

		// Fetch page 1 and page 2
		const page1Response = await request.get(
			`${baseUrl}/alerts?page=1&limit=10`,
		);
		const page2Response = await request.get(
			`${baseUrl}/alerts?page=2&limit=10`,
		);

		expect(page1Response.ok()).toBeTruthy();
		expect(page2Response.ok()).toBeTruthy();

		const page1Data = await page1Response.json();
		const page2Data = await page2Response.json();

		// Verify pagination metadata
		expect(page1Data.pagination.page).toBe(1);
		expect(page2Data.pagination.page).toBe(2);

		// Verify we got data arrays
		expect(Array.isArray(page1Data.data)).toBeTruthy();
		expect(Array.isArray(page2Data.data)).toBeTruthy();
		expect(page1Data.data.length).toBeGreaterThan(0);
		expect(page2Data.data.length).toBeGreaterThan(0);

		// CRITICAL: Data should be different between pages
		// Create unique keys for alerts (user + conditionId + timestamp)
		const createAlertKey = (alert: { user?: string; conditionId?: string; alert_time?: number }) =>
			`${alert.user || ""}-${alert.conditionId || ""}-${alert.alert_time || 0}`;

		const page1Keys = page1Data.data.map(createAlertKey);
		const page2Keys = page2Data.data.map(createAlertKey);

		// No overlap between page 1 and page 2
		const overlap = page1Keys.filter((key: string) => page2Keys.includes(key));
		expect(
			overlap.length,
			`Pages 1 and 2 should have different alerts, but found ${overlap.length} overlaps`,
		).toBe(0);

		console.log(`✓ Alerts: Page 1 has ${page1Keys.length} items, Page 2 has ${page2Keys.length} items`);
		console.log(`✓ No overlapping alerts between pages`);
	});

	test("alerts endpoint respects limit parameter", async ({ request }) => {
		const baseUrl = process.env.API_BASE_URL || "http://127.0.0.1:4069";

		// Test with limit=5
		const response5 = await request.get(`${baseUrl}/alerts?page=1&limit=5`);
		expect(response5.ok()).toBeTruthy();
		const data5 = await response5.json();
		expect(data5.data.length).toBeLessThanOrEqual(5);
		expect(data5.pagination.limit).toBe(5);

		// Test with limit=20
		const response20 = await request.get(`${baseUrl}/alerts?page=1&limit=20`);
		expect(response20.ok()).toBeTruthy();
		const data20 = await response20.json();
		expect(data20.data.length).toBeLessThanOrEqual(20);
		expect(data20.pagination.limit).toBe(20);

		console.log(`✓ Alerts limit=5 returned ${data5.data.length} items`);
		console.log(`✓ Alerts limit=20 returned ${data20.data.length} items`);
	});

	test("markets endpoint limit applies to markets (conditionIds), not outcomes", async ({ request }) => {
		const baseUrl = process.env.API_BASE_URL || "http://127.0.0.1:4069";

		const response = await request.get(
			`${baseUrl}/top-liquidity-markets?page=1&limit=5`,
		);
		expect(response.ok()).toBeTruthy();
		const data = await response.json();

		// limit=5 means 5 markets (conditionIds), but each market has 2 outcomes (Yes/No)
		// So we expect up to 10 outcomes
		const uniqueConditionIds = [...new Set(data.data.map((m: { conditionId?: string }) => m.conditionId))];
		expect(uniqueConditionIds.length).toBeLessThanOrEqual(5);
		expect(data.pagination.limit).toBe(5);

		console.log(`✓ Markets limit=5 returned ${uniqueConditionIds.length} markets (${data.data.length} total outcomes)`);
	});

	test("alerts endpoint enforces max limit of 100", async ({ request }) => {
		const baseUrl = process.env.API_BASE_URL || "http://127.0.0.1:4069";

		// Request more than max limit
		const response = await request.get(`${baseUrl}/alerts?page=1&limit=200`);
		expect(response.ok()).toBeTruthy();
		const data = await response.json();

		// Should be capped at 100
		expect(data.data.length).toBeLessThanOrEqual(100);
		expect(data.pagination.limit).toBe(100);

		console.log(`✓ Alerts limit=200 was capped to ${data.pagination.limit}`);
	});

	test("sequential pages return sequentially different data", async ({ request }) => {
		const baseUrl = process.env.API_BASE_URL || "http://127.0.0.1:4069";

		// Fetch pages 1, 2, and 3
		const [page1Res, page2Res, page3Res] = await Promise.all([
			request.get(`${baseUrl}/alerts?page=1&limit=10`),
			request.get(`${baseUrl}/alerts?page=2&limit=10`),
			request.get(`${baseUrl}/alerts?page=3&limit=10`),
		]);

		const page1 = await page1Res.json();
		const page2 = await page2Res.json();
		const page3 = await page3Res.json();

		const createKey = (alert: { user?: string; conditionId?: string; alert_time?: number }) =>
			`${alert.user || ""}-${alert.conditionId || ""}-${alert.alert_time || 0}`;

		const keys1 = new Set(page1.data.map(createKey));
		const keys2 = new Set(page2.data.map(createKey));
		const keys3 = new Set(page3.data.map(createKey));

		// Check no overlap between consecutive pages
		const overlap12 = [...keys1].filter(k => keys2.has(k));
		const overlap23 = [...keys2].filter(k => keys3.has(k));

		expect(overlap12.length, "Pages 1 and 2 should not overlap").toBe(0);
		expect(overlap23.length, "Pages 2 and 3 should not overlap").toBe(0);

		console.log(`✓ Pages 1, 2, 3 are all distinct (no overlapping data)`);
	});
});
