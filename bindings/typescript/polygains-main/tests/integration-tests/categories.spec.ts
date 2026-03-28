import { expect, test } from "@playwright/test";
import path from "path";

const API_BASE_URL = process.env.API_BASE_URL || "http://localhost:4069";
const FRONTEND_URL = process.env.FRONTEND_URL || "http://localhost:4033";

interface CategoryCount {
	name: string;
	count: number;
	enabled: boolean;
	group?: string;
	displayName?: string;
}

// Helper to take screenshots
test.beforeEach(async ({ page }, testInfo) => {
	// Add screenshot on failure
	testInfo.attachments = [];
});

test.describe("Categories API Tests", () => {
	test("should return categories with counts from /categories-with-counts", async ({
		request,
	}, testInfo) => {
		const response = await request.get(`${API_BASE_URL}/categories-with-counts`);

		// Screenshot the response for debugging
		const responseBody = await response.text();
		console.log("Categories API response:", responseBody.substring(0, 500));

		if (!response.ok()) {
			console.error(`API returned ${response.status()}: ${responseBody}`);
		}

		expect(response.ok()).toBeTruthy();
		expect(response.status()).toBe(200);

		const data = JSON.parse(responseBody) as CategoryCount[];
		expect(Array.isArray(data)).toBeTruthy();
		expect(data.length).toBeGreaterThan(0);

		// First category should be ALL
		expect(data[0].name).toBe("ALL");
		expect(data[0].enabled).toBe(true);

		// Verify each category has required fields
		for (const category of data) {
			expect(category).toHaveProperty("name");
			expect(category).toHaveProperty("count");
			expect(category).toHaveProperty("enabled");
			expect(typeof category.name).toBe("string");
			expect(typeof category.count).toBe("number");
			expect(typeof category.enabled).toBe("boolean");
		}

		// Attach response for debugging
		await testInfo.attach("api-response.json", {
			body: responseBody,
			contentType: "application/json",
		});
	});

	test("should not include empty categories", async ({ request }, testInfo) => {
		const response = await request.get(`${API_BASE_URL}/categories-with-counts`);
		const data = JSON.parse(await response.text()) as CategoryCount[];

		// Check that categories with 0 count are disabled
		const emptyCategories = data.filter((c) => c.count === 0 && c.name !== "ALL");
		for (const cat of emptyCategories) {
			expect(cat.enabled).toBe(false);
		}
	});

	test("legacy /categories endpoint should still work", async ({
		request,
	}, testInfo) => {
		const response = await request.get(`${API_BASE_URL}/categories`);

		expect(response.ok()).toBeTruthy();
		expect(response.status()).toBe(200);

		const data = JSON.parse(await response.text()) as string[];
		expect(Array.isArray(data)).toBeTruthy();
		expect(data.length).toBeGreaterThan(0);
		expect(data).toContain("ALL");

		// All items should be strings (uppercase)
		for (const category of data) {
			expect(typeof category).toBe("string");
			expect(category).toBe(category.toUpperCase());
		}
	});
});

test.describe("Categories Frontend - Screenshots", () => {
	test("initial load - screenshot category filters", async ({ page }, testInfo) => {
		await page.goto(FRONTEND_URL);
		await page.waitForLoadState("networkidle");

		// Wait for category buttons to appear
		await page.waitForSelector('button:has-text("ALL")', { timeout: 10000 });

		// Take screenshot of the entire page
		await page.screenshot({
			path: `test-results/categories-initial.png`,
			fullPage: false,
		});

		await testInfo.attach("initial-load.png", {
			path: `test-results/categories-initial.png`,
			contentType: "image/png",
		});

		// Check that main category buttons exist
		const allButton = page.locator('button:has-text("ALL")');
		await expect(allButton).toBeVisible();

		// Check for CRYPTO, SPORTS, POLITICS buttons
		const cryptoButton = page.locator('button:has-text("Crypto")');
		const sportsButton = page.locator('button:has-text("Sports")');
		const politicsButton = page.locator('button:has-text("Politics")');

		// These may or may not be visible depending on data
		const cryptoVisible = await cryptoButton.isVisible().catch(() => false);
		const sportsVisible = await sportsButton.isVisible().catch(() => false);
		const politicsVisible = await politicsButton.isVisible().catch(() => false);

		console.log("Category buttons visible:", {
			crypto: cryptoVisible,
			sports: sportsVisible,
			politics: politicsVisible,
		});
	});

	test("click ... menu - screenshot dropdown", async ({ page }, testInfo) => {
		await page.goto(FRONTEND_URL);
		await page.waitForLoadState("networkidle");

		// Wait for category buttons
		await page.waitForSelector('button:has-text("ALL")', { timeout: 10000 });

		// Find and click the "..." button
		const moreButton = page.locator('button:has-text("...")').first();

		if (await moreButton.isVisible().catch(() => false)) {
			await moreButton.click();

			// Wait a bit for dropdown to appear
			await page.waitForTimeout(300);

			// Take screenshot with dropdown open
			await page.screenshot({
				path: `test-results/categories-dropdown.png`,
				fullPage: false,
			});

			await testInfo.attach("dropdown-open.png", {
				path: `test-results/categories-dropdown.png`,
				contentType: "image/png",
			});
		} else {
			console.log("... button not found, checking for more categories button");

			// Try to find any button that might be the "more" button
			const buttons = await page.locator('button').all();
			for (const button of buttons) {
				const text = await button.textContent();
				console.log("Button text:", text);
			}

			await page.screenshot({
				path: `test-results/categories-no-dropdown.png`,
				fullPage: false,
			});

			await testInfo.attach("no-dropdown.png", {
				path: `test-results/categories-no-dropdown.png`,
				contentType: "image/png",
			});
		}
	});

	test("select category from dropdown", async ({ page }, testInfo) => {
		await page.goto(FRONTEND_URL);
		await page.waitForLoadState("networkidle");

		// Wait for category buttons
		await page.waitForSelector('button:has-text("ALL")', { timeout: 10000 });

		// Click ... to open dropdown
		const moreButton = page.locator('button:has-text("...")').first();

		if (await moreButton.isVisible().catch(() => false)) {
			await moreButton.click();
			await page.waitForTimeout(300);

			// Try to click on an enabled category in the dropdown
			const dropdownButtons = page.locator('div[class*="rounded-box"] button');
			const count = await dropdownButtons.count();

			for (let i = 0; i < count; i++) {
				const button = dropdownButtons.nth(i);
				const isDisabled = await button.isDisabled().catch(() => true);

				if (!isDisabled) {
					const text = await button.textContent();
					console.log("Clicking dropdown item:", text);
					await button.click();
					break;
				}
			}

			await page.waitForTimeout(500);

			// Screenshot after selection
			await page.screenshot({
				path: `test-results/categories-after-select.png`,
				fullPage: false,
			});

			await testInfo.attach("after-select.png", {
				path: `test-results/categories-after-select.png`,
				contentType: "image/png",
			});
		}
	});

	test("mobile view - category filter", async ({ page }, testInfo) => {
		// Set mobile viewport
		await page.setViewportSize({ width: 375, height: 667 });
		await page.goto(FRONTEND_URL);
		await page.waitForLoadState("networkidle");

		// Wait for category buttons
		await page.waitForSelector('button:has-text("ALL")', { timeout: 10000 });

		// Take mobile screenshot
		await page.screenshot({
			path: `test-results/categories-mobile.png`,
			fullPage: false,
		});

		await testInfo.attach("mobile-view.png", {
			path: `test-results/categories-mobile.png`,
			contentType: "image/png",
		});
	});
});

test.describe("Categories Debug", () => {
	test("debug: log all buttons on page", async ({ page }) => {
		await page.goto(FRONTEND_URL);
		await page.waitForLoadState("networkidle");

		// Get all buttons and their text
		const buttons = await page.locator('button').all();
		console.log(`\n=== Found ${buttons.length} buttons ===`);

		for (let i = 0; i < buttons.length; i++) {
			const button = buttons[i];
			const text = await button.textContent();
			const ariaLabel = await button.getAttribute('aria-label');
			const visible = await button.isVisible().catch(() => false);
			console.log(`Button ${i}: text="${text?.trim()}", aria-label="${ariaLabel}", visible=${visible}`);
		}
		console.log("=== End buttons ===\n");
	});

	test("debug: check API response directly", async ({ request }) => {
		const response = await request.get(`${API_BASE_URL}/categories-with-counts`);
		console.log("\n=== API Debug ===");
		console.log("Status:", response.status());
		console.log("Headers:", await response.allHeaders());
		console.log("Body:", await response.text());
		console.log("=== End API Debug ===\n");
	});
});
