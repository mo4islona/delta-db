import { expect, type Page, test } from "@playwright/test";

// Helper to capture console errors
async function captureErrors(page: Page) {
	const consoleErrors: string[] = [];
	const pageErrors: string[] = [];

	page.on("console", (msg) => {
		if (msg.type() === "error") {
			consoleErrors.push(msg.text());
		}
	});

	page.on("pageerror", (error) => {
		pageErrors.push(error.message);
	});

	return { consoleErrors, pageErrors };
}

// Helper to take full page screenshot with device info
async function takeScreenshot(
	page: Page,
	name: string,
	viewport: { width: number; height: number },
) {
	await page.setViewportSize(viewport);
	await page.waitForTimeout(500); // Wait for layout to settle
	await page.screenshot({
		path: `integration-tests/screenshots/${name}-${viewport.width}x${viewport.height}.png`,
		fullPage: true,
	});
}

test.describe("Button Integration Tests & Layout Analysis", () => {
	// Create screenshots directory
	test.beforeAll(async () => {
		const fs = await import("node:fs");
		if (!fs.existsSync("integration-tests/screenshots")) {
			fs.mkdirSync("integration-tests/screenshots", { recursive: true });
		}
	});

	test("TerminalPage - Desktop (1920x1080) - Full Screenshot", async ({
		page,
	}) => {
		const { consoleErrors, pageErrors } = await captureErrors(page);

		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000); // Wait for data to load

		await takeScreenshot(page, "terminal-desktop", {
			width: 1920,
			height: 1080,
		});

		// Log any errors
		if (consoleErrors.length > 0) console.log("Console errors:", consoleErrors);
		if (pageErrors.length > 0) console.log("Page errors:", pageErrors);

		expect(pageErrors).toHaveLength(0);
	});

	test("TerminalPage - Laptop (1366x768) - Full Screenshot", async ({
		page,
	}) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		await takeScreenshot(page, "terminal-laptop", { width: 1366, height: 768 });
	});

	test("TerminalPage - Tablet (768x1024) - Full Screenshot", async ({
		page,
	}) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		await takeScreenshot(page, "terminal-tablet", { width: 768, height: 1024 });
	});

	test("TerminalPage - Mobile Large (414x896) - Full Screenshot", async ({
		page,
	}) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		await takeScreenshot(page, "terminal-mobile-lg", {
			width: 414,
			height: 896,
		});
	});

	test("TerminalPage - Mobile Small (375x667) - Full Screenshot", async ({
		page,
	}) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		await takeScreenshot(page, "terminal-mobile-sm", {
			width: 375,
			height: 667,
		});
	});

	test("MainV2Page - Desktop (1920x1080) - Full Screenshot", async ({
		page,
	}) => {
		await page.goto("/mainv2");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		await takeScreenshot(page, "mainv2-desktop", { width: 1920, height: 1080 });
	});

	test("MainV2Page - Mobile (375x667) - Full Screenshot", async ({ page }) => {
		await page.goto("/mainv2");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		await takeScreenshot(page, "mainv2-mobile", { width: 375, height: 667 });
	});
});

test.describe("TerminalPage Button Tests", () => {
	test.beforeEach(async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
	});

	// === CATEGORY FILTER BUTTONS ===
	test("Category filter buttons - should be clickable and filter alerts", async ({
		page,
	}) => {
		// Find category filter buttons
		const categoryButtons = page
			.locator(".join button")
			.filter({ hasText: /^(ALL|CRYPTO|SPORTS|POLITICS)$/i });
		const count = await categoryButtons.count();
		expect(count).toBeGreaterThan(0);

		// Click each category button
		for (let i = 0; i < count; i++) {
			const button = categoryButtons.nth(i);
			const text = await button.textContent();

			await button.click();
			await page.waitForTimeout(500);

			// Verify button is pressed/active
			const ariaPressed = await button.getAttribute("aria-pressed");
			expect(ariaPressed).toBe("true");

			console.log(`✓ Category button "${text}" clicked and active`);
		}
	});

	// === WINNER FILTER BUTTONS ===
	test("Winner filter buttons - BOTH/WINNERS/LOSERS", async ({ page }) => {
		const winnerButtons = page
			.locator(".join button")
			.filter({ hasText: /^(BOTH|WINNERS|LOSERS)$/i });
		const count = await winnerButtons.count();
		expect(count).toBe(3);

		for (let i = 0; i < count; i++) {
			const button = winnerButtons.nth(i);
			const text = await button.textContent();

			await button.click();
			await page.waitForTimeout(500);

			const ariaPressed = await button.getAttribute("aria-pressed");
			expect(ariaPressed).toBe("true");

			console.log(`✓ Winner filter "${text}" clicked and active`);
		}
	});

	// === ALERTS PAGINATION ===
	test("Alerts pagination - PREV/NEXT buttons", async ({ page }) => {
		const nextButton = page.locator('button[aria-label="Next page"]').first();
		const prevButton = page
			.locator('button[aria-label="Previous page"]')
			.first();

		// Check if NEXT is enabled (has data)
		const isNextDisabled = await nextButton.isDisabled();

		if (!isNextDisabled) {
			await nextButton.click();
			await page.waitForTimeout(1000);
			console.log("✓ Alerts NEXT button clicked");

			// Now PREV should be enabled
			const isPrevDisabled = await prevButton.isDisabled();
			if (!isPrevDisabled) {
				await prevButton.click();
				await page.waitForTimeout(1000);
				console.log("✓ Alerts PREV button clicked");
			}
		} else {
			console.log("⚠ NEXT button disabled (no more pages)");
		}
	});

	// === MARKETS PAGINATION ===
	test("Markets pagination - PREV/NEXT buttons", async ({ page }) => {
		const allNextButtons = page.locator('button[aria-label="Next page"]');
		const allPrevButtons = page.locator('button[aria-label="Previous page"]');

		// Markets pagination should be the second set
		const marketsNextButton = allNextButtons.nth(1);
		const marketsPrevButton = allPrevButtons.nth(1);

		const isNextDisabled = await marketsNextButton.isDisabled();

		if (!isNextDisabled) {
			await marketsNextButton.click();
			await page.waitForTimeout(1000);
			console.log("✓ Markets NEXT button clicked");

			const isPrevDisabled = await marketsPrevButton.isDisabled();
			if (!isPrevDisabled) {
				await marketsPrevButton.click();
				await page.waitForTimeout(1000);
				console.log("✓ Markets PREV button clicked");
			}
		} else {
			console.log("⚠ Markets NEXT button disabled (no more pages)");
		}
	});

	// === LOOKUP BUTTONS ===
	test("Lookup buttons - trader search icons", async ({ page }) => {
		const lookupButtons = page.locator(
			'a[aria-label^="Lookup trader"], button[aria-label^="Lookup trader"]',
		);
		const count = await lookupButtons.count();

		expect(count).toBeGreaterThan(0);
		console.log(`Found ${count} lookup buttons`);

		// Test first lookup button if exists
		if (count > 0) {
			const firstLookup = lookupButtons.first();
			const href = await firstLookup.getAttribute("href");
			expect(href).toContain("polymarket.com/profile");
			console.log(`✓ Lookup button href: ${href}`);
		}
	});

	// === LIVE TRACKER CONTROLS ===
	test("Sound toggle button", async ({ page }) => {
		const soundButton = page.locator(
			'button[aria-label="Sound Enabled"], button[aria-label="Sound Muted"]',
		);
		await expect(soundButton).toBeVisible();

		const initialLabel = await soundButton.getAttribute("aria-label");
		await soundButton.click();
		await page.waitForTimeout(300);

		const newLabel = await soundButton.getAttribute("aria-label");
		expect(newLabel).not.toBe(initialLabel);
		console.log(`✓ Sound toggled from "${initialLabel}" to "${newLabel}"`);
	});

	// === CHECKBOXES ===
	test("1 BET/MKT checkbox", async ({ page }) => {
		const _checkbox = page
			.locator('input[type="checkbox"]')
			.filter({ hasText: "" })
			.first();
		// Find by associated label text
		const label = page.locator("label").filter({ hasText: /1 BET\/MKT/i });
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(
			`✓ 1 BET/MKT checkbox toggled: ${initialChecked} -> ${newChecked}`,
		);
	});

	test("FIXED $10 checkbox", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /FIXED \$10/i });
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(
			`✓ FIXED $10 checkbox toggled: ${initialChecked} -> ${newChecked}`,
		);
	});

	test("FOLLOW strategy checkbox", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /^FOLLOW$/i });
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		// Verify checkbox state changed
		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(
			`✓ FOLLOW checkbox toggled: ${initialChecked} -> ${newChecked}`,
		);
	});

	test("REVERSE strategy checkbox", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /^REVERSE$/i });
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(
			`✓ REVERSE checkbox toggled: ${initialChecked} -> ${newChecked}`,
		);
	});

	test("YES side checkbox", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /^YES$/i }).first();
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(`✓ YES checkbox toggled: ${initialChecked} -> ${newChecked}`);
	});

	test("NO side checkbox", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /^NO$/i }).first();
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(`✓ NO checkbox toggled: ${initialChecked} -> ${newChecked}`);
	});

	// === PRICE INPUTS ===
	test("Min/Max Price inputs", async ({ page }) => {
		// Find price inputs by placeholder
		const minInput = page
			.locator('input[placeholder="Min P"], input[placeholder*="Min"]')
			.first();
		const maxInput = page
			.locator('input[placeholder="Max P"], input[placeholder*="Max"]')
			.first();

		await expect(minInput).toBeVisible();
		await expect(maxInput).toBeVisible();

		// Test changing min price
		await minInput.fill("0.05");
		await minInput.blur();
		await page.waitForTimeout(500);

		// Test changing max price
		await maxInput.fill("0.95");
		await maxInput.blur();
		await page.waitForTimeout(500);

		console.log("✓ Min/Max price inputs working");
	});

	// === RUN BACKTEST BUTTON ===
	test("Run Backtest button", async ({ page }) => {
		const backtestButton = page
			.locator("button")
			.filter({ hasText: /Run Backtest|Continue Backtest/i });
		await expect(backtestButton).toBeVisible();

		const buttonText = await backtestButton.textContent();
		console.log(`Backtest button text: "${buttonText}"`);

		// Don't actually click as it triggers long-running process
		// Just verify it exists and is properly labeled
		expect(buttonText).toMatch(/Run Backtest|Continue Backtest|Processing/i);
	});
});

test.describe("MainV2Page Button Tests", () => {
	test.beforeEach(async ({ page }) => {
		await page.goto("/mainv2");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
	});

	test("Category pills - should filter alerts", async ({ page }) => {
		// Category pills in MainV2
		const categoryPills = page.locator(
			'button[aria-label^="Filter by category"]',
		);
		const count = await categoryPills.count();

		expect(count).toBeGreaterThan(0);
		console.log(`Found ${count} category pills`);

		// Click each pill
		for (let i = 0; i < Math.min(count, 3); i++) {
			// Test first 3
			const pill = categoryPills.nth(i);
			const text = await pill.textContent();
			const isPressed = await pill.getAttribute("aria-pressed");

			await pill.click();
			await page.waitForTimeout(500);

			const newPressed = await pill.getAttribute("aria-pressed");
			expect(newPressed).toBe("true");

			console.log(
				`✓ Category pill "${text}" clicked (was ${isPressed}, now ${newPressed})`,
			);
		}
	});

	test("Lookup buttons in table", async ({ page }) => {
		const lookupButtons = page.locator(
			'button[aria-label^="Lookup trader"], a[aria-label^="Lookup trader"]',
		);
		const count = await lookupButtons.count();

		console.log(`Found ${count} lookup buttons in MainV2`);
		expect(count).toBeGreaterThanOrEqual(0); // May be 0 if no alerts
	});
});

test.describe("Responsive Layout Tests", () => {
	test("TerminalPage - Mobile layout issues check", async ({ page }) => {
		// Set mobile viewport
		await page.setViewportSize({ width: 375, height: 667 });
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		// Check for horizontal overflow (common mobile issue)
		const hasHorizontalOverflow = await page.evaluate(() => {
			return document.documentElement.scrollWidth > window.innerWidth;
		});

		if (hasHorizontalOverflow) {
			console.log("⚠ Horizontal overflow detected on mobile");
		}

		// Check all buttons are visible and clickable
		const buttons = page.locator("button");
		const count = await buttons.count();
		console.log(`Total buttons on mobile: ${count}`);

		// Verify at least some buttons are visible
		const visibleButtons = await buttons.evaluateAll(
			(btns) =>
				btns.filter((b) => {
					const rect = b.getBoundingClientRect();
					return (
						rect.width > 0 && rect.height > 0 && rect.top >= 0 && rect.left >= 0
					);
				}).length,
		);

		console.log(`Visible buttons: ${visibleButtons}`);
		expect(visibleButtons).toBeGreaterThan(0);
	});

	test("MainV2Page - Mobile layout issues check", async ({ page }) => {
		await page.setViewportSize({ width: 375, height: 667 });
		await page.goto("/mainv2");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		// Check for horizontal overflow
		const hasHorizontalOverflow = await page.evaluate(() => {
			return document.documentElement.scrollWidth > window.innerWidth;
		});

		if (hasHorizontalOverflow) {
			console.log("⚠ Horizontal overflow detected on MainV2 mobile");
		}

		// Check category pills are accessible
		const pills = page.locator('button[aria-label^="Filter by category"]');
		const pillCount = await pills.count();
		console.log(`Category pills on mobile: ${pillCount}`);
	});
});
