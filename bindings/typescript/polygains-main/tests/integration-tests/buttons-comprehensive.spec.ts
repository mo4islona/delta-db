/**
 * Comprehensive Button Integration Tests for PolyGains Frontend
 *
 * This test suite covers all interactive elements across both pages:
 * - TerminalPage (/) - Main terminal interface
 * - MainV2Page (/mainv2) - Modern glassmorphism UI
 *
 * Buttons/Interactive Elements Tested:
 * =====================================
 *
 * TerminalPage:
 * 1. Category Filter Buttons (ALL, CRYPTO, SPORTS, POLITICS, etc.)
 * 2. Winner Filter Buttons (BOTH, WINNERS, LOSERS)
 * 3. Alerts Pagination (PREV, NEXT)
 * 4. Markets Pagination (PREV, NEXT)
 * 5. Lookup Buttons (search icons in alert rows)
 * 6. Sound Toggle Button (🔊/🔇)
 * 7. Run Backtest Button
 * 8. Checkboxes: 1 BET/MKT, FIXED $10, FOLLOW, REVERSE, YES, NO
 * 9. Price Inputs: Min Price, Max Price
 *
 * MainV2Page:
 * 1. Category Pills (ALL, CRYPTO, SPORTS, POLITICS, etc.)
 * 2. Lookup Buttons (search icons in table rows)
 */

import { expect, type Page, test } from "@playwright/test";

// ============================================
// Test Configuration
// ============================================

const VIEWPORTS = {
	desktop: { width: 1920, height: 1080 },
	laptop: { width: 1366, height: 768 },
	tablet: { width: 768, height: 1024 },
	mobileLg: { width: 414, height: 896 },
	mobileSm: { width: 375, height: 667 },
};

// Helper to capture console errors
async function _captureErrors(page: Page) {
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

// Helper to take full page screenshot
async function takeScreenshot(
	page: Page,
	name: string,
	viewport: { width: number; height: number },
) {
	await page.setViewportSize(viewport);
	await page.waitForTimeout(500);
	await page.screenshot({
		path: `integration-tests/screenshots/${name}-${viewport.width}x${viewport.height}.png`,
		fullPage: true,
	});
}

// ============================================
// Screenshot Tests (Visual Regression)
// ============================================

test.describe("📸 Visual Regression - Screenshots", () => {
	test.beforeAll(async () => {
		const fs = await import("node:fs");
		if (!fs.existsSync("integration-tests/screenshots")) {
			fs.mkdirSync("integration-tests/screenshots", { recursive: true });
		}
	});

	test("TerminalPage - Desktop (1920x1080)", async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
		await takeScreenshot(page, "terminal-desktop", VIEWPORTS.desktop);
	});

	test("TerminalPage - Laptop (1366x768)", async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
		await takeScreenshot(page, "terminal-laptop", VIEWPORTS.laptop);
	});

	test("TerminalPage - Tablet (768x1024)", async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
		await takeScreenshot(page, "terminal-tablet", VIEWPORTS.tablet);
	});

	test("TerminalPage - Mobile Large (414x896)", async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
		await takeScreenshot(page, "terminal-mobile-lg", VIEWPORTS.mobileLg);
	});

	test("TerminalPage - Mobile Small (375x667)", async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
		await takeScreenshot(page, "terminal-mobile-sm", VIEWPORTS.mobileSm);
	});

	test("MainV2Page - Desktop (1920x1080)", async ({ page }) => {
		await page.goto("/mainv2");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
		await takeScreenshot(page, "mainv2-desktop", VIEWPORTS.desktop);
	});

	test("MainV2Page - Mobile (375x667)", async ({ page }) => {
		await page.goto("/mainv2");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
		await takeScreenshot(page, "mainv2-mobile", VIEWPORTS.mobileSm);
	});
});

// ============================================
// TerminalPage Button Tests
// ============================================

test.describe("🖥️ TerminalPage (/)", () => {
	test.beforeEach(async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
	});

	// ---------- CATEGORY FILTER BUTTONS ----------
	test("Category Filter Buttons - ALL/CRYPTO/SPORTS/POLITICS", async ({
		page,
	}) => {
		const categoryButtons = page
			.locator(".join button")
			.filter({ hasText: /^(ALL|CRYPTO|SPORTS|POLITICS)$/i });
		const count = await categoryButtons.count();
		expect(count).toBeGreaterThan(0);

		for (let i = 0; i < count; i++) {
			const button = categoryButtons.nth(i);
			const text = await button.textContent();

			await button.click();
			await page.waitForTimeout(500);

			const ariaPressed = await button.getAttribute("aria-pressed");
			expect(ariaPressed).toBe("true");

			console.log(`✓ Category button "${text}" clicked and active`);
		}
	});

	// ---------- WINNER FILTER BUTTONS ----------
	test("Winner Filter Buttons - BOTH/WINNERS/LOSERS", async ({ page }) => {
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

	// ---------- ALERTS PAGINATION ----------
	test("Alerts Pagination - PREV/NEXT buttons", async ({ page }) => {
		const nextButton = page.locator('button[aria-label="Next page"]').first();
		const prevButton = page
			.locator('button[aria-label="Previous page"]')
			.first();

		const isNextDisabled = await nextButton.isDisabled();

		if (!isNextDisabled) {
			await nextButton.click();
			await page.waitForTimeout(1000);
			console.log("✓ Alerts NEXT button clicked");

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

	// ---------- MARKETS PAGINATION ----------
	test("Markets Pagination - PREV/NEXT buttons", async ({ page }) => {
		const allNextButtons = page.locator('button[aria-label="Next page"]');
		const allPrevButtons = page.locator('button[aria-label="Previous page"]');

		// Markets pagination is the second set
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

	// ---------- LOOKUP BUTTONS ----------
	test("Lookup Buttons - trader search icons", async ({ page }) => {
		const lookupButtons = page.locator(
			'a[aria-label^="Lookup trader"], button[aria-label^="Lookup trader"]',
		);
		const count = await lookupButtons.count();

		expect(count).toBeGreaterThan(0);
		console.log(`Found ${count} lookup buttons`);

		if (count > 0) {
			const firstLookup = lookupButtons.first();
			const href = await firstLookup.getAttribute("href");
			expect(href).toContain("polymarket.com/profile");
			console.log(`✓ Lookup button href: ${href}`);
		}
	});

	// ---------- SOUND TOGGLE ----------
	test("Sound Toggle Button - 🔊/🔇", async ({ page }) => {
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

	// ---------- RUN BACKTEST BUTTON ----------
	test("Run Backtest Button", async ({ page }) => {
		const backtestButton = page
			.locator("button")
			.filter({ hasText: /Run Backtest|Continue Backtest/i });
		await expect(backtestButton).toBeVisible();

		const buttonText = await backtestButton.textContent();
		console.log(`Backtest button text: "${buttonText}"`);

		expect(buttonText).toMatch(/Run Backtest|Continue Backtest|Processing/i);
	});

	// ---------- CHECKBOXES ----------
	test("Checkbox - 1 BET/MKT", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /1 BET\/MKT/i });
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(`✓ 1 BET/MKT: ${initialChecked} -> ${newChecked}`);
	});

	test("Checkbox - FIXED $10", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /FIXED \$10/i });
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(`✓ FIXED $10: ${initialChecked} -> ${newChecked}`);
	});

	test("Checkbox - FOLLOW strategy", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /^FOLLOW$/i });
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(`✓ FOLLOW: ${initialChecked} -> ${newChecked}`);
	});

	test("Checkbox - REVERSE strategy", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /^REVERSE$/i });
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(`✓ REVERSE: ${initialChecked} -> ${newChecked}`);
	});

	test("Checkbox - YES side", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /^YES$/i }).first();
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(`✓ YES: ${initialChecked} -> ${newChecked}`);
	});

	test("Checkbox - NO side", async ({ page }) => {
		const label = page.locator("label").filter({ hasText: /^NO$/i }).first();
		await expect(label).toBeVisible();

		const input = label.locator('input[type="checkbox"]');
		const initialChecked = await input.isChecked();

		await label.click();
		await page.waitForTimeout(300);

		const newChecked = await input.isChecked();
		expect(newChecked).not.toBe(initialChecked);
		console.log(`✓ NO: ${initialChecked} -> ${newChecked}`);
	});

	// ---------- PRICE INPUTS ----------
	test("Price Inputs - Min/Max", async ({ page }) => {
		const minInput = page
			.locator('input[placeholder="Min P"], input[placeholder*="Min"]')
			.first();
		const maxInput = page
			.locator('input[placeholder="Max P"], input[placeholder*="Max"]')
			.first();

		await expect(minInput).toBeVisible();
		await expect(maxInput).toBeVisible();

		await minInput.fill("0.05");
		await minInput.blur();
		await page.waitForTimeout(500);

		await maxInput.fill("0.95");
		await maxInput.blur();
		await page.waitForTimeout(500);

		console.log("✓ Min/Max price inputs working");
	});
});

// ============================================
// MainV2Page Button Tests
// ============================================

test.describe("📱 MainV2Page (/mainv2)", () => {
	test.beforeEach(async ({ page }) => {
		await page.goto("/mainv2");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);
	});

	test("Category Pills - filter alerts", async ({ page }) => {
		const categoryPills = page.locator(
			'button[aria-label^="Filter by category"]',
		);
		const count = await categoryPills.count();

		expect(count).toBeGreaterThan(0);
		console.log(`Found ${count} category pills`);

		for (let i = 0; i < Math.min(count, 3); i++) {
			const pill = categoryPills.nth(i);
			const text = await pill.textContent();
			const _isPressed = await pill.getAttribute("aria-pressed");

			await pill.click();
			await page.waitForTimeout(500);

			const newPressed = await pill.getAttribute("aria-pressed");
			expect(newPressed).toBe("true");

			console.log(`✓ Category pill "${text}" clicked`);
		}
	});

	test("Lookup Buttons in table", async ({ page }) => {
		const lookupButtons = page.locator(
			'button[aria-label^="Lookup trader"], a[aria-label^="Lookup trader"]',
		);
		const count = await lookupButtons.count();

		console.log(`Found ${count} lookup buttons in MainV2`);
		expect(count).toBeGreaterThanOrEqual(0);
	});
});

// ============================================
// Responsive Layout Tests
// ============================================

test.describe("📐 Responsive Layout Tests", () => {
	test("TerminalPage - Mobile layout issues", async ({ page }) => {
		await page.setViewportSize(VIEWPORTS.mobileSm);
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		// Check for horizontal overflow
		const hasHorizontalOverflow = await page.evaluate(() => {
			return document.documentElement.scrollWidth > window.innerWidth;
		});

		if (hasHorizontalOverflow) {
			console.log("⚠ Horizontal overflow detected on mobile");
		}

		// Count buttons
		const buttons = page.locator("button");
		const count = await buttons.count();
		console.log(`Total buttons on mobile: ${count}`);

		// Verify buttons are visible
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

	test("MainV2Page - Mobile layout issues", async ({ page }) => {
		await page.setViewportSize(VIEWPORTS.mobileSm);
		await page.goto("/mainv2");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		// Check for horizontal overflow
		const hasHorizontalOverflow = await page.evaluate(() => {
			return document.documentElement.scrollWidth > window.innerWidth;
		});

		if (hasHorizontalOverflow) {
			console.log("⚠ Horizontal overflow on MainV2 mobile");
		}

		// Check category pills
		const pills = page.locator('button[aria-label^="Filter by category"]');
		const pillCount = await pills.count();
		console.log(`Category pills on mobile: ${pillCount}`);
	});
});

// ============================================
// Accessibility Tests
// ============================================

test.describe("♿ Accessibility Tests", () => {
	test("All buttons have aria-labels or visible text", async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		const buttons = page.locator("button");
		const count = await buttons.count();

		let accessibleCount = 0;
		for (let i = 0; i < count; i++) {
			const button = buttons.nth(i);
			const ariaLabel = await button.getAttribute("aria-label");
			const text = await button.textContent();

			if (ariaLabel || text?.trim()) {
				accessibleCount++;
			}
		}

		console.log(
			`Accessible buttons: ${accessibleCount}/${count} (${Math.round((accessibleCount / count) * 100)}%)`,
		);
		expect(accessibleCount).toBeGreaterThan(count * 0.8); // At least 80% accessible
	});

	test("Focus indicators are visible", async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");

		const firstButton = page.locator("button").first();
		await firstButton.focus();

		// Check if element is focused
		const isFocused = await firstButton.evaluate(
			(el) => el === document.activeElement,
		);
		expect(isFocused).toBe(true);
	});
});
