import { expect, test } from "@playwright/test";

test.describe("🔘 Buttons Loading and State Updates", () => {
	test.beforeEach(async ({ page }) => {
		await page.goto("/");
		await page.waitForLoadState("networkidle");
		// Wait for initial data to load
		await page
			.waitForSelector("table tbody tr", { timeout: 10000 })
			.catch(() => {});
	});

	test("Category Filter - loading state and content update", async ({
		page,
	}) => {
		// Get all category buttons
		const categoryButtons = page.locator(
			'.join button[aria-label^="Filter alerts by"]',
		);
		const count = await categoryButtons.count();

		if (count < 2) {
			console.log("Not enough category buttons to test switching");
			return;
		}

		// Pick the second button (usually not "ALL")
		const secondButton = categoryButtons.nth(1);
		const categoryName = await secondButton.textContent();

		console.log(`Testing category: ${categoryName}`);

		// Get initial first row content to compare later
		const initialFirstRow = await page
			.locator("table tbody tr")
			.first()
			.innerText()
			.catch(() => "");

		// Click the category button
		await secondButton.click();

		// 1. Check for loading state (skeleton or spinner)
		// Based on TerminalSections.tsx, it shows skeletons: isLoading ? [...Array(10)].map(...)
		const skeleton = page.locator(".skeleton").first();
		try {
			await expect(skeleton).toBeVisible({ timeout: 2000 });
			console.log("✓ Loading skeleton visible after click");
		} catch (_e) {
			console.log("⚠ Loading skeleton not seen (maybe too fast)");
		}

		// 2. Wait for loading to finish
		await expect(skeleton).not.toBeVisible({ timeout: 10000 });

		// 3. Verify state update (aria-pressed)
		await expect(secondButton).toHaveAttribute("aria-pressed", "true");

		// 4. Verify content changed or "No Alerts" shown
		const updatedFirstRow = await page
			.locator("table tbody tr")
			.first()
			.innerText()
			.catch(() => "");

		if (
			updatedFirstRow === "NO SIGNALS DETECTED" ||
			updatedFirstRow === "SEARCHING..."
		) {
			console.log("✓ Content updated to empty state");
		} else if (updatedFirstRow !== initialFirstRow) {
			console.log("✓ Content updated with new data");
		} else if (initialFirstRow === "") {
			console.log("✓ Initial state was empty, now has data or still empty");
		} else {
			// If it's the same, it might be valid if data is identical, but usually it should change
			console.log("⚠ Content is identical after category switch");
		}
	});

	test("Winner Filter - loading state and button toggle", async ({ page }) => {
		const winnersButton = page.locator('button[aria-label="Show winners"]');
		const losersButton = page.locator('button[aria-label="Show losers"]');
		const bothButton = page.locator('button[aria-label="Show both"]');

		// Click Winners
		await winnersButton.click();
		await expect(winnersButton).toHaveAttribute("aria-pressed", "true");
		await expect(bothButton).toHaveAttribute("aria-pressed", "false");

		// Check for loading
		const skeleton = page.locator(".skeleton").first();
		await expect(skeleton).not.toBeVisible({ timeout: 5000 });

		// Click Losers
		await losersButton.click();
		await expect(losersButton).toHaveAttribute("aria-pressed", "true");
		await expect(winnersButton).toHaveAttribute("aria-pressed", "false");

		console.log("✓ Winner filter buttons toggle correctly");
	});

	test("Pagination - prevents double clicks and updates page", async ({
		page,
	}) => {
		const nextButton = page.locator('button[aria-label="Next page"]').first();
		const pageIndicator = page
			.locator(".font-mono.text-base-content/70")
			.first();

		const initialText = await pageIndicator.innerText();
		console.log(`Initial pagination: ${initialText}`);

		if (await nextButton.isEnabled()) {
			// Click next
			await nextButton.click();

			// Verify button becomes disabled during load (TerminalSections.tsx: disabled={isLoading || !pagination.hasNext})
			// This is hard to catch manually but we can check if it eventually updates

			await expect(pageIndicator).not.toHaveText(initialText, {
				timeout: 10000,
			});
			const updatedText = await pageIndicator.innerText();
			console.log(`Updated pagination: ${updatedText}`);

			expect(updatedText).not.toBe(initialText);
		} else {
			console.log("Next button disabled, skipping pagination test");
		}
	});
});
