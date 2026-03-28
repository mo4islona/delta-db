import { expect, test } from "@playwright/test";

test.describe("Console Error Tests", () => {
	test("should not have dateFormatter ReferenceError", async ({ page }) => {
		const consoleErrors: string[] = [];
		const pageErrors: string[] = [];

		// Capture console errors
		page.on("console", (msg) => {
			if (msg.type() === "error") {
				consoleErrors.push(msg.text());
			}
		});

		// Capture page errors
		page.on("pageerror", (error) => {
			pageErrors.push(error.message);
		});

		// Navigate to the page
		await page.goto("/");
		await page.waitForLoadState("networkidle");

		// Wait a bit for any async scripts to run
		await page.waitForTimeout(2000);

		// Check for dateFormatter error specifically
		const allErrors = [...consoleErrors, ...pageErrors];
		const dateFormatterErrors = allErrors.filter((e) =>
			e.includes("dateFormatter"),
		);

		if (dateFormatterErrors.length > 0) {
			console.log("dateFormatter errors found:", dateFormatterErrors);
		}

		expect(dateFormatterErrors).toHaveLength(0);
	});

	test("should not have any ReferenceError", async ({ page }) => {
		const referenceErrors: string[] = [];

		page.on("pageerror", (error) => {
			if (error.message.includes("ReferenceError")) {
				referenceErrors.push(error.message);
			}
		});

		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(2000);

		expect(referenceErrors).toHaveLength(0);
	});

	test("should capture all console messages for debugging", async ({
		page,
	}) => {
		const allConsoleMessages: { type: string; text: string }[] = [];

		page.on("console", (msg) => {
			allConsoleMessages.push({
				type: msg.type(),
				text: msg.text(),
			});
		});

		await page.goto("/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(2000);

		// Log all messages for debugging
		console.log("Console messages:", allConsoleMessages);

		// The test passes - this is for diagnostic purposes
		expect(true).toBe(true);
	});

	test("should read console from production https://polygains.com/", async ({
		page,
	}) => {
		const consoleLogs: { type: string; text: string }[] = [];
		const consoleErrors: string[] = [];
		const pageErrors: string[] = [];

		// Capture all console messages
		page.on("console", (msg) => {
			const logEntry = {
				type: msg.type(),
				text: msg.text(),
			};
			consoleLogs.push(logEntry);

			// Also capture errors separately
			if (msg.type() === "error") {
				consoleErrors.push(msg.text());
			}
		});

		// Capture page errors
		page.on("pageerror", (error) => {
			pageErrors.push(error.message);
		});

		// Navigate to production site
		await page.goto("https://polygains.com/");
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(3000);

		// Print out all console messages
		console.log("\n=== Console Logs from https://polygains.com/ ===");
		if (consoleLogs.length === 0) {
			console.log("(No console messages captured)");
		} else {
			consoleLogs.forEach((log, index) => {
				console.log(`[${index + 1}] [${log.type}] ${log.text}`);
			});
		}
		console.log("=== End Console Logs ===\n");

		// Print errors if any
		if (consoleErrors.length > 0) {
			console.log("\n⚠️ Console Errors found:");
			for (const err of consoleErrors) {
				console.log(`  - ${err}`);
			}
		}

		if (pageErrors.length > 0) {
			console.log("\n⚠️ Page Errors found:");
			for (const err of pageErrors) {
				console.log(`  - ${err}`);
			}
		}

		// The test passes - this is for diagnostic purposes
		expect(true).toBe(true);
	});
});
