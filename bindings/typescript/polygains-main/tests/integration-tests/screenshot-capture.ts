/**
 * Screenshot Capture Script using Puppeteer
 * Captures full-page screenshots of PolyGains frontend at different viewports
 */

import fs from "node:fs";
import path from "node:path";
import puppeteer from "puppeteer";

const FRONTEND_URL = process.env.FRONTEND_URL || "http://localhost:4033";
const OUTPUT_DIR = "integration-tests/screenshots";

const VIEWPORTS = [
	{ name: "desktop", width: 1920, height: 1080 },
	{ name: "laptop", width: 1366, height: 768 },
	{ name: "tablet", width: 768, height: 1024 },
	{ name: "mobile-lg", width: 414, height: 896 },
	{ name: "mobile-sm", width: 375, height: 667 },
];

const PAGES = [
	{ path: "/", name: "terminal" },
	{ path: "/mainv2", name: "mainv2" },
];

async function captureScreenshots() {
	// Ensure output directory exists
	if (!fs.existsSync(OUTPUT_DIR)) {
		fs.mkdirSync(OUTPUT_DIR, { recursive: true });
	}

	console.log("🚀 Launching browser...");
	const browser = await puppeteer.launch({
		headless: true,
		args: ["--no-sandbox", "--disable-setuid-sandbox"],
		executablePath:
			"/home/franz/.cache/puppeteer/chromium/linux-1083080/chrome-linux/chrome",
	});

	const results: {
		page: string;
		viewport: string;
		file: string;
		success: boolean;
		errors?: string[];
	}[] = [];

	try {
		for (const pageConfig of PAGES) {
			console.log(
				`\n📄 Capturing page: ${pageConfig.name} (${pageConfig.path})`,
			);

			for (const viewport of VIEWPORTS) {
				const context = await browser.createBrowserContext();
				const page = await context.newPage();

				// Collect console errors
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

				try {
					// Set viewport
					await page.setViewport({
						width: viewport.width,
						height: viewport.height,
						deviceScaleFactor: 1,
					});

					// Navigate to page
					const url = `${FRONTEND_URL}${pageConfig.path}`;
					console.log(
						`  📐 Viewport: ${viewport.name} (${viewport.width}x${viewport.height})`,
					);
					console.log(`     URL: ${url}`);

					await page.goto(url, {
						waitUntil: "networkidle2",
						timeout: 30000,
					});

					// Wait for content to load
					await page.waitForTimeout(3000);

					// Check for horizontal overflow
					const hasOverflow = await page.evaluate(() => {
						return document.documentElement.scrollWidth > window.innerWidth;
					});

					if (hasOverflow) {
						console.log(`     ⚠️  Horizontal overflow detected!`);
					}

					// Count buttons
					const buttonCount = await page.evaluate(() => {
						return document.querySelectorAll("button").length;
					});
					console.log(`     🔘 Buttons found: ${buttonCount}`);

					// Capture screenshot
					const filename = `${pageConfig.name}-${viewport.name}-${viewport.width}x${viewport.height}.png`;
					const filepath = path.join(OUTPUT_DIR, filename);

					await page.screenshot({
						path: filepath,
						fullPage: true,
					});

					console.log(`     ✅ Screenshot saved: ${filename}`);

					// Log any errors
					const allErrors = [...consoleErrors, ...pageErrors];
					if (allErrors.length > 0) {
						console.log(`     ⚠️  Errors: ${allErrors.length}`);
						for (const err of allErrors.slice(0, 3)) {
							console.log(`        - ${err.substring(0, 100)}`);
						}
					}

					results.push({
						page: pageConfig.name,
						viewport: viewport.name,
						file: filename,
						success: true,
						errors: allErrors,
					});
				} catch (error) {
					console.log(
						`     ❌ Failed: ${error instanceof Error ? error.message : String(error)}`,
					);
					results.push({
						page: pageConfig.name,
						viewport: viewport.name,
						file: "",
						success: false,
						errors: [error instanceof Error ? error.message : String(error)],
					});
				} finally {
					await context.close();
				}
			}
		}
	} finally {
		await browser.close();
	}

	// Print summary
	console.log(`\n${"=".repeat(60)}`);
	console.log("📊 SCREENSHOT CAPTURE SUMMARY");
	console.log("=".repeat(60));

	const successCount = results.filter((r) => r.success).length;
	const failCount = results.filter((r) => !r.success).length;

	console.log(`\n✅ Successful: ${successCount}/${results.length}`);
	console.log(`❌ Failed: ${failCount}/${results.length}`);

	if (failCount > 0) {
		console.log("\nFailed captures:");
		results
			.filter((r) => !r.success)
			.forEach((r) => {
				console.log(`  - ${r.page} / ${r.viewport}`);
			});
	}

	// Save report
	const reportPath = path.join(OUTPUT_DIR, "capture-report.json");
	fs.writeFileSync(reportPath, JSON.stringify(results, null, 2));
	console.log(`\n📝 Report saved: ${reportPath}`);

	// List all screenshot files
	console.log("\n📁 Screenshot files:");
	const files = fs.readdirSync(OUTPUT_DIR).filter((f) => f.endsWith(".png"));
	files.forEach((f) => {
		const stats = fs.statSync(path.join(OUTPUT_DIR, f));
		console.log(`  - ${f} (${(stats.size / 1024).toFixed(1)} KB)`);
	});

	return results;
}

// Run the capture
captureScreenshots()
	.then(() => {
		console.log("\n✨ Done!");
		process.exit(0);
	})
	.catch((error) => {
		console.error("\n💥 Fatal error:", error);
		process.exit(1);
	});
