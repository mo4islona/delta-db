import fs from "node:fs";
import path from "node:path";
import { chromium } from "@playwright/test";

async function takePixel8Screenshot() {
	const pixel8 = {
		name: "Pixel 8",
		userAgent:
			"Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.164 Mobile Safari/537.36",
		viewport: {
			width: 412,
			height: 915,
		},
		deviceScaleFactor: 2.625,
		isMobile: true,
		hasTouch: true,
		defaultBrowserType: "chromium",
	};

	const OUTPUT_DIR = path.join(
		process.cwd(),
		"integration-tests",
		"screenshots",
	);
	if (!fs.existsSync(OUTPUT_DIR)) {
		fs.mkdirSync(OUTPUT_DIR, { recursive: true });
	}

	const browser = await chromium.launch();
	const context = await browser.newContext({
		...pixel8,
	});
	const page = await context.newPage();

	const url = "http://localhost:4033/";
	console.log(`Navigating to ${url} on Pixel 8...`);

	try {
		await page.goto(url, { waitUntil: "networkidle" });
		await page.waitForTimeout(8000); // Wait for animations

		const outputPath = path.join(OUTPUT_DIR, "pixel8-full-height.png");

		await page.screenshot({
			path: outputPath,
			fullPage: true,
		});

		console.log(`Pixel 8 screenshot saved to: ${outputPath}`);

		// Also check for overlapping elements using JS
		const overlaps = await page.evaluate(() => {
			const elements = Array.from(
				document.querySelectorAll("h1, h2, h3, div, span, button, th, td"),
			);
			const results: string[] = [];

			for (let i = 0; i < elements.length; i++) {
				for (let j = i + 1; j < elements.length; j++) {
					const r1 = elements[i].getBoundingClientRect();
					const r2 = elements[j].getBoundingClientRect();

					if (
						r1.width === 0 ||
						r1.height === 0 ||
						r2.width === 0 ||
						r2.height === 0
					)
						continue;

					// Simple overlap check
					const isOverlapping = !(
						r2.left >= r1.right ||
						r2.right <= r1.left ||
						r2.top >= r1.bottom ||
						r2.bottom <= r1.top
					);

					if (isOverlapping) {
						const text1 = elements[i].textContent?.trim().substring(0, 20);
						const text2 = elements[j].textContent?.trim().substring(0, 20);
						if (text1 && text2 && text1 !== text2) {
							// Filter out parent-child relationships which naturally overlap
							if (
								!elements[i].contains(elements[j]) &&
								!elements[j].contains(elements[i])
							) {
								results.push(`Overlap: "${text1}" and "${text2}"`);
							}
						}
					}
				}
			}
			return results.slice(0, 10); // Return first 10 for analysis
		});

		if (overlaps.length > 0) {
			console.log("Detected potential text overlaps:");
			for (const o of overlaps) {
				console.log(` - ${o}`);
			}
		}
	} catch (error) {
		console.error("Error taking screenshot:", error);
	} finally {
		await browser.close();
	}
}

takePixel8Screenshot();
