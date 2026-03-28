import path from "node:path";
import { chromium, devices } from "@playwright/test";

async function takeIphoneScreenshot() {
	const iPhone = devices["iPhone 13"];
	const browser = await chromium.launch();
	const context = await browser.newContext({
		...iPhone,
	});
	const page = await context.newPage();

	const url = "http://localhost:4033/";
	console.log(`Navigating to ${url}...`);

	try {
		await page.goto(url, { waitUntil: "networkidle" });

		// Wait a bit for any animations or lazy loading
		await page.waitForTimeout(5000);

		const outputPath = path.join(
			process.cwd(),
			"integration-tests",
			"screenshots",
			"iphone-full-height.png",
		);

		await page.screenshot({
			path: outputPath,
			fullPage: true,
		});

		console.log(`Screenshot saved to: ${outputPath}`);
	} catch (error) {
		console.error("Error taking screenshot:", error);
	} finally {
		await browser.close();
	}
}

takeIphoneScreenshot();
