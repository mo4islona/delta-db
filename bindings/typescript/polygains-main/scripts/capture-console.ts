#!/usr/bin/env bun
/**
 * Capture browser console logs from a URL
 * Usage: bun scripts/capture-console.ts [url]
 */

const url = process.argv[2] || "https://polygains.com/";

console.log(`🔍 Capturing console logs from: ${url}\n`);

// Create a simple HTML page that loads the target in an iframe and captures console
const _html = `
<!DOCTYPE html>
<html>
<head>
  <title>Console Capture</title>
</head>
<body>
  <h1>Console Capture for ${url}</h1>
  <div id="logs"></div>
  <iframe id="target" src="${url}" width="100%" height="600"></iframe>
  <script>
    const logs = [];
    const originalLog = console.log;
    const originalError = console.error;
    const originalWarn = console.warn;
    
    console.log = (...args) => {
      logs.push({type: 'log', data: args.join(' ')});
      originalLog.apply(console, args);
    };
    
    console.error = (...args) => {
      logs.push({type: 'error', data: args.join(' ')});
      originalError.apply(console, args);
    };
    
    console.warn = (...args) => {
      logs.push({type: 'warn', data: args.join(' ')});
      originalWarn.apply(console, args);
    };
    
    window.onerror = (msg, url, line, col, error) => {
      logs.push({type: 'pageerror', data: msg});
    };
    
    // Export logs for access
    window.getLogs = () => logs;
  </script>
</body>
</html>
`;

// For now, just fetch the page and analyze it
console.log("Fetching page content...\n");

try {
	const response = await fetch(url);
	const text = await response.text();

	// Extract script tags
	const scriptRegex = /<script[^>]*>([\s\S]*?)<\/script>/gi;
	const scripts = [];
	let match: RegExpExecArray | null = null;

	while (true) {
		match = scriptRegex.exec(text);
		if (match === null) break;
		const src = match[0].match(/src=["']([^"']+)["']/);
		const content = match[1]?.trim();

		if (src) {
			scripts.push({ type: "external", src: src[1] });
		} else if (content) {
			scripts.push({
				type: "inline",
				content:
					content.substring(0, 200) + (content.length > 200 ? "..." : ""),
			});
		}
	}

	console.log(`Found ${scripts.length} script tags:\n`);
	scripts.forEach((script, i) => {
		if (script.type === "external") {
			console.log(`  [${i + 1}] External: ${script.src}`);
		} else {
			console.log(`  [${i + 1}] Inline: ${script.content}`);
		}
	});

	// Check for common error patterns in the HTML
	console.log("\n\n📋 Page Analysis:");
	console.log(`  - Content length: ${text.length} bytes`);
	console.log(
		`  - Has React root: ${text.includes('id="root"') || text.includes('id="__next"')}`,
	);
	console.log(
		`  - Has error boundaries: ${text.includes("error") || text.includes("Error")}`,
	);
} catch (error) {
	console.error("Failed to fetch:", error);
}

console.log(
	"\n\nNote: Full browser console capture requires a headless browser.",
);
console.log(
	"The integration test has been updated to capture console when Playwright is available.",
);
