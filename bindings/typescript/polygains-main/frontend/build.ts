#!/usr/bin/env bun
import { existsSync } from "node:fs";
import { rm } from "node:fs/promises";
import path from "node:path";
import plugin from "bun-plugin-tailwind";

if (process.argv.includes("--help") || process.argv.includes("-h")) {
	console.log(`
🏗️  Bun Build Script

Usage: bun run build.ts [options]

Common Options:
  --outdir <path>          Output directory (default: "../public/dist")
  --minify                 Enable minification (or --minify.whitespace, --minify.syntax, etc)
  --sourcemap <type>       Sourcemap type: none|linked|inline|external
  --target <target>        Build target: browser|bun|node
  --format <format>        Output format: esm|cjs|iife
  --splitting              Enable code splitting
  --packages <type>        Package handling: bundle|external
  --public-path <path>     Public path for assets
  --env <mode>             Environment handling: inline|disable|prefix*
  --conditions <list>      Package.json export conditions (comma separated)
  --external <list>        External packages (comma separated)
  --banner <text>          Add banner text to output
  --footer <text>          Add footer text to output
  --define <obj>           Define global constants (e.g. --define.VERSION=1.0.0)
  --help, -h               Show this help message

Example:
  bun run build.ts --minify --sourcemap=linked
`);
	process.exit(0);
}

const toCamelCase = (str: string): string =>
	str.replace(/-([a-z])/g, (_, letter: string) => letter.toUpperCase());

const parseValue = (value: string): string | boolean | number | string[] => {
	if (value === "true") return true;
	if (value === "false") return false;
	if (/^\d+$/.test(value)) return parseInt(value, 10);
	if (/^\d*\.\d+$/.test(value)) return parseFloat(value);
	if (value.includes(",")) return value.split(",").map((v) => v.trim());
	return value;
};

function parseArgs(): Partial<Bun.BuildConfig> {
	const config: Record<string, unknown> = {};
	const args = process.argv.slice(2);

	for (let i = 0; i < args.length; i++) {
		const arg = args[i];
		if (arg === undefined) continue;
		if (!arg.startsWith("--")) continue;

		if (arg.startsWith("--no-")) {
			const key = toCamelCase(arg.slice(5));
			config[key] = false;
			continue;
		}

		if (
			!arg.includes("=") &&
			(i === args.length - 1 || args[i + 1]?.startsWith("--"))
		) {
			const key = toCamelCase(arg.slice(2));
			config[key] = true;
			continue;
		}

		let key: string;
		let value: string;

		if (arg.includes("=")) {
			[key, value] = arg.slice(2).split("=", 2) as [string, string];
		} else {
			key = arg.slice(2);
			value = args[++i] ?? "";
		}

		key = toCamelCase(key);

		if (key.includes(".")) {
			const parts = key.split(".");
			if (parts.length > 2) {
				console.warn(
					`Warning: Deeply nested option "${key}" is not supported. Only single-level nesting (e.g., --minify.whitespace) is allowed.`,
				);
				continue;
			}
			const parentKey = parts[0];
			const childKey = parts[1];
			if (!parentKey || !childKey) continue;
			const existing = config[parentKey];
			if (
				typeof existing !== "object" ||
				existing === null ||
				Array.isArray(existing)
			) {
				config[parentKey] = {};
			}
			(config[parentKey] as Record<string, unknown>)[childKey] =
				parseValue(value);
		} else {
			config[key] = parseValue(value);
		}
	}

	return config as Partial<Bun.BuildConfig>;
}

const formatFileSize = (bytes: number): string => {
	const units = ["B", "KB", "MB", "GB"];
	let size = bytes;
	let unitIndex = 0;

	while (size >= 1024 && unitIndex < units.length - 1) {
		size /= 1024;
		unitIndex++;
	}

	return `${size.toFixed(2)} ${units[unitIndex]}`;
};

console.log("\n🚀 Starting build process...\n");

const cliConfig = parseArgs();

// Preserved logic: Default to ../public/dist
const outdir =
	cliConfig.outdir || path.join(process.cwd(), "..", "public", "dist");

if (existsSync(outdir)) {
	console.log(`🗑️  Cleaning previous build at ${outdir}`);
	await rm(outdir, { recursive: true, force: true });
}

const start = performance.now();

const entrypoints = [...new Bun.Glob("**.html").scanSync("src")]
	.map((a) => path.resolve("src", a))
	.filter((dir) => !dir.includes("node_modules"));

console.log(
	`📄 Found ${entrypoints.length} HTML ${entrypoints.length === 1 ? "file" : "files"} to process\n`,
);

const result = await Bun.build({
	entrypoints,
	outdir,
	plugins: [plugin],
	minify: true,
	target: "browser",
	sourcemap: "linked",
	alias: {
		react: "preact/compat",
		"react-dom/client": "preact/compat",
		"react-dom": "preact/compat",
		"react/jsx-runtime": "preact/jsx-runtime",
	},
	define: {
		"process.env.NODE_ENV": JSON.stringify(process.env.NODE_ENV || "production"),
		"process.env.BUN_PUBLIC_API_BASE_URL": JSON.stringify(
			process.env.BUN_PUBLIC_API_BASE_URL || "https://api.polygains.com",
		),
	},
	...cliConfig,
});

// Preserved logic: Copy _redirects file for Cloudflare Pages SPA routing
const redirectsSrc = path.resolve("src", "_redirects");
if (existsSync(redirectsSrc)) {
	await Bun.write(path.join(outdir, "_redirects"), Bun.file(redirectsSrc));
	console.log("📝 Copied _redirects for Cloudflare Pages\n");
}

// Copy static assets from public/ to dist/
const publicDir = path.resolve("public");
if (existsSync(publicDir)) {
	const publicAssets = [...new Bun.Glob("**/*").scanSync(publicDir)];
	for (const asset of publicAssets) {
		const srcPath = path.join(publicDir, asset);
		const destPath = path.join(outdir, asset);
		await Bun.write(destPath, Bun.file(srcPath));
	}
	if (publicAssets.length > 0) {
		console.log(`📦 Copied ${publicAssets.length} static asset(s) from public/\n`);
	}
}

const end = performance.now();

const outputTable = result.outputs.map((output) => ({
	File: path.relative(process.cwd(), output.path),
	Type: output.kind,
	Size: formatFileSize(output.size),
}));

console.table(outputTable);
const buildTime = (end - start).toFixed(2);

console.log(`\n✅ Build completed in ${buildTime}ms\n`);
