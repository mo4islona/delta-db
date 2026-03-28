/**
 * Terminal utility functions for formatting, ASCII art, and market calculations
 */

export const TOP_LOGO_ASCII = ` 
██████╗  ██████╗ ██╗     ██╗   ██╗
██╔══██╗██╔═══██╗██║     ╚██╗ ██╔╝
██████╔╝██║   ██║██║      ╚████╔╝
██╔═══╝ ██║   ██║██║       ╚██╔╝
██║     ╚██████╔╝███████╗   ██║
╚═╝      ╚═════╝ ╚══════╝   ╚═╝
 ██████╗  █████╗  ██╗███╗   ██╗███████╗
██╔════╝ ██╔══██╗ ██║████╗  ██║██╔════╝
██║  ███╗███████║ ██║██╔██╗ ██║███████╗
██║   ██║██╔══██║ ██║██║╚██╗██║╚════██║
╚██████╔╝██║  ██║ ██║██║ ╚████║███████║
 ╚═════╝ ╚═╝  ╚═╝ ╚═╝╚═╝  ╚═══╝╚══════╝`;

export const BANNER_ASCII = `
██████╗  ██████╗ ██╗  ██╗   ██╗ ██████╗  █████╗  ██╗███╗   ██╗███████╗
██╔══██╗██╔═══██╗██║  ╚██╗ ██╔╝██╔════╝ ██╔══██╗ ██║████╗  ██║██╔════╝
██████╔╝██║   ██║██║   ╚████╔╝ ██║  ███╗███████║ ██║██╔██╗ ██║███████╗
██╔═══╝ ██║   ██║██║    ╚██╔╝  ██║   ██║██╔══██║ ██║██║╚██╗██║╚════██║
██║     ╚██████╔╝███████╗██║   ╚██████╔╝██║  ██║ ██║██║ ╚████║███████║
╚═╝      ╚═════╝ ╚══════╝╚═╝    ╚═════╝ ╚═╝  ╚═╝ ╚═╝╚═╝  ╚═══╝╚══════╝`;

export const NO_ALERTS_ASCII = `
    .   .      .
   ... ...    ...
  .......  .......
 ................
.....................
 ................
  .......  .......
   ... ...    ...
    .   .      .
`;

/**
 * Format a timestamp as relative time (e.g., "5m ago")
 */
export function timeAgo(alertTime: number): string {
	const then = alertTime * 1000;
	const now = Date.now();
	const s = Math.max(1, Math.floor((now - then) / 1000));
	const m = Math.floor(s / 60);
	const h = Math.floor(m / 60);
	const d = Math.floor(h / 24);
	if (d >= 1) return `${d}d ago`;
	if (h >= 1) return `${h}h ago`;
	if (m >= 1) return `${m}m ago`;
	return `${s}s ago`;
}

/**
 * Format a number as USD currency with no decimal places
 */
export function formatMoney(n: number): string {
	return n.toLocaleString(undefined, {
		style: "currency",
		currency: "USD",
		maximumFractionDigits: 0,
	});
}

/**
 * Format a number as a price with 2 decimal places
 */
export function formatPrice(n: number): string {
	return n.toLocaleString(undefined, {
		style: "currency",
		currency: "USD",
		minimumFractionDigits: 2,
		maximumFractionDigits: 2,
	});
}

/**
 * Format a large number with 2 decimal places
 */
export function formatLargeNumber(value: number): string {
	return value.toLocaleString(undefined, {
		minimumFractionDigits: 2,
		maximumFractionDigits: 2,
	});
}

/**
 * Format market price as percentage, handling resolved markets
 */
export function renderMarketPrice(
	lastPrice: number,
	isClosed: boolean,
): string {
	const clamped = Math.max(
		0,
		Math.min(1, Number.isFinite(lastPrice) ? lastPrice : 0),
	);
	const pct = `${(clamped * 100).toFixed(2)}%`;
	if (isClosed && (clamped >= 0.99 || clamped <= 0.01)) {
		return `RESOLVED ${pct}`;
	}
	return pct;
}

/**
 * Get outcome metadata for styling
 */
export function getOutcomeMeta(outcome: string | number): {
	label: string;
	toneClass: string;
} {
	const text = String(outcome).toUpperCase();
	if (text === "YES" || text === "1")
		return {
			label: "YES",
			toneClass: "bg-success/20 text-success border-success/20",
		};
	if (text === "NO" || text === "0")
		return { label: "NO", toneClass: "bg-error/20 text-error border-error/20" };
	return {
		label: text,
		toneClass: "bg-base-content/20 text-base-content border-base-content/20",
	};
}

/**
 * Check if outcome has all market stats available
 */
export function hasAllStats(outcome: {
	mean?: number | null;
	stdDev?: number | null;
	p95?: number | null;
}): boolean {
	return (
		outcome.mean !== null &&
		outcome.mean !== undefined &&
		outcome.stdDev !== null &&
		outcome.stdDev !== undefined &&
		outcome.p95 !== null &&
		outcome.p95 !== undefined
	);
}

/**
 * Format a market stat value
 */
export function formatMarketStat(value: number | null | undefined): string {
	const num = Number(value);
	return Number.isFinite(num) ? `$${num.toFixed(2)}` : "--";
}
