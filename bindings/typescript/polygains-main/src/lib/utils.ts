import { UPSERT_CHUNK_SIZE, USDC_DENOMINATOR } from "@/lib/const";

export const parseOptionalString = (
	value: string | null,
): string | undefined => {
	if (value === null) return undefined;
	const normalized = value.trim();
	return normalized.length > 0 ? normalized : undefined;
};

export const parsePositiveInt = (
	value: string | null,
	fallback: number,
	max = Number.MAX_SAFE_INTEGER,
): number => {
	if (!value) return fallback;
	const parsed = Number.parseInt(value, 10);
	if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
	return Math.min(parsed, max);
};

export const parseOptionalBoolean = (
	value: string | null,
): boolean | undefined => {
	if (value === null) return undefined;
	const normalized = value.trim().toLowerCase();
	if (normalized === "true" || normalized === "1" || normalized === "yes") {
		return true;
	}
	if (normalized === "false" || normalized === "0" || normalized === "no") {
		return false;
	}
	return undefined;
};

export const readEnv = (...keys: string[]): string => {
	for (const key of keys) {
		const value = process.env[key]?.trim();
		if (value) return value;
	}
	throw new Error(`[HTTP] Missing required env var: ${keys.join(" or ")}`);
};

export const readPort = (...keys: string[]): number => {
	const raw = readEnv(...keys);
	const port = Number.parseInt(raw, 10);
	if (!Number.isFinite(port) || port <= 0 || port > 65535) {
		throw new Error(`[HTTP] Invalid port "${raw}" from ${keys.join(" or ")}`);
	}
	return port;
};

export const normalizeInt32 = (value: unknown): number | null => {
	if (typeof value !== "number" || !Number.isFinite(value)) return null;
	if (!Number.isInteger(value)) return null;
	return value | 0;
};

export const chunk = <T>(items: T[], size = UPSERT_CHUNK_SIZE): T[][] => {
	if (items.length === 0) return [];
	const out: T[][] = [];
	for (let i = 0; i < items.length; i += size) {
		out.push(items.slice(i, i + size));
	}
	return out;
};

export const toBigInt = (v: unknown): bigint => {
	if (typeof v === "bigint") return v;
	if (typeof v === "number") return BigInt(Math.floor(v));
	if (typeof v === "string") return BigInt(v);
	return 0n;
};

export const toTokenId = (value: string | number | bigint) =>
	typeof value === "bigint" ? value.toString() : String(value);

export const toUsdVolume = (usdc: bigint): number =>
	Number(usdc) / Number(USDC_DENOMINATOR);
