"use client";
import useSWR from "swr";
import { fetchCategoriesWithCounts, type CategoryCount } from "@/api/terminalApi";
import { swrKeys } from "@/hooks/swr/keys";
import { SWR_REFRESH } from "@/hooks/swr/options";

interface UseCategoriesOptions {
	refreshInterval?: number;
	enabled?: boolean;
}

export interface CategoryOption {
	name: string;
	count: number;
	enabled: boolean;
	displayName: string;
}

// Priority categories in display order with their display names
const PRIORITY_CATEGORIES: Record<string, string> = {
	Politics: "Politics",
	Sports: "Sports",
	Crypto: "Crypto",
	Finance: "Finance",
	Geopolitics: "Geopolitics",
	Earnings: "Earnings",
	Tech: "Tech",
	Culture: "Culture",
	World: "World",
	Economy: "Economy",
};

// Default categories that always show - all enabled so users can filter by them
const DEFAULT_CATEGORIES: CategoryOption[] = [
	{ name: "ALL", count: 0, enabled: true, displayName: "ALL" },
	{ name: "CRYPTO", count: 0, enabled: true, displayName: "Crypto" },
	{ name: "SPORTS", count: 0, enabled: true, displayName: "Sports" },
	{ name: "POLITICS", count: 0, enabled: true, displayName: "Politics" },
];

// Group fetched categories into priority groups
function groupCategories(categories: CategoryCount[]): CategoryOption[] {
	// Start with deep-cloned defaults so we never mutate the shared constant
	const result: CategoryOption[] = DEFAULT_CATEGORIES.map(c => ({ ...c }));

	// Track which additional categories to add
	const additionalCategories: CategoryOption[] = [];

	for (const cat of categories) {
		if (cat.name === "ALL") continue;

		const upperName = cat.name.toUpperCase();

		// Check if this is one of our default categories
		const defaultIndex = result.findIndex((c) => c.name === upperName);
		if (defaultIndex >= 0) {
			// Update the default category with real data, keep enabled
			result[defaultIndex] = {
				...result[defaultIndex],
				count: cat.count,
				enabled: true, // Always enable for filtering
			};
		} else if (cat.group && PRIORITY_CATEGORIES[cat.group]) {
			// This is a grouped category - add to additional if not already present
			const groupName = cat.group.toUpperCase();
			if (!result.find((c) => c.name === groupName) &&
				!additionalCategories.find((c) => c.name === groupName)) {
				additionalCategories.push({
					name: groupName,
					count: cat.count,
					enabled: true, // Always enable for filtering
					displayName: PRIORITY_CATEGORIES[cat.group],
				});
			} else {
				// Add count to existing
				const existing = result.find((c) => c.name === groupName) ||
					additionalCategories.find((c) => c.name === groupName);
				if (existing) {
					existing.count += cat.count;
					existing.enabled = true; // Always enable
				}
			}
		} else {
			// Other categories - always enable so users can filter
			additionalCategories.push({
				name: cat.name,
				count: cat.count,
				enabled: true, // Always enable for filtering
				displayName: cat.name,
			});
		}
	}

	// Sort additional categories by count descending
	additionalCategories.sort((a, b) => b.count - a.count);

	return [...result, ...additionalCategories];
}

// Flat list: every individual tag as its own category, no merging
function flattenCategories(categories: CategoryCount[]): CategoryOption[] {
	const result: CategoryOption[] = DEFAULT_CATEGORIES.map(c => ({ ...c }));

	for (const cat of categories) {
		if (cat.name === "ALL") continue;

		const upperName = cat.name.toUpperCase();
		const defaultIndex = result.findIndex((c) => c.name === upperName);
		if (defaultIndex >= 0) {
			result[defaultIndex] = { ...result[defaultIndex], count: cat.count, enabled: true };
		} else {
			result.push({
				name: cat.name,
				count: cat.count,
				enabled: true,
				displayName: cat.displayName ?? cat.name,
			});
		}
	}

	// Sort non-default entries by count descending
	const defaults = result.slice(0, DEFAULT_CATEGORIES.length);
	const rest = result.slice(DEFAULT_CATEGORIES.length).sort((a, b) => b.count - a.count);
	return [...defaults, ...rest];
}

export function useCategoriesQuery(options: UseCategoriesOptions = {}) {
	const { refreshInterval = SWR_REFRESH.categories, enabled = true } = options;

	const query = useSWR(
		enabled ? swrKeys.categories() : null,
		fetchCategoriesWithCounts,
		{
			refreshInterval,
			revalidateOnFocus: false,
			fallbackData: DEFAULT_CATEGORIES.map(c => ({ ...c, group: undefined })),
		},
	);

	const raw = query.data ?? [];

	// Group and normalize categories
	const groupedCategories = groupCategories(raw);
	// Flat: every individual tag, no merging
	const allCategories = flattenCategories(raw);

	return {
		categories: groupedCategories.map((c) => c.name),
		categoryDetails: groupedCategories,
		allCategoryDetails: allCategories,
		...query,
	};
}
