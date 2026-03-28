/**
 * Theme Reducer - Pure logic for theme state changes
 */

export type Theme = "light" | "dark" | "system";

export interface ThemeState {
	theme: Theme;
	resolvedTheme: "light" | "dark";
}

export type ThemeAction =
	| { type: "SET_THEME"; payload: Theme }
	| { type: "SET_RESOLVED_THEME"; payload: "light" | "dark" };

export const initialThemeState: ThemeState = {
	theme: "system",
	resolvedTheme: "dark",
};

export function themeReducer(state: ThemeState, action: ThemeAction): ThemeState {
	switch (action.type) {
		case "SET_THEME":
			return {
				...state,
				theme: action.payload,
			};

		case "SET_RESOLVED_THEME":
			return {
				...state,
				resolvedTheme: action.payload,
			};

		default:
			return state;
	}
}

/**
 * Resolve theme based on system preference
 */
export function resolveSystemTheme(): "light" | "dark" {
	if (typeof window === "undefined") return "dark";

	return window.matchMedia("(prefers-color-scheme: dark)").matches
		? "dark"
		: "light";
}

/**
 * Apply theme to document
 */
export function applyTheme(theme: "light" | "dark"): void {
	if (typeof document === "undefined") return;

	const root = document.documentElement;
	root.classList.remove("light", "dark");
	root.classList.add(theme);
}
