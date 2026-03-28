/**
 * Reducers index - Export all reducers
 */

// Auth reducer
export {
	authReducer,
	initialAuthState,
} from "./authReducer";

export type {
	User,
	AuthState,
	AuthAction,
} from "./authReducer";

// Theme reducer
export {
	themeReducer,
	initialThemeState,
	resolveSystemTheme,
	applyTheme,
} from "./themeReducer";

export type {
	Theme,
	ThemeState,
	ThemeAction,
} from "./themeReducer";

// Terminal reducers
export {
	terminalUiReducer,
	initialTerminalUiState,
} from "./terminalUiReducer";

export type {
	TerminalUiState,
	TerminalUiAction,
	TradeSide,
} from "./terminalUiReducer";

export {
	terminalDataReducer,
	initialTerminalDataState,
} from "./terminalDataReducer";

export type {
	TerminalDataState,
	TerminalDataAction,
	BacktestStatus,
	BacktestRuntimeState,
} from "./terminalDataReducer";
