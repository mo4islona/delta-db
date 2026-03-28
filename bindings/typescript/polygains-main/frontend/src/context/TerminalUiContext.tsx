"use client";

import {
	createContext,
	useContext,
	useMemo,
	useReducer,
	type Dispatch,
} from "react";
import type React from "react";
import {
	initialTerminalUiState,
	terminalUiReducer,
	type TerminalUiAction,
	type TerminalUiState,
} from "@/reducers/terminalUiReducer";

let warnedMissingUiProvider = false;

const noopDispatch: Dispatch<TerminalUiAction> = () => {
	if (!warnedMissingUiProvider) {
		warnedMissingUiProvider = true;
		console.error("TerminalUiProvider missing; ignoring UI dispatch");
	}
};

const TerminalUiStateContext = createContext<TerminalUiState>(
	initialTerminalUiState,
);
const TerminalUiDispatchContext = createContext<Dispatch<TerminalUiAction>>(
	noopDispatch,
);

export function TerminalUiProvider({ children }: { children: React.ReactNode }) {
	const [state, dispatch] = useReducer(terminalUiReducer, initialTerminalUiState);
	const stableState = useMemo(() => state, [state]);

	return (
		<TerminalUiStateContext.Provider value={stableState}>
			<TerminalUiDispatchContext.Provider value={dispatch}>
				{children}
			</TerminalUiDispatchContext.Provider>
		</TerminalUiStateContext.Provider>
	);
}

export function useTerminalUiState(): TerminalUiState {
	return useContext(TerminalUiStateContext);
}

export function useTerminalUiDispatch(): Dispatch<TerminalUiAction> {
	return useContext(TerminalUiDispatchContext);
}
