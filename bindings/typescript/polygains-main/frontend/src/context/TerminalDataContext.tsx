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
	initialTerminalDataState,
	terminalDataReducer,
	type TerminalDataAction,
	type TerminalDataState,
} from "@/reducers/terminalDataReducer";

let warnedMissingDataProvider = false;

const noopDispatch: Dispatch<TerminalDataAction> = () => {
	if (!warnedMissingDataProvider) {
		warnedMissingDataProvider = true;
		console.error("TerminalDataProvider missing; ignoring data dispatch");
	}
};

const TerminalDataStateContext = createContext<TerminalDataState>(
	initialTerminalDataState,
);
const TerminalDataDispatchContext = createContext<Dispatch<TerminalDataAction>>(
	noopDispatch,
);

export function TerminalDataProvider({ children }: { children: React.ReactNode }) {
	const [state, dispatch] = useReducer(
		terminalDataReducer,
		initialTerminalDataState,
	);
	const stableState = useMemo(() => state, [state]);

	return (
		<TerminalDataStateContext.Provider value={stableState}>
			<TerminalDataDispatchContext.Provider value={dispatch}>
				{children}
			</TerminalDataDispatchContext.Provider>
		</TerminalDataStateContext.Provider>
	);
}

export function useTerminalDataState(): TerminalDataState {
	return useContext(TerminalDataStateContext);
}

export function useTerminalDataDispatch(): Dispatch<TerminalDataAction> {
	return useContext(TerminalDataDispatchContext);
}
