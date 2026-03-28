"use client";
import type React from "react";
import { SWRConfig } from "swr";
import { TerminalDataProvider } from "@/context/TerminalDataContext";
import { TerminalUiProvider } from "@/context/TerminalUiContext";
import { appSWRConfig } from "./swrConfig";

export function AppProviders({ children }: { children: React.ReactNode }) {
	return (
		<SWRConfig value={appSWRConfig}>
			<TerminalUiProvider>
				<TerminalDataProvider>{children}</TerminalDataProvider>
			</TerminalUiProvider>
		</SWRConfig>
	);
}
