"use client";
import { useEffect, useState } from "react";
import { AppProviders } from "./app/providers/AppProviders";
import { MainV2Page } from "./pages/PlaygroundLegacy";
import { TerminalPage } from "./pages/TerminalPage";
import "./index.css";

/**
 * Router component - handles client-side routing
 */
function Router() {
	const [path, setPath] = useState(window.location.pathname);

	useEffect(() => {
		const handleLocationChange = () => {
			setPath(window.location.pathname);
		};
		window.addEventListener("popstate", handleLocationChange);
		return () => window.removeEventListener("popstate", handleLocationChange);
	}, []);

	if (path === "/legacy") {
		return <MainV2Page />;
	}

	return <TerminalPage />;
}

/**
 * App component with providers
 *
 * Wraps the application with terminal providers and SWR config.
 */
export function App() {
	return (
		<AppProviders>
			<Router />
		</AppProviders>
	);
}

export default App;
