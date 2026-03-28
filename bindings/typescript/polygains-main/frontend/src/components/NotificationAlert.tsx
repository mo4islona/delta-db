import { useState } from "react";

export function NotificationAlert() {
	const [expanded, setExpanded] = useState(false);

	return (
		<div className="mb-2" data-theme="alert">
			<div
				role="alert"
				// CHANGES HERE:
				// 1. Removed 'alert-vertical'
				// 2. Added 'flex flex-row' to keep button on right
				// 3. Added 'items-start' to align icon/button with top of text
				// 4. Added 'text-left' for mobile alignment
				// 5. Added logic to flatten bottom corners when expanded
				className={`alert flex flex-row items-start text-left shadow-md ${expanded ? "rounded-t-lg rounded-b-none" : "rounded-lg"
					}`}
			>
				<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" className="h-6 w-6 shrink-0 stroke-info mt-1">
					<path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
				</svg>

				{/* CHANGE: Added 'flex-1' to push the button to the right */}
				<div className="flex-1">
					<h3 className="font-bold">Insider Alert: Venezuela</h3>
					<div className="text-xs">3 wallets bet on Maduro's ouster hours before arrest, netting $630K+</div>
				</div>

				<button
					type="button"
					// CHANGE: Added 'shrink-0' so the button doesn't get squished on very small screens
					className="btn btn-sm btn-info rounded-lg shrink-0"
					onClick={() => setExpanded(!expanded)}
				>
					{expanded ? "Hide" : "See"}
				</button>
			</div>

			{expanded && (
				<div className="bg-base-200 border-x border-b border-base-300 rounded-b-lg p-4 text-sm text-left">
					<p className="mb-4 text-base-content/80">
						Three insider wallets on Polymarket bet on Venezuelan President Maduro being out of office just hours before his arrest, netting a total profit of $630,484!
					</p>
					<p className="mb-4 text-base-content/80">
						The three wallets were created and pre-funded days in advance. Then, just hours before Maduro's arrest, they suddenly placed large bets on Maduro being out of office.
					</p>
					<p className="mb-4 text-base-content/80">
						Notably, all three wallets only bet on events related to Venezuela and Maduro, with no history of other bets — a clear case of insider trading.
					</p>
					<ul className="space-y-2 mt-4">

						<li>
							👉 Wallet <strong>0xa72D</strong> invested $5.8K and profited $75K{" "}
							<a
								href="https://polymarket.com/0xa72DB1749e9AC2379D49A3c12708325ED17FeBd4?tab=activity"
								target="_blank"
								rel="noreferrer"
								className="link link-info"
							>
								View on Polymarket
							</a>
						</li>
						<li>
							👉 Wallet <strong>SBet365</strong> invested $25K and profited $145.6K{" "}
							<a
								href="https://polymarket.com/@SBet365?tab=activity"
								target="_blank"
								rel="noreferrer"
								className="link link-info"
							>
								View on Polymarket
							</a>
						</li>
					</ul>
					<p className="mt-4 text-xs text-base-content/60">
						Source: <a href="https://x.com/lookonchain" target="_blank" rel="noreferrer" className="link link-info">@lookonchain</a> · Jan 4
					</p>
				</div>
			)}
		</div>
	);
}