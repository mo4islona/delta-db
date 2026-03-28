import React, { Fragment, useEffect, useState } from "react";
import { formatPnL } from "../../lib/backtest";
import {
	BANNER_ASCII,
	formatLargeNumber,
	formatMarketStat,
	formatMoney,
	getOutcomeMeta,
	hasAllStats,
	NO_ALERTS_ASCII,
	renderMarketPrice,
	TOP_LOGO_ASCII,
	timeAgo,
} from "../../lib/terminal";
import type {
	AlertsSectionProps,
	BannerProps,
	CategoryOption,
	DetectionSectionProps,
	GlobalStatsSectionProps,
	HeaderProps,
	LiveTrackerCardsProps,
	LiveTrackerControlsProps,

} from "../../types/terminal";

export function TerminalHeader({
	currentBlock,
	syncLabel,
	syncHealthy,
}: HeaderProps) {
	return (
		<div className="border-b border-base-content/10 mb-2 pb-3">
			<div className="flex items-start justify-between">
				<pre
					className="font-mono text-primary whitespace-pre overflow-hidden select-none flex-1 min-w-0"
					style={{
						fontSize: "clamp(4px, calc((100vw - 2rem) / 70), 9px)",
						lineHeight: 1.2,
					}}
					aria-label="PolyGains"
				>
					{BANNER_ASCII}
				</pre>
				<a
					href="https://x.com/mevtools"
					target="_blank"
					rel="noreferrer"
					className="text-[10px] md:text-xs font-mono text-base-content/50 hover:text-primary transition-colors whitespace-nowrap shrink-0 pt-1 pl-2"
				>
					@mevtools
				</a>
			</div>
			<div className="flex items-center justify-between mt-2 text-[10px] md:text-xs font-mono">
				<div className="text-base-content/70 whitespace-nowrap">
					BLOCK: {currentBlock}
				</div>
				<div
					className={`font-bold whitespace-nowrap ${syncHealthy ? "text-accent" : "text-error"}`}
				>
					{syncLabel}
				</div>
			</div>
		</div>
	);
}




// New component: Simulation explanation header
export function SimulationHeader() {
	return (
		<div className="card bg-base-300 shadow-xl border-l-4 border-primary mb-4 font-mono text-xs md:text-sm">
			<div className="card-body p-5 md:p-6">
				<h3 className="text-primary uppercase text-xs mb-3">
					<span className="text-primary mr-2">$</span> RUN EXPLAIN-SIMULATION
				</h3>
				<div className="flex flex-col gap-3 text-base-content/80">
					<p className="font-bold text-base-content">
						Simulate Copytrading
					</p>
					<p className="text-xs opacity-70 -mt-2">
						Set your trading parameters to test alpha.
					</p>
					<ul className="flex flex-col gap-2 mt-1">
						<li className="flex items-start gap-2">
							<span className="text-primary opacity-50 select-none">{`>`}</span>
							<span>
								<span className="uppercase text-[10px] tracking-wider opacity-60 mr-2">Mode:</span>
								<span className="text-success font-bold">FOLLOW</span> (Copy trades exactly) or <span className="text-error font-bold">REVERSE</span> (Bet against the wallet).
							</span>
						</li>
						<li className="flex items-start gap-2">
							<span className="text-primary opacity-50 select-none">{`>`}</span>
							<span>
								<span className="uppercase text-[10px] tracking-wider opacity-60 mr-2">Sizing:</span>
								<span className="font-bold">FIXED</span> ($10 per bet) or <span className="font-bold">PROPORTIONAL</span> (% of their size).
							</span>
						</li>
						<li className="flex items-start gap-2">
							<span className="text-primary opacity-50 select-none">{`>`}</span>
							<span>
								Choose your minimum price for entry.
								<span
									className="inline-block w-1.5 h-3 bg-accent align-middle ml-1"
									style={{ animation: "cursor-blink 1s step-end infinite" }}
								/>
							</span>
						</li>
					</ul>
				</div>
			</div>
		</div>
	);
}

export function LiveTrackerControls({
	minPrice,
	maxPrice,
	onlyBetOnce,
	betOneDollarPerTrade,
	disabled = false,
	selectedStrategies,
	selectedSides,
	onMinPriceChange,
	onMaxPriceChange,
	onOnlyBetOnceChange,
	onBetOneDollarPerTradeChange,
	onStrategyChange,
	onSideToggle,
}: LiveTrackerControlsProps) {
	const [minDraft, setMinDraft] = useState(minPrice.toFixed(2));
	const [maxDraft, setMaxDraft] = useState(maxPrice.toFixed(2));

	useEffect(() => {
		setMinDraft(minPrice.toFixed(2));
	}, [minPrice]);

	useEffect(() => {
		setMaxDraft(maxPrice.toFixed(2));
	}, [maxPrice]);

	const commitMinPrice = () => {
		const parsed = Number(minDraft);
		const next = Number.isFinite(parsed) ? parsed : minPrice;
		setMinDraft(next.toFixed(2));
		onMinPriceChange(next);
	};

	const commitMaxPrice = () => {
		const parsed = Number(maxDraft);
		const next = Number.isFinite(parsed) ? parsed : maxPrice;
		setMaxDraft(next.toFixed(2));
		onMaxPriceChange(next);
	};

	return (
		<div className="card bg-base-300 shadow-xl mb-4 font-mono text-xs md:text-sm">
			<div className="card-body p-4">
				<div className="overflow-x-auto pb-0">
					<div className="flex gap-4 items-center flex-nowrap min-w-max">
						<div className="flex gap-1 items-center">
							<input
								type="text"
								inputMode="decimal"
								disabled={disabled}
								value={minDraft}
								placeholder="Min P"
								className="input input-xs input-bordered w-14 text-center"
								onChange={(event) => setMinDraft(event.currentTarget.value)}
								onFocus={(event) => event.currentTarget.select()}
								onBlur={commitMinPrice}
								onKeyDown={(event) => {
									if (event.key === "Enter") event.currentTarget.blur();
								}}
							/>
							<input
								type="text"
								inputMode="decimal"
								disabled={disabled}
								value={maxDraft}
								placeholder="Max P"
								className="input input-xs input-bordered w-14 text-center"
								onChange={(event) => setMaxDraft(event.currentTarget.value)}
								onFocus={(event) => event.currentTarget.select()}
								onBlur={commitMaxPrice}
								onKeyDown={(event) => {
									if (event.key === "Enter") event.currentTarget.blur();
								}}
							/>
						</div>
						<div className="flex gap-3 items-center">
							<label className="cursor-pointer label p-0 gap-2 whitespace-nowrap">
								<input
									className="checkbox checkbox-xs checkbox-primary"
									type="checkbox"
									disabled={disabled}
									checked={onlyBetOnce}
									onChange={(event) =>
										onOnlyBetOnceChange(event.currentTarget.checked)
									}
								/>
								<span className="label-text text-[10px] text-base-content/80">
									1 BET/MKT
								</span>
							</label>
							<label className="cursor-pointer label p-0 gap-2 whitespace-nowrap">
								<input
									className="checkbox checkbox-xs checkbox-accent"
									type="checkbox"
									disabled={disabled}
									checked={betOneDollarPerTrade}
									onChange={(event) =>
										onBetOneDollarPerTradeChange(event.currentTarget.checked)
									}
								/>
								<span className="label-text text-[10px] text-base-content/80">
									FIXED $10
								</span>
							</label>
						</div>

						<div className="divider divider-horizontal mx-0" />
						<div className="flex gap-3 items-center">
							<label className="cursor-pointer label p-0 gap-2 whitespace-nowrap">
								<input
									className="checkbox checkbox-xs checkbox-success"
									type="checkbox"
									disabled={disabled}
									checked={selectedStrategies.includes("follow_insider")}
									onChange={(event) =>
										onStrategyChange(
											"follow_insider",
											event.currentTarget.checked,
										)
									}
								/>
								<span className="label-text text-[10px] text-base-content/80">
									FOLLOW
								</span>
							</label>
							<label className="cursor-pointer label p-0 gap-2 whitespace-nowrap">
								<input
									className="checkbox checkbox-xs checkbox-error"
									type="checkbox"
									disabled={disabled}
									checked={selectedStrategies.includes("reverse_insider")}
									onChange={(event) =>
										onStrategyChange(
											"reverse_insider",
											event.currentTarget.checked,
										)
									}
								/>
								<span className="label-text text-[10px] text-base-content/80">
									REVERSE
								</span>
							</label>
						</div>
						<div className="divider divider-horizontal mx-0" />
						<div className="flex gap-3 items-center">
							<label className="cursor-pointer label p-0 gap-2 whitespace-nowrap">
								<input
									className="checkbox checkbox-xs checkbox-info"
									type="checkbox"
									disabled={disabled}
									checked={selectedSides.includes("YES")}
									onChange={(event) =>
										onSideToggle("YES", event.currentTarget.checked)
									}
								/>
								<span className="label-text text-[10px] text-base-content/80">
									YES
								</span>
							</label>
							<label className="cursor-pointer label p-0 gap-2 whitespace-nowrap">
								<input
									className="checkbox checkbox-xs checkbox-warning"
									type="checkbox"
									disabled={disabled}
									checked={selectedSides.includes("NO")}
									onChange={(event) =>
										onSideToggle("NO", event.currentTarget.checked)
									}
								/>
								<span className="label-text text-[10px] text-base-content/80">
									NO
								</span>
							</label>
						</div>
					</div>
				</div>
			</div>
		</div>
	);
}

"use client";
import { useState } from "react";
import { useSignupMutation } from "@/hooks/mutations/useSignupMutation";

export function EmailSignup() {
	const [email, setEmail] = useState("");
	const { signup, isLoading, data, error, reset } = useSignupMutation();

	const handleSubmit = async () => {
		if (!email) return;
		await signup(email);
		setEmail("");
	};

	const showSuccess = data?.success;
	const showError = error || (data && !data.success);

	return (
		<div className="card bg-base-300 shadow-xl mb-4 font-mono text-xs md:text-sm">
			<div className="card-body p-4">
				<div className="flex flex-col lg:flex-row items-center justify-center gap-4 lg:gap-6">
					<div className="text-base-content/80 text-xs whitespace-nowrap">
						<span className="text-primary font-bold">$</span> Subscribe to alerts
					</div>
					<div className="join w-full lg:w-auto">
						<div className="flex-1">
							<label className={`input validator join-item w-full sm:w-[32rem] ${showError ? 'input-error' : showSuccess ? 'input-success' : ''}`}>
								<svg className="h-[1em] opacity-50" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">
									<g
										strokeLinejoin="round"
										strokeLinecap="round"
										strokeWidth="2.5"
										fill="none"
										stroke="currentColor"
									>
										<rect width="20" height="16" x="2" y="4" rx="2"></rect>
										<path d="m22 7-8.97 5.7a1.94 1.94 0 0 1-2.06 0L2 7"></path>
									</g>
								</svg>
								<input
									type="email"
									placeholder="mail@site.com"
									required
									value={email}
									onChange={(e) => {
										setEmail(e.target.value);
										if (showSuccess || showError) reset();
									}}
									onKeyDown={(e) => {
										if (e.key === 'Enter') handleSubmit();
									}}
								/>
								{showSuccess && <span className="text-success">✓</span>}
								{showError && <span className="text-error">✗</span>}
							</label>
							<div className="validator-hint hidden">Enter valid email address</div>
						</div>
						<button
							className={`btn join-item ${isLoading ? 'btn-disabled' : showSuccess ? 'btn-success' : showError ? 'btn-error' : 'btn-neutral'}`}
							onClick={handleSubmit}
							disabled={isLoading || !email}
						>
							{isLoading ? 'Sending...' : showSuccess ? 'Subscribed!' : showError ? 'Failed' : 'Join'}
						</button>
					</div>
				</div>
				{showError && (
					<div className="text-error text-xs text-center mt-2">
						{data?.error || error?.message || 'Failed to subscribe'}
					</div>
				)}
				{showSuccess && (
					<div className="text-success text-xs text-center mt-2">
						{data?.message || 'Subscribed successfully!'}
					</div>
				)}
			</div>
		</div>
	);
}

export function LiveTrackerCards({
	totalBet,
	openInterest,
	realizedPnL,
	liveTrades,
	liveWins,
	liveLosses,
	backtestCanContinue,
	backtestRunning,
	onRunBacktest,
}: LiveTrackerCardsProps) {
	return (
		<div className="stats stats-vertical lg:stats-horizontal shadow w-full bg-base-200 border border-base-content/10 mb-8">
			<div className="stat">
				<div className="stat-title text-base-content/70 uppercase text-xs tracking-wider font-bold">
					Money Bet
				</div>
				<div className="stat-value text-base-content text-xl font-mono">
					${formatLargeNumber(totalBet)}
				</div>
				<div className="stat-desc text-base-content/70 text-xs mt-1">
					Open:{" "}
					<span className="text-base-content">
						${formatLargeNumber(openInterest)}
					</span>
				</div>
			</div>

			<div className="stat">
				<div className="stat-title text-base-content/70 uppercase text-xs tracking-wider font-bold">
					PnL
				</div>
				<div
					className={`stat-value text-xl font-mono ${realizedPnL > 0 ? "text-accent" : realizedPnL < 0 ? "text-error" : ""}`}
				>
					{formatPnL(realizedPnL)}
				</div>
			</div>

			<div className="stat relative">
				<div className="stat-title text-base-content/70 uppercase text-xs tracking-wider font-bold">
					Trades
				</div>
				<div className="stat-value text-base-content text-xl font-mono">
					{liveTrades}
					<span className="text-xs text-base-content/70 ml-2 font-normal">
						(W:{liveWins} L:{liveLosses})
					</span>
				</div>
				<div className="stat-actions absolute top-0 right-0 bottom-0 flex items-center pr-4">
					<button
						type="button"
						className={`btn ${backtestRunning ? "btn-disabled" : "btn-primary"} h-full rounded-none border-t-0 border-b-0 border-r-0 border-l px-6 shadow-lg`}
						disabled={backtestRunning}
						onClick={onRunBacktest}
						aria-label={
							backtestRunning
								? "Processing backtest"
								: backtestCanContinue
									? "Continue backtest"
									: "Run backtest"
						}
						title={
							backtestRunning
								? "Processing..."
								: backtestCanContinue
									? "Continue Backtest"
									: "Run Backtest"
						}
					>
						{backtestRunning
							? "Processing..."
							: backtestCanContinue
								? "Continue"
								: "Run Backtest"}
					</button>
				</div>
			</div>
		</div>
	);
}

function NoAlertsAscii() {
	const [frame, setFrame] = useState(0);
	useEffect(() => {
		const timer = setInterval(() => setFrame((f) => f + 1), 200);
		return () => clearInterval(timer);
	}, []);

	const glitch = frame % 2 === 0 ? "opacity-100" : "opacity-50";
	const text = frame % 4 === 0 ? "NO SIGNALS DETECTED" : "SEARCHING...";

	return (
		<div className="flex flex-col items-center justify-center py-12 gap-4 font-mono text-xs text-primary/70">
			<pre className={`leading-[0.6rem] whitespace-pre ${glitch}`}>
				{NO_ALERTS_ASCII}
			</pre>
			<div className="tracking-[0.2em] animate-pulse-gpu">{text}</div>
		</div>
	);
}

const MORE_PAGE_SIZE = 8;

// Main category filter with ALL, CRYPTO, SPORTS, POLITICS + "..." daisyUI dropdown
// Supports toggling between "grouped" (merged parent categories) and "all" (every tag)
function CategoryFilter({
	categories,
	allCategories,
	selectedCategory,
	onCategoryChange,
}: {
	categories: CategoryOption[];
	allCategories?: CategoryOption[];
	selectedCategory: string;
	onCategoryChange: (value: string) => void;
}) {
	const [morePage, setMorePage] = useState(0);
	const [showAll, setShowAll] = useState(false);
	const safeAllCategories = allCategories ?? [];

	// Main categories to always show as buttons
	const mainCategoryNames = ["ALL", "CRYPTO", "SPORTS", "POLITICS"];

	const mainCategories = mainCategoryNames
		.map((name) => categories.find((c) => c.name === name))
		.filter(Boolean) as CategoryOption[];

	// Pick the active source based on mode
	const source = showAll ? safeAllCategories : categories;
	const moreCategories = source.filter(
		(c) => !mainCategoryNames.includes(c.name),
	);

	// Reset page when switching mode or when list length changes
	const clampedPage = Math.min(morePage, Math.max(0, Math.ceil((moreCategories?.length || 0) / MORE_PAGE_SIZE) - 1));
	if (clampedPage !== morePage) setMorePage(clampedPage);

	const pageCount = Math.ceil((moreCategories?.length || 0) / MORE_PAGE_SIZE);
	const pageItems = moreCategories.slice(
		morePage * MORE_PAGE_SIZE,
		(morePage + 1) * MORE_PAGE_SIZE,
	);

	// Check if selected category is in "more" — show its name on the button
	const selectedInMore = moreCategories.find((c) => c.name === selectedCategory);
	const moreButtonLabel = selectedInMore ? selectedInMore.displayName : "...";
	const isMoreSelected = Boolean(selectedInMore);

	// Two-part layout: the main buttons scroll horizontally inside their own overflow
	// container, while the "..." dropdown is a sibling outside it — this prevents the
	// overflow context from clipping the dropdown-content (position:absolute).
	return (
		<div className="flex items-center gap-1">
			{/* Main category buttons in their own scrollable container */}
			<div className="flex items-center gap-1 overflow-x-auto no-scrollbar">
				{mainCategories.map((category) => (
					<button
						key={category.name}
						type="button"
						className={`btn btn-sm min-h-[30px] whitespace-nowrap ${category.name === selectedCategory
							? "btn-primary"
							: "btn-ghost"
							}`}
						onClick={() => onCategoryChange(category.name)}
						aria-label={`Filter alerts by ${category.displayName}`}
						aria-pressed={category.name === selectedCategory}
						title={`${category.displayName}${category.count > 0 ? ` (${category.count} markets)` : ""}`}
					>
						{category.displayName}
					</button>
				))}
			</div>

			{/* "..." dropdown - sibling to overflow div, so it is never clipped */}
			<div className="dropdown dropdown-end shrink-0">
				<div
					tabIndex={(moreCategories?.length || 0) > 0 ? 0 : -1}
					role="button"
					className={`btn btn-sm min-h-[40px] min-w-[40px] ${isMoreSelected ? "btn-primary" : "btn-ghost"} ${moreCategories.length === 0 ? "btn-disabled opacity-40" : ""}`}
					aria-label="More categories"
					title="More categories"
				>
					{moreButtonLabel}
				</div>
				{(moreCategories?.length || 0) > 0 && (
					<ul tabIndex={0} className="menu dropdown-content bg-base-200 rounded-box z-50 mt-1 w-52 p-2 shadow-xl max-h-[60vh] overflow-y-auto">
						{/* Mode toggle */}
						<li className="mb-1">
							<div
								className="flex justify-between px-1 pb-1 border-b border-base-content/10"
								onMouseDown={(e) => e.preventDefault()}
							>
								<button
									type="button"
									className={`btn btn-xs ${!showAll ? "btn-primary" : "btn-ghost"}`}
									onClick={(e) => { e.stopPropagation(); setShowAll(false); setMorePage(0); }}
								>
									Grouped
								</button>
								<button
									type="button"
									className={`btn btn-xs ${showAll ? "btn-primary" : "btn-ghost"}`}
									onClick={(e) => { e.stopPropagation(); setShowAll(true); setMorePage(0); }}
								>
									All ({safeAllCategories?.length || 0 - mainCategoryNames?.length || 0})
								</button>
							</div>
						</li>
						{pageItems.map((category) => (
							<li key={category.name}>
								<a
									className={category.name === selectedCategory ? "active" : ""}
									onClick={() => {
										onCategoryChange(category.name);
										(document.activeElement as HTMLElement)?.blur();
									}}
								>
									{category.displayName}
									{category.count > 0 && (
										<span className="badge badge-sm badge-ghost">{category.count}</span>
									)}
								</a>
							</li>
						))}
						{pageCount > 1 && (
							<li className="mt-1">
								<div
									className="flex justify-between px-1 pt-1 border-t border-base-content/10"
									onMouseDown={(e) => e.preventDefault()}
								>
									<button
										type="button"
										className="btn btn-xs btn-ghost"
										disabled={morePage === 0}
										onClick={(e) => { e.stopPropagation(); setMorePage(p => p - 1); }}
									>
										←
									</button>
									<span className="text-xs text-base-content/50 self-center">
										{morePage + 1}/{pageCount}
									</span>
									<button
										type="button"
										className="btn btn-xs btn-ghost"
										disabled={morePage >= pageCount - 1}
										onClick={(e) => { e.stopPropagation(); setMorePage(p => p + 1); }}
									>
										→
									</button>
								</div>
							</li>
						)}
					</ul>
				)}
			</div>
		</div>
	);
}

const AlertsSectionComponent = ({
	rows,
	pagination,
	selectedCategory,
	selectedWinnerFilter,
	categoryOptions,
	allCategoryOptions,
	isLoading = false,
	onPrev,
	onNext,
	onCategoryChange,
	onWinnerFilterChange,
}: AlertsSectionProps) => {
	return (
		<>
			<div className="flex flex-col lg:flex-row lg:justify-between lg:items-center mb-4 mt-8 gap-4 filter-bar-min-height">
				<h2 className="text-xs font-bold text-base-content/70 uppercase tracking-wider section-header-min-height flex items-center shrink-0">
					RECENT_UNUSUAL_ALERTS
				</h2>
				<div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center">
					{/* Category filter with main 4 + ... menu */}
					<div className="w-full sm:w-auto">
						<CategoryFilter
							categories={categoryOptions}
							allCategories={allCategoryOptions}
							selectedCategory={selectedCategory}
							onCategoryChange={onCategoryChange}
						/>
					</div>
					{/* disable for now! <div className="join shrink-0">
						{(["BOTH", "WINNERS", "LOSERS"] as const).map((filter) => (
							<button
								key={filter}
								type="button"
								className={`join-item btn btn-sm min-w-[48px] min-h-[48px] ${filter === selectedWinnerFilter ? "btn-secondary" : "btn-ghost"}`}
								onClick={() => onWinnerFilterChange(filter)}
								aria-label={`Show ${filter.toLowerCase()}`}
								aria-pressed={filter === selectedWinnerFilter}
								title={`Show ${filter.toLowerCase()}`}
							>
								{filter}
							</button>
						))}
					</div> */}
				</div>
			</div>

			<div className="overflow-x-auto bg-base-200 rounded-box border border-base-content/10 mb-8 alerts-table-container">
				<table className="table table-xs w-full table-fixed min-w-[600px] md:min-w-full">
					<thead>
						<tr className="bg-base-300 text-base-content/70 uppercase tracking-wider">
							<th className="w-[30%] md:w-[35%]">Recent unuasal activity</th>
							<th className="w-[12%] md:w-[15%]">Place bet on</th>
							<th className="w-[12%] md:w-[12%] text-right">Entry Price</th>
							<th className="w-[15%] md:w-[15%] text-right">$ Total</th>
							<th className="w-[18%] md:w-[13%] text-right">Time</th>
							<th className="w-[13%] md:w-[10%] text-center">Lookup</th>
						</tr>
					</thead>
					<tbody className="alerts-tbody-min-height">
						{isLoading ? (
							[...Array(10)].map((_, i) => (
								<tr
									key={`skeleton-${i}`}
									className="border-b border-base-content/5"
								>
									<td>
										<div className="skeleton h-4 w-full max-w-[250px]" />
									</td>
									<td>
										<div className="skeleton h-4 w-12" />
									</td>
									<td className="text-right">
										<div className="skeleton h-4 w-16 ml-auto" />
									</td>
									<td className="text-right">
										<div className="skeleton h-4 w-20 ml-auto" />
									</td>
									<td className="text-right">
										<div className="skeleton h-4 w-14 ml-auto" />
									</td>
									<td className="text-center">
										<div className="skeleton h-8 w-8 mx-auto" />
									</td>
								</tr>
							))
						) : rows.length === 0 ? (
							<tr>
								<td colSpan={6} className="text-center p-0">
									<NoAlertsAscii />
								</td>
							</tr>
						) : (
							rows.map((row, index) => {
								const isYes = row.outcomeLabel === "YES";
								return (
									<Fragment key={row.rowId}>
										<tr
											className={`table-row-optimized border-b border-base-content/5 ${index % 2 === 1 ? "bg-white/5" : "bg-transparent"
												}`}
										>
											<td className="max-w-[300px]">
												<div
													className="font-bold text-base-content truncate"
													title={row.question}
												>
													{row.question || `Condition: ${row.conditionId}`}
												</div>
												<div className="text-[10px] font-mono text-base-content/60 truncate">
													{row.conditionId}
												</div>
											</td>
											<td>
												<div className="flex items-center gap-2">
													<span
														className={`badge badge-sm font-bold border-none rounded-sm px-2 py-0.5 text-[10px] uppercase ${isYes
															? "bg-success/20 text-success"
															: row.outcomeLabel === "NO"
																? "bg-error/20 text-error"
																: "bg-base-content/20 text-base-content"
															}`}
													>
														{row.outcomeLabel}
													</span>
													{row.statusBadgeHtml && (
														<span
															dangerouslySetInnerHTML={{
																__html: row.statusBadgeHtml,
															}}
														/>
													)}
												</div>
											</td>
											<td className="text-right font-mono text-base-content/80">
												@{row.priceFormatted}
											</td>
											<td className="text-right font-mono font-bold text-base-content">
												{formatMoney(row.volume)}
											</td>
											<td className="text-right text-xs tabular-nums text-base-content/70">
												{timeAgo(row.timestamp)}
											</td>
											<td className="text-center">
												<a
													href={`https://polymarket.com/profile/${row.profileAddress}`}
													target="_blank"
													rel="noreferrer"
													className="btn btn-ghost btn-sm text-base-content/80 hover:text-base-content min-w-[44px] min-h-[44px]"
													aria-label={`Lookup trader ${row.user}`}
													title={`Lookup trader ${row.user}`}
												>
													<svg
														xmlns="http://www.w3.org/2000/svg"
														viewBox="0 0 20 20"
														fill="currentColor"
														className="w-4 h-4"
													>
														<title>Search Icon</title>
														<path
															fillRule="evenodd"
															d="M9 3.5a5.5 5.5 0 100 11 5.5 5.5 0 000-11zM2 9a7 7 0 1112.452 4.391l3.328 3.329a.75.75 0 11-1.06 1.06l-3.329-3.328A7 7 0 012 9z"
															clipRule="evenodd"
														/>
													</svg>
												</a>
											</td>
										</tr>
									</Fragment>
								);
							})
						)}
					</tbody>
				</table>

				<div className="flex justify-between items-center p-4 border-t border-base-content/10 bg-base-200">
					<button
						type="button"
						className="btn btn-sm btn-ghost min-w-[48px] min-h-[44px]"
						onClick={onPrev}
						disabled={isLoading || !pagination.hasPrev}
						aria-label="Previous page"
					>
						{isLoading ? (
							<span
								className="loading loading-spinner loading-xs loading-optimized"
								aria-hidden="true"
							/>
						) : (
							"← PREV"
						)}
					</button>
					<span className="text-xs font-mono text-base-content/70 flex items-center gap-2">
						{isLoading && <span className="loading loading-dots loading-xs" />}
						Page {pagination.page} of {pagination.totalPages} (
						{pagination.total} total)
					</span>
					<button
						type="button"
						className="btn btn-sm btn-ghost min-w-[48px] min-h-[44px]"
						onClick={onNext}
						disabled={isLoading || !pagination.hasNext}
						aria-label="Next page"
					>
						{isLoading ? (
							<span
								className="loading loading-spinner loading-xs loading-optimized"
								aria-hidden="true"
							/>
						) : (
							"NEXT →"
						)}
					</button>
				</div>
			</div>
		</>
	);
};

export const AlertsSection = React.memo(
	AlertsSectionComponent,
	(prev: AlertsSectionProps, next: AlertsSectionProps) => {
		if (prev.isLoading !== next.isLoading) return false;
		if (prev.selectedCategory !== next.selectedCategory) return false;
		if (prev.selectedWinnerFilter !== next.selectedWinnerFilter) return false;
		if (JSON.stringify(prev.pagination) !== JSON.stringify(next.pagination))
			return false;
		if (
			JSON.stringify(prev.categoryOptions) !==
			JSON.stringify(next.categoryOptions)
		)
			return false;
		if (
			JSON.stringify(prev.allCategoryOptions) !==
			JSON.stringify(next.allCategoryOptions)
		)
			return false;
		return JSON.stringify(prev.rows) === JSON.stringify(next.rows);
	},
);

export function DetectionSection({
	totalInsiders,
	yesInsiders,
	noInsiders,
	insiderVolume,
}: DetectionSectionProps) {
	return (
		<>
			<h2 className="text-xs font-bold text-base-content/80 uppercase tracking-wider mb-2 mt-4 section-header-min-height flex items-center">
				UNUSUAL_TRADES_STATS
			</h2>
			<div className="stats stats-vertical lg:stats-horizontal shadow w-full bg-base-200 border border-base-content/10">
				<div className="stat">
					<div className="stat-title text-base-content/70 uppercase text-xs font-bold">
						Total
					</div>
					<div className="stat-value text-accent text-xl">{totalInsiders}</div>
				</div>
				<div className="stat">
					<div className="stat-title text-base-content/70 uppercase text-xs font-bold">
						YES
					</div>
					<div className="stat-value text-accent text-xl">{yesInsiders}</div>
				</div>
				<div className="stat">
					<div className="stat-title text-base-content/70 uppercase text-xs font-bold">
						NO
					</div>
					<div className="stat-value text-error text-xl">{noInsiders}</div>
				</div>
				<div className="stat">
					<div className="stat-title text-base-content/70 uppercase text-xs font-bold">
						Volume
					</div>
					<div className="stat-value text-base-content text-xl">
						{insiderVolume}
					</div>
				</div>
			</div>
		</>
	);
}



export function GlobalStatsSection({
	accounts,
	markets,
	trades,
	activePositions,
}: GlobalStatsSectionProps) {
	return (
		<>
			<h2 className="text-xs font-bold text-base-content/80 uppercase tracking-wider mb-2 mt-4 section-header-min-height flex items-center">
				GLOBAL_STATS
			</h2>
			<div className="stats stats-vertical lg:stats-horizontal shadow w-full bg-base-200 border border-base-content/10">
				<div className="stat">
					<div className="stat-title text-base-content/80 uppercase text-xs font-bold">
						Accounts
					</div>
					<div className="stat-value text-base-content text-xl">{accounts}</div>
				</div>
				<div className="stat">
					<div className="stat-title text-base-content/80 uppercase text-xs font-bold">
						Markets
					</div>
					<div className="stat-value text-base-content text-xl">{markets}</div>
				</div>
				<div className="stat">
					<div className="stat-title text-base-content/80 uppercase text-xs font-bold">
						Total Fills
					</div>
					<div className="stat-value text-base-content text-xl">{trades}</div>
				</div>
				<div className="stat">
					<div className="stat-title text-base-content/80 uppercase text-xs font-bold">
						Active Pos
					</div>
					<div className="stat-value text-accent text-xl">
						{activePositions}
					</div>
				</div>
			</div>
		</>
	);
}

export function TerminalBanner({ currentBlock }: BannerProps) {
	return (
		<div className="card bg-base-100 border-y-2 border-accent rounded-none mb-8 relative overflow-hidden">
			<div className="absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r from-transparent via-accent/70 to-transparent shadow-[0_0_10px_rgba(16,185,129,0.6)] animate-[scan_4s_ease-in-out_infinite] pointer-events-none z-10" />
			<div className="card-body p-6 font-mono text-xs md:text-sm">
				<pre className="text-accent text-[0.5rem] md:text-xs leading-none mb-4 whitespace-pre overflow-x-hidden">
					{BANNER_ASCII}
				</pre>

				<div className="flex flex-col md:flex-row justify-between border-b border-accent/30 pb-3 mb-4 text-accent gap-2 text-[10px] md:text-xs">
					<span className="break-all md:break-normal">
						[v1.0.3] | Zero-History Trade Detection System by @mevtools
					</span>
					<span className="whitespace-nowrap">
						BLOCK: {currentBlock} | STATUS:{" "}
						<span className="text-base-100 bg-accent px-1 font-bold">
							INDEXING
						</span>
					</span>
				</div>

				<div className="flex flex-col gap-1 text-accent relative z-0">
					<div className="opacity-0 animate-[typeIn_0.3s_forwards_0.5s]">
						$ connecting to <span className="font-bold">polymarket</span>...
					</div>
					<div className="opacity-0 animate-[typeIn_0.3s_forwards_1.2s]">
						&gt; using <span className="font-bold">subsquid pipes</span> for
						indexing...
					</div>
					<div className="opacity-0 animate-[typeIn_0.3s_forwards_1.8s]">
						&gt; triggering alerts...
					</div>
					<div className="opacity-0 animate-[typeIn_0.3s_forwards_2.4s]">
						&gt; calibrating detection thresholds...
					</div>
					<div className="opacity-0 animate-[typeIn_0.3s_forwards_3.0s]">
						<span className="font-bold">[OK]</span> polygains detection system{" "}
						<span className="text-base-100 bg-accent px-1 font-bold">
							ONLINE
						</span>
						<span className="inline-block w-1.5 h-3 bg-accent animate-pulse-gpu align-middle ml-1" />
					</div>
				</div>
			</div>
		</div>
	);
}

export function TerminalFooter() {
	return (
		<footer className="border-t border-base-content/10 py-6 mt-8">
			<div className="flex flex-col md:flex-row items-center justify-between gap-4">
				<div className="flex items-center gap-4">
					<a
						href="https://github.com/mevtools/polygains"
						target="_blank"
						rel="noreferrer"
						className="text-base-content/60 hover:text-primary transition-colors"
						aria-label="GitHub"
					>
						<svg
							xmlns="http://www.w3.org/2000/svg"
							viewBox="0 0 24 24"
							fill="currentColor"
							className="w-5 h-5"
						>
							<title>GitHub</title>
							<path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12" />
						</svg>
					</a>
					<a
						href="https://sqd.dev"
						target="_blank"
						rel="noreferrer"
						className="opacity-80 hover:opacity-100 transition-opacity"
						aria-label="Powered by SQD Network"
					>
						<img
							src="/assets/Powered by SQD Network.svg"
							alt="Powered by SQD Network"
							className="h-6 w-auto"
						/>
					</a>
				</div>
				<div className="text-[10px] text-base-content/40 font-mono">
					© {new Date().getFullYear()} PolyGains
				</div>
			</div>
		</footer>
	);
}
