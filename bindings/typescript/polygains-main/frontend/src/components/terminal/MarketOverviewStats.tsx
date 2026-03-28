import { useEffect, useLayoutEffect, useRef, useState } from "react";
import {
	formatMarketStat,
	getOutcomeMeta,
	hasAllStats,
	renderMarketPrice,
} from "../../lib/terminal";
import type { GroupedMarket, MarketsSectionProps } from "../../types/terminal";

const LOADING_MARKETS_ASCII = `
╔══════════════════════════════╗
║   SYNCING MARKET OVERVIEW    ║
║       PLEASE STAND BY        ║
╚══════════════════════════════╝
`;

export function MarketsOverviewStatsPreview({
	markets,
	pagination,
	isLoading = false,
	marketStatsLoadingByCondition = {},
	onPrev,
	onNext,
}: MarketsSectionProps) {
	const [lastStableMarkets, setLastStableMarkets] = useState<GroupedMarket[]>([]);
	const [lockedListHeight, setLockedListHeight] = useState<number | null>(null);
	const listShellRef = useRef<HTMLDivElement | null>(null);

	useEffect(() => {
		if (!isLoading && markets.length > 0) {
			setLastStableMarkets(markets);
		}
	}, [isLoading, markets]);

	const displayMarkets =
		markets.length > 0 ? markets : isLoading ? lastStableMarkets : markets;
	const showPlaceholderList = isLoading && displayMarkets.length === 0;
	const placeholderCount = Math.max(1, pagination.limit || 10);

	useLayoutEffect(() => {
		if (isLoading) return;
		const el = listShellRef.current;
		if (!el) return;
		const nextHeight = Math.ceil(el.getBoundingClientRect().height);
		if (nextHeight > 0) {
			setLockedListHeight(nextHeight);
		}
	}, [isLoading, displayMarkets, pagination.page, pagination.total, pagination.totalPages]);

	return (
		<>
			<h2 className="text-xs font-bold text-base-content/80 uppercase tracking-wider mb-4 mt-8 section-header-min-height flex items-center">
				MARKET_OVERVIEW
			</h2>

			<div
			// className="rounded-box border border-base-content/10 mb-8 p-2 contain-paint relative"
			>

				<div className="relative">
					<div
						ref={listShellRef}
						className={isLoading ? "pointer-events-none select-none" : undefined}
						style={
							isLoading && lockedListHeight
								? { minHeight: `${lockedListHeight}px` }
								: undefined
						}
					>
						{showPlaceholderList ? (
							<div className="flex flex-col gap-4">
								{Array.from({ length: placeholderCount }).map((_, index) => (
									<div
										key={`market-stats-skel-${index}`}
										className="card bg-base-300/30 border border-base-content/5 p-4 rounded-box"
									>
										<div className="skeleton h-5 w-full max-w-[600px] mb-3" />
										<div className="stats stats-vertical lg:stats-horizontal shadow w-full bg-base-100 border border-base-content/10">
											<div className="stat">
												<div className="skeleton h-4 w-20 mb-2" />
												<div className="skeleton h-10 w-36 mb-2" />
												<div className="skeleton h-4 w-full" />
											</div>
											<div className="stat">
												<div className="skeleton h-4 w-20 mb-2" />
												<div className="skeleton h-10 w-36 mb-2" />
												<div className="skeleton h-4 w-full" />
											</div>
										</div>
									</div>
								))}
							</div>
						) : displayMarkets.length === 0 ? (
							<div className="p-8 text-center text-base-content/70 min-h-[200px] flex items-center justify-center">
								No markets found
							</div>
						) : (
							<div className="flex flex-col gap-4">
								{displayMarkets.map((market) => (
									<section
										key={`stats-preview-${market.conditionId}`}
									// className="card bg-base-300/30 border border-base-content/5 p-4 rounded-box"
									>

										<div className="stats stats-vertical lg:stats-horizontal shadow w-full bg-base-100 border border-base-content/10 overflow-x-auto">
											{market.outcomes.map((outcome) => {
												const outcomeMeta = getOutcomeMeta(
													outcome.outcome ?? "N/A",
												);
												const statsLoading = Boolean(
													marketStatsLoadingByCondition[market.conditionId],
												);
												const totalTrades = Number(outcome.total_trades || 0);
												const insiderTradeCount = Number(
													outcome.insider_trade_count || 0,
												);
												const noTradeData = totalTrades <= 0;
												const missingStats = !hasAllStats(outcome);

												return (
													<div
														key={`stats-${market.conditionId}-${String(outcome.outcome)}`}
														className="stat gap-2 min-w-[16rem] flex flex-col items-start justify-between"
													>
														{/* TOP SECTION: Headline & Badge */}
														<div className="w-full flex justify-between items-start gap-3">
															<h3
																className="text-sm font-bold text-base-content line-clamp-2 leading-snug"
																title={market.question}
															>
																{market.question}
															</h3>

															{/* Badge: Increased size (badge-sm -> badge-md) and font size */}
															<span
																className={`badge badge-md border font-bold rounded-md px-3 py-1 text-xs uppercase shrink-0 ${outcomeMeta.toneClass}`}
															>
																{outcomeMeta.label}
															</span>
														</div>

														{/* BOTTOM SECTION: Odds moved below headline */}
														<div className="mt-3 flex flex-col items-start leading-none">
															<span className="text-[10px] uppercase text-base-content/60 font-mono whitespace-nowrap mb-1">
																Current Odds
															</span>

															{/* Price: Reduced text size slightly (text-3xl -> text-2xl) */}
															<span className="text-accent text-xl md:text-2xl font-mono font-bold leading-none tracking-tight whitespace-nowrap">
																{renderMarketPrice(
																	Number(outcome.last_price || 0),
																	Boolean(market.closed || outcome.closed)
																)}
															</span>
														</div>

														<div className="stat-desc">
															<div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-2 text-[11px] md:text-xs font-mono">
																<div className="min-w-0 flex flex-col">
																	<span className="text-base-content/60 uppercase">
																		Trades
																	</span>
																	<span className="text-base-content/90 text-sm md:text-base">
																		{totalTrades.toLocaleString()}
																	</span>
																</div>
																<div className="min-w-0 flex flex-col">
																	<span className="text-base-content/60 uppercase">
																		Insider Trades
																	</span>
																	<span className="text-base-content/90 text-sm md:text-base">
																		{insiderTradeCount.toLocaleString()}
																	</span>
																</div>
																<div className="min-w-0 flex flex-col">
																	<span className="text-base-content/60 uppercase">
																		Volume
																	</span>
																	<span className="text-base-content/90 text-sm md:text-base">
																		$
																		{Number(outcome.volume || 0).toLocaleString(
																			undefined,
																			{
																				minimumFractionDigits: 0,
																				maximumFractionDigits: 0,
																			},
																		)}
																	</span>
																</div>
																<div className="min-w-0 flex flex-col">
																	<span className="text-base-content/60 uppercase">
																		Market Stats
																	</span>
																	<span className="text-base-content/90 text-sm md:text-base md:whitespace-nowrap">
																		{noTradeData ? (
																			<span className="text-base-content/60">
																				no trade data
																			</span>
																		) : missingStats && statsLoading ? (
																			<span className="flex items-center gap-1 opacity-70">
																				<span
																					className="loading loading-spinner loading-xs loading-optimized"
																					aria-hidden="true"
																				/>
																				loading...
																			</span>
																		) : (
																			`${formatMarketStat(outcome.mean)} / ${formatMarketStat(outcome.stdDev)} / ${formatMarketStat(outcome.p95)}`
																		)}
																	</span>
																</div>
															</div>
														</div>
													</div>
												);
											})}
										</div>
									</section>
								))}
							</div>
						)}
					</div>
					{isLoading && (
						<div className="absolute inset-0 rounded-box bg-base-100/75 backdrop-blur-[1px] border border-base-content/10 flex items-center justify-center z-10">
							<div className="flex flex-col items-center gap-2 text-center px-4">
								<pre className="text-[9px] md:text-[10px] leading-tight font-mono text-primary whitespace-pre">
									{LOADING_MARKETS_ASCII}
								</pre>
								<div className="flex items-center gap-2 text-xs font-mono text-base-content/70 uppercase">
									<span
										className="loading loading-spinner loading-xs loading-optimized"
										aria-hidden="true"
									/>
									loading market overview
								</div>
							</div>
						</div>
					)}
				</div>
				<div className="flex justify-between items-center p-2 mt-2 border-t border-base-content/10">
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
}
