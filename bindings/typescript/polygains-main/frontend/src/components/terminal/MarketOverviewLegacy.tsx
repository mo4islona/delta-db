export function MarketsSection({
	markets,
	pagination,
	isLoading = false,
	marketStatsLoadingByCondition = {},
	onPrev,
	onNext,
}: MarketsSectionProps) {
	return (
		<>
			<h2 className="text-xs font-bold text-base-content/80 uppercase tracking-wider mb-4 mt-8 section-header-min-height flex items-center">
				MARKETS_OVERVIEW
			</h2>
			<div className="rounded-box border border-base-content/10 mb-8 p-2 markets-table-container contain-paint">
				{isLoading ? (
					<div className="flex flex-col gap-4">
						{[...Array(5)].map((_, i) => (
							<div
								key={`market-skel-${i}`}
								className="card bg-base-300/30 border border-base-content/5 p-4 rounded-box"
							>
								<div className="skeleton h-5 w-full max-w-[600px] mb-3" />
								<div className="w-full rounded-lg border border-base-content/5 bg-base-100/50 p-2">
									<div className="skeleton h-4 w-full mb-2" />
									<div className="skeleton h-4 w-full" />
								</div>
							</div>
						))}
					</div>
				) : markets.length === 0 ? (
					<div className="p-8 text-center text-base-content/70 min-h-[200px] flex items-center justify-center">
						No markets found
					</div>
				) : (
					<div className="flex flex-col gap-4">
						{markets.map((market) => (
							<section
								key={market.conditionId}
								className="card bg-base-300/30 border border-base-content/5 p-4 rounded-box card-optimized"
							>
								<h3
									className="text-sm font-bold text-base-content mb-3 line-clamp-2"
									title={market.question}
								>
									{market.question}
								</h3>
								<div className="w-full rounded-lg border border-base-content/5 bg-base-100/50 overflow-x-auto scrollbar-thin">
									<table className="table table-xs w-full table-fixed min-w-[700px] md:min-w-full">
										<thead>
											<tr className="bg-base-200 text-base-content/70 uppercase">
												<th className="w-[12%] md:w-[15%]">Outcome</th>
												<th className="hidden md:table-cell md:w-[12%]">
													Trades
												</th>
												<th className="hidden md:table-cell md:w-[15%]">
													Insider Trades
												</th>
												<th className="w-[18%] md:w-[15%]">Volume</th>
												<th className="w-[20%] md:w-[15%]">Current Odds</th>
												<th className="w-[50%] md:w-[28%]">
													<div className="flex items-center gap-1">
														<span>Market Stats (ø / std / P95)</span>
													</div>
												</th>
											</tr>
										</thead>
										<tbody>
											{market.outcomes.map((outcome, index) => {
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
													<tr
														key={`${market.conditionId}-${String(outcome.outcome)}`}
														className={index % 2 === 1 ? "bg-base-200/50" : ""}
													>
														<td>
															<span
																className={`badge badge-sm font-bold border rounded-sm px-2 py-0.5 text-[10px] uppercase ${outcomeMeta.toneClass}`}
															>
																{outcomeMeta.label}
															</span>
														</td>
														<td className="hidden md:table-cell font-mono tabular-nums text-base-content/90">
															{totalTrades.toLocaleString()}
														</td>
														<td className="hidden md:table-cell font-mono tabular-nums text-base-content/90">
															{insiderTradeCount.toLocaleString()}
														</td>
														<td className="font-mono tabular-nums text-base-content/90">
															$
															{Number(outcome.volume || 0).toLocaleString(
																undefined,
																{
																	minimumFractionDigits: 0,
																	maximumFractionDigits: 0,
																},
															)}
														</td>
														<td className="font-mono font-bold text-accent">
															{renderMarketPrice(
																Number(outcome.last_price || 0),
																Boolean(market.closed || outcome.closed),
															)}
														</td>
														<td className="font-mono text-base-content/80 text-[10px] whitespace-nowrap">
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
														</td>
													</tr>
												);
											})}
										</tbody>
									</table>
								</div>
							</section>
						))}
					</div>
				)}
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