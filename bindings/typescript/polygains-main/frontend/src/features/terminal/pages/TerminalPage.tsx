import {
	AlertsSection,
	DetectionSection,
	EmailSignup,
	GlobalStatsSection,
	LiveTrackerCards,
	LiveTrackerControls,
	MarketsSection,
	MarketsOverviewStatsPreview,
	SimulationHeader,
	TerminalBanner,
	TerminalFooter,
	TerminalHeader,
	TerminalIntro,
} from "../components/TerminalSections";
import { NotificationAlert } from "../../../components/NotificationAlert";
import { useTerminalController } from "../controller/useTerminalController";

export function TerminalPage() {
	const vm = useTerminalController();

	return (
		<>


			<div className="terminal-app">
				<main className="container mx-auto max-w-6xl px-4">
					<TerminalHeader
						currentBlock={vm.currentBlockText}
						syncLabel={vm.syncState.label}
						syncHealthy={vm.syncState.healthy}
					/>

					<NotificationAlert />
					<TerminalIntro
						totalInsiders={vm.detection.totalInsiders}
						yesInsiders={vm.detection.yesInsiders}
						noInsiders={vm.detection.noInsiders}
						insiderVolume={vm.detection.insiderVolume}
						backtestPnl={vm.tracker.realizedPnL}
						backtestTotalBet={vm.tracker.totalBet}
						backtestTrades={vm.tracker.liveTrades}
						backtestWins={vm.tracker.liveWins}
						backtestLosses={vm.tracker.liveLosses}
						backtestSeries={vm.introBacktestPnlSeries}
					/>
					<AlertsSection
						rows={vm.alertsRows}
						pagination={vm.alertsPagination}
						selectedCategory={vm.selectedCategory}
						selectedWinnerFilter={vm.selectedWinnerFilter}
						categoryOptions={vm.categoryOptions}
						allCategoryOptions={vm.allCategoryOptions}
						isLoading={vm.alertsLoading}
						onPrev={() => vm.changeAlertsPage(-1)}
						onNext={() => vm.changeAlertsPage(1)}
						onCategoryChange={vm.setCategory}
						onWinnerFilterChange={vm.setWinnerFilter}
					/>

					<DetectionSection
						totalInsiders={vm.detection.totalInsiders}
						yesInsiders={vm.detection.yesInsiders}
						noInsiders={vm.detection.noInsiders}
						insiderVolume={vm.detection.insiderVolume}
					/>

					<MarketsOverviewStatsPreview
						markets={vm.markets}
						pagination={vm.marketsPagination}
						isLoading={vm.marketsLoading}
						marketStatsLoadingByCondition={vm.marketStatsLoadingByCondition}
						onPrev={() => vm.changeMarketsPage(-1)}
						onNext={() => vm.changeMarketsPage(1)}
					/>

					<GlobalStatsSection
						accounts={vm.globalStats.accounts}
						markets={vm.globalStats.markets}
						trades={vm.globalStats.trades}
						activePositions={vm.globalStats.activePositions}
					/>

					<div className="mt-8 mb-2">
						<h2 className="text-xs font-bold text-base-content/70 uppercase tracking-wider font-mono">
							COPY_TRADER
						</h2>
					</div>
					<SimulationHeader />

					<LiveTrackerControls
						minPrice={vm.liveControls.minPrice}
						maxPrice={vm.liveControls.maxPrice}
						onlyBetOnce={vm.liveControls.onlyBetOnce}
						betOneDollarPerTrade={vm.liveControls.betOneDollarPerTrade}
						disabled={vm.liveControls.disabled}
						selectedStrategies={vm.liveControls.selectedStrategies}
						selectedSides={vm.liveControls.selectedSides}
						onMinPriceChange={vm.liveControls.onMinPriceChange}
						onMaxPriceChange={vm.liveControls.onMaxPriceChange}
						onOnlyBetOnceChange={vm.liveControls.onOnlyBetOnceChange}
						onBetOneDollarPerTradeChange={
							vm.liveControls.onBetOneDollarPerTradeChange
						}
						onStrategyChange={vm.liveControls.onStrategyChange}
						onSideToggle={vm.liveControls.onSideToggle}
					/>



					<LiveTrackerCards
						totalBet={vm.tracker.totalBet}
						openInterest={vm.tracker.openInterest}
						realizedPnL={vm.tracker.realizedPnL}
						liveTrades={vm.tracker.liveTrades}
						liveWins={vm.tracker.liveWins}
						liveLosses={vm.tracker.liveLosses}
						alertsPage={vm.tracker.alertsPage}
						alertsTotalPages={vm.tracker.alertsTotalPages}
						alertsFilledThroughPage={vm.tracker.alertsFilledThroughPage}
						backtestCanContinue={vm.tracker.backtestCanContinue}
						backtestRunning={vm.tracker.backtestRunning}
						onRunBacktest={vm.tracker.onRunBacktest}
					/>

					<EmailSignup />

					<TerminalBanner currentBlock={vm.currentBlockText} />
					<TerminalFooter />
				</main>
			</div>
		</>
	);
}
