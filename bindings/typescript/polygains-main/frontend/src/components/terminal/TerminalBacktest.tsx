import type { TerminalIntroProps } from "@/types/terminal";

const INTRO_STEPS = [
    {
        label: "STEP 1: MONITOR",
        desc: "Scan every every market on Polymarket",
    },
    {
        label: "STEP 2: DISCOVER",
        desc: 'Detect unusual activity. New Wallets, no history and large bets',
    },
    {
        label: "STEP 3: FOLLOW",
        desc: "Get notified on new trades. Watch, copy, or reverse.",
    },
];

export function TerminalIntro({
    totalInsiders = 0,
    yesInsiders = 0,
    noInsiders = 0,
    insiderVolume = "0.00",
    backtestPnl = 0,
    backtestTotalBet = 0,
    backtestTrades = 0,
    backtestWins = 0,
    backtestLosses = 0,
    backtestSeries = [],
}: Omit<TerminalIntroProps, "text">) {
    const safeTotal = Math.max(totalInsiders, yesInsiders + noInsiders, 1);
    const yesShare = Math.round((yesInsiders / safeTotal) * 100);
    const noShare = Math.round((noInsiders / safeTotal) * 100);
    const series =
        backtestSeries.length > 1 ? backtestSeries : [0, Number(backtestPnl || 0)];
    const chartWidth = 520;
    const chartHeight = 220;
    const padX = 12;
    const padY = 12;
    const plotW = chartWidth - padX * 2;
    const plotH = chartHeight - padY * 2;
    const minY = Math.min(...series, 0);
    const maxY = Math.max(...series, 0);
    const rangeY = Math.max(maxY - minY, 1);
    const stepX = series.length > 1 ? plotW / (series.length - 1) : 0;
    const getY = (value: number) => padY + ((maxY - value) / rangeY) * plotH;
    const zeroY = getY(0);
    const linePoints = series
        .map((value, index) => `${padX + index * stepX},${getY(value)}`)
        .join(" ");
    const areaPoints = `${padX},${zeroY} ${linePoints} ${padX + plotW},${zeroY}`;
    const lastX = padX + (series.length - 1) * stepX;
    const lastY = getY(series[series.length - 1] ?? 0);
    const winRate =
        backtestTrades > 0 ? Math.round((backtestWins / backtestTrades) * 100) : 0;

    return (
        <div className="mb-8">
            <div className="grid grid-cols-12 gap-4">
                <div className="col-span-12 sm:col-span-12 md:col-span-8">
                    <div className="card bg-base-300 shadow-xl border-l-4 border-primary mb-0 font-mono text-xs md:text-sm h-full">
                        <div className="card-body p-5 md:p-6">
                            <h3 className="text-primary uppercase text-xs mb-3">
                                <span className="text-primary mr-2">$</span> RUN EXPLAIN-DETECTION
                            </h3>
                            <ul className="flex flex-col gap-4 mb-4">
                                {INTRO_STEPS.map((step, i) => (
                                    <li key={step.label} className="flex flex-col sm:grid sm:grid-cols-12 sm:gap-2 leading-snug">
                                        {/* Mobile: Full width header 
                Desktop: 4/12 columns 
            */}
                                        <span className="text-primary font-bold sm:col-span-4 uppercase text-[10px] tracking-wider sm:normal-case sm:text-sm">
                                            {step.label}
                                        </span>

                                        {/* Mobile: Full width description 
                Desktop: 8/12 columns 
            */}
                                        <div className="text-base-content/70 sm:col-span-8 text-sm">
                                            {step.desc}
                                            {i === INTRO_STEPS.length - 1 && (
                                                <span
                                                    className="inline-block w-1.5 h-3 bg-accent align-middle ml-1"
                                                    style={{ animation: "cursor-blink 1s step-end infinite" }}
                                                />
                                            )}
                                        </div>
                                    </li>
                                ))}
                            </ul>
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                                <div className="rounded-box border border-base-content/10 px-3 py-2">
                                    <div className="text-[10px] uppercase tracking-wider text-base-content/60">
                                        Insiders
                                    </div>
                                    <div className="text-2xl font-semibold text-base-content leading-none mt-1">
                                        {totalInsiders.toLocaleString()}
                                    </div>
                                    <div className="text-[11px] text-base-content/60 mt-1">
                                        YES {yesShare}% / NO {noShare}%
                                    </div>
                                </div>
                                <div className="rounded-box border border-base-content/10 px-3 py-2">
                                    <div className="text-[10px] uppercase tracking-wider text-base-content/60">
                                        Tracked volume
                                    </div>
                                    <div className="text-2xl font-semibold text-base-content leading-none mt-1">
                                        ${insiderVolume}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                {/* <div className="col-span-6 sm:col-span-6">
                    <div className="card bg-base-300 shadow-xl border-l-4 border-info font-mono text-xs md:text-sm h-full">
                        <div className="card-body p-5 md:p-6">
                            <h3 className="text-info uppercase text-xs mb-3">
                                <span className="text-info mr-2">$</span> RENDER LIVE-SIGNAL-CHART
                            </h3>
                            <div className="flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-base-content/70 mb-2">
                                <span>PNL {formatPnL(backtestPnl)}</span>
                                <span>TRADES {backtestTrades.toLocaleString()}</span>
                                <span>WIN {winRate}%</span>
                                <span>
                                    W/L {backtestWins}/{backtestLosses}
                                </span>
                                <span>CAP ${backtestTotalBet.toFixed(2)}</span>
                            </div>
                            <div className="rounded-box border border-base-content/10 p-2 bg-base-200/40">
                                <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} className="w-full h-56 md:h-64">
                                    <defs>
                                        <linearGradient id="introPnlFill" x1="0" y1="0" x2="0" y2="1">
                                            <stop offset="0%" stopColor="oklch(72% 0.19 149)" stopOpacity="0.28" />
                                            <stop offset="100%" stopColor="oklch(72% 0.19 149)" stopOpacity="0.04" />
                                        </linearGradient>
                                    </defs>
                                    <line
                                        x1={padX}
                                        y1={zeroY}
                                        x2={padX + plotW}
                                        y2={zeroY}
                                        stroke="oklch(70% 0.01 240 / 0.35)"
                                        strokeDasharray="4 5"
                                        strokeWidth="1"
                                    />
                                    <polyline
                                        points={areaPoints}
                                        fill="url(#introPnlFill)"
                                        stroke="none"
                                    />
                                    <polyline
                                        points={linePoints}
                                        fill="none"
                                        stroke="oklch(72% 0.19 149)"
                                        strokeWidth="2.2"
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                    />
                                    <circle
                                        cx={lastX}
                                        cy={lastY}
                                        r="3"
                                        fill="oklch(72% 0.19 149)"
                                    />
                                </svg>
                            </div>
                        </div>
                    </div>
                </div> */}

            </div>
        </div>
    );
}