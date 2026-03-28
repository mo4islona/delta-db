import { useEffect, useState } from "react";
import { fetchAlerts, fetchCategories } from "../api/terminalApi";
import type { AlertItem } from "../types/terminal";

function formatMoney(n: number) {
	return n.toLocaleString(undefined, {
		style: "currency",
		currency: "USD",
		maximumFractionDigits: 0,
	});
}

function formatPrice(n: number) {
	return n.toLocaleString(undefined, {
		style: "currency",
		currency: "USD",
		minimumFractionDigits: 2,
		maximumFractionDigits: 2,
	});
}

function timeAgo(alertTime: number) {
	const then = alertTime * 1000;
	const now = Date.now();
	const s = Math.max(1, Math.floor((now - then) / 1000));
	const m = Math.floor(s / 60);
	const h = Math.floor(m / 60);
	const d = Math.floor(h / 24);
	if (d >= 1) return `${d}d ago`;
	if (h >= 1) return `${h}h ago`;
	if (m >= 1) return `${m}m ago`;
	return `${s}s ago`;
}

export function MainV2Page() {
	const [alerts, setAlerts] = useState<AlertItem[]>([]);
	const [loading, setLoading] = useState(true);
	const [categories, setCategories] = useState<string[]>(["ALL"]);
	const [activeCategory, setActiveCategory] = useState("ALL");

	useEffect(() => {
		fetchCategories().then((cats) => setCategories(["ALL", ...cats]));
	}, []);

	useEffect(() => {
		let mounted = true;
		setLoading(true);
		const load = async () => {
			try {
				const cat = activeCategory === "ALL" ? undefined : activeCategory;
				const res = await fetchAlerts(1, 50, cat);
				if (mounted) {
					setAlerts(res.data);
					setLoading(false);
				}
			} catch (err) {
				console.error("Failed to fetch alerts", err);
				if (mounted) setLoading(false);
			}
		};
		load();
		const interval = setInterval(load, 30000);
		return () => {
			mounted = false;
			clearInterval(interval);
		};
	}, [activeCategory]);

	return (
		<div className="min-h-screen w-full bg-[#06070a] text-white">
			{/* Background Gradients */}
			<div className="pointer-events-none fixed inset-x-0 top-0 h-96 bg-[radial-gradient(1000px_400px_at_50%_-50px,rgba(16,185,129,0.15),transparent_70%)]" />
			<div className="pointer-events-none fixed inset-x-0 top-0 h-64 bg-[radial-gradient(800px_300px_at_50%_-100px,rgba(99,102,241,0.15),transparent_60%)]" />

			<div className="relative w-full px-0 md:px-8 py-6 pb-20 md:pb-8">
				{/* Header */}
				<div className="flex flex-col md:flex-row md:items-center justify-between gap-6 mb-8 px-4 md:px-0">
					<div className="flex items-center gap-4">
						<div className="relative h-12 w-12 flex items-center justify-center rounded-2xl bg-white/5 ring-1 ring-white/10 backdrop-blur-xl shadow-lg shadow-emerald-500/5">
							<svg
								viewBox="0 0 24 24"
								className="h-6 w-6 text-emerald-400"
								fill="none"
								stroke="currentColor"
								strokeWidth="2"
							>
								<title>Insider Alerts Icon</title>
								<path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
							</svg>
							{/* Pulse effect */}
							<div className="absolute inset-0 rounded-2xl ring-1 ring-emerald-500/20 animate-pulse" />
						</div>
						<div>
							<h1 className="text-xl font-bold tracking-tight text-white/90">
								Insider Alerts
							</h1>
							<div className="flex items-center gap-3 text-xs font-medium text-white/70 mt-1">
								<span>{alerts.length} signals</span>
								<span className="w-1 h-1 rounded-full bg-white/20" />
								<span className="flex items-center gap-1.5 text-emerald-400/90">
									<span className="relative flex h-2 w-2">
										<span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
										<span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
									</span>
									Live Feed
								</span>
							</div>
						</div>
					</div>

					{/* Category Pills */}
					<div className="w-full md:w-auto overflow-x-auto pb-2 -mx-4 px-4 md:mx-0 md:px-0 no-scrollbar">
						<div className="flex gap-2">
							{categories.map((cat) => (
								<button
									type="button"
									key={cat}
									className={`btn btn-sm border-0 font-medium tracking-wide transition-all duration-200 whitespace-nowrap min-h-[44px] ${activeCategory === cat
										? "bg-white/10 text-white shadow-sm ring-1 ring-white/20"
										: "bg-transparent text-white/70 hover:text-white/70 hover:bg-white/5"
										}`}
									onClick={() => setActiveCategory(cat)}
									aria-pressed={activeCategory === cat}
									aria-label={`Filter by category ${cat}`}
								>
									{cat}
								</button>
							))}
						</div>
					</div>
				</div>

				{/* Loading State */}
				{loading && (
					<div className="flex justify-center py-32">
						<span className="loading loading-bars loading-lg text-emerald-500/50"></span>
					</div>
				)}

				{/* MOBILE VIEW: Glass List */}
				{!loading && (
					<div className="md:hidden space-y-3">
						<div className="px-2 pb-1 text-[10px] font-bold uppercase tracking-widest text-white/60">
							Recent Activity
						</div>
						<ul className="flex flex-col gap-3">
							{alerts.map((item, idx) => {
								const isYes = item.outcome?.toLowerCase() === "yes";
								return (
									<li
										key={`${item.conditionId}-${idx}`}
										className="relative flex items-center gap-4 p-4 rounded-2xl bg-white/5 ring-1 ring-white/10 backdrop-blur-md active:scale-[0.98] transition-all duration-200"
									>
										{/* Icon */}
										<div
											className={`flex-none w-11 h-11 rounded-xl flex items-center justify-center font-black text-[10px] tracking-wide shadow-lg ${isYes
												? "bg-gradient-to-br from-emerald-500/20 to-teal-500/20 text-emerald-400 ring-1 ring-emerald-500/20 shadow-emerald-900/20"
												: "bg-gradient-to-br from-rose-500/20 to-pink-500/20 text-rose-400 ring-1 ring-rose-500/20 shadow-rose-900/20"
												}`}
										>
											{item.outcome?.toUpperCase().slice(0, 3)}
										</div>

										{/* Content */}
										<div className="flex-grow min-w-0 flex flex-col gap-1">
											<div className="text-sm font-medium leading-snug text-white/90 line-clamp-2">
												{item.question}
											</div>
											<div className="flex items-center gap-2 text-[11px] font-medium text-white/70">
												<span className="font-mono text-white/70">
													{item.user?.slice(0, 6)}...{item.user?.slice(-4)}
												</span>
												<span className="w-0.5 h-0.5 rounded-full bg-white/20" />
												<span>{timeAgo(item.alert_time)}</span>
											</div>
										</div>

										{/* Value */}
										<div className="flex-none text-right flex flex-col justify-center min-w-[70px]">
											<div
												className={`font-mono font-bold text-sm ${isYes ? "text-emerald-400" : "text-rose-400"}`}
											>
												{formatMoney(item.volume)}
											</div>
											<div className="text-[10px] font-mono text-white/60">
												@{formatPrice(item.price)}
											</div>
										</div>
									</li>
								);
							})}
						</ul>
					</div>
				)}

				{/* DESKTOP VIEW: Full Width Glass Table */}
				{!loading && (
					<div className="hidden md:block overflow-hidden rounded-2xl bg-white/5 ring-1 ring-white/10 backdrop-blur-xl shadow-2xl shadow-black/50">
						<table className="table w-full border-collapse">
							<thead>
								<tr className="border-b border-white/5 bg-white/[0.02]">
									<th className="py-5 px-6 text-[10px] font-bold uppercase tracking-widest text-white/60">
										Market
									</th>
									<th className="py-5 px-6 text-left text-[10px] font-bold uppercase tracking-widest text-white/60">
										Side
									</th>
									<th className="py-5 px-6 text-right text-[10px] font-bold uppercase tracking-widest text-white/60">
										Price
									</th>
									<th className="py-5 px-6 text-right text-[10px] font-bold uppercase tracking-widest text-white/60">
										Volume
									</th>
									<th className="py-5 px-6 text-right text-[10px] font-bold uppercase tracking-widest text-white/60">
										Time
									</th>
									<th className="py-5 px-6 text-center text-[10px] font-bold uppercase tracking-widest text-white/60">
										Lookup
									</th>
								</tr>
							</thead>
							<tbody className="divide-y divide-white/5">
								{alerts.map((item, idx) => {
									const isYes = item.outcome?.toLowerCase() === "yes";
									return (
										<tr
											key={`${item.conditionId}-${idx}`}
											className="group hover:bg-white/[0.04] transition-colors duration-150"
										>
											<td className="py-4 px-6 max-w-[400px]">
												<div className="font-medium text-sm text-white/90 truncate group-hover:text-emerald-300 transition-colors">
													{item.question}
												</div>
												<div className="text-[10px] font-mono text-white/60 truncate mt-1">
													{item.conditionId}
												</div>
											</td>
											<td className="py-4 px-6 text-left">
												<div
													className={`inline-flex items-center px-2.5 py-1 rounded-lg text-[10px] font-black tracking-wider uppercase ring-1 ring-inset ${isYes
														? "bg-emerald-500/10 text-emerald-400 ring-emerald-500/20"
														: "bg-rose-500/10 text-rose-400 ring-rose-500/20"
														}`}
												>
													{item.outcome}
												</div>
											</td>
											<td className="py-4 px-6 text-right">
												<span className="font-mono text-sm font-medium text-white/80">
													{formatPrice(item.price)}
												</span>
											</td>
											<td className="py-4 px-6 text-right">
												<span
													className={`font-mono text-sm font-bold ${isYes ? "text-emerald-400" : "text-rose-400"}`}
												>
													{formatMoney(item.volume)}
												</span>
											</td>
											<td className="py-4 px-6 text-right">
												<span className="text-xs font-medium text-white/70 tabular-nums">
													{timeAgo(item.alert_time)}
												</span>
											</td>
											<td className="py-4 px-6 text-center">
												<button
													type="button"
													className="btn btn-ghost btn-sm text-white/80 hover:text-white hover:bg-white/10 min-w-[44px] min-h-[44px]"
													aria-label={`Lookup trader ${item.user}`}
													title={`Lookup trader ${item.user}`}
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
												</button>
											</td>
										</tr>
									);
								})}
							</tbody>
						</table>
					</div>
				)}

				{!loading && alerts.length === 0 && (
					<div className="flex flex-col items-center justify-center py-24 opacity-40">
						<div className="p-4 rounded-full bg-white/5 mb-4">
							<svg
								className="w-8 h-8 text-white/70"
								fill="none"
								viewBox="0 0 24 24"
								stroke="currentColor"
							>
								<title>No Results Icon</title>
								<path
									strokeLinecap="round"
									strokeLinejoin="round"
									strokeWidth="1.5"
									d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
								/>
							</svg>
						</div>
						<p className="text-sm font-medium text-white/60">
							No alerts match your filter
						</p>
					</div>
				)}
			</div>
		</div>
	);
}
