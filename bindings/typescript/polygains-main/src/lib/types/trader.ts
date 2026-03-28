import type { PositionStats } from "./positions";

export interface TraderData {
	id: string;
	wallet?: string;
	tokenstats: Record<string, PositionStats>;
	userStats: {
		tradeVol: bigint;
		tradeCount: number;
		firstSeen: number;
		lastSeen?: number;
	};
}
