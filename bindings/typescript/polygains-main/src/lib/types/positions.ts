export type PositionStats = {
	firstSeen: number;
	lastSeen: number;
	volume: number;
	trades: number;
	sumPrice: number;
	sumPriceSq: number;
};

export type PersistTask = {
	accountHash: number;
	detectedAt: number;
	positions: Record<string, PositionStats>;
};

export type AggregatedPosition = PositionStats & {
	accountHash: number;
	tokenId: string;
	detectedAt: number;
};

export type AccountAddressTask = {
	accountHash: number;
	walletAddress: string;
	seenAt: number;
};
