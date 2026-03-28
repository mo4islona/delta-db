export enum EVENT {
	ORDER,
	CONDITION,
}

export enum SIDE {
	BUY,
	SELL,
}

export interface WindowBufferItem {
	id: string;
	userStats: {
		firstSeen: number;
	};
}

export type ParsedOrder = {
	trader: string;
	assetId: string | number | bigint;
	usdc: number | bigint;
	shares: number | bigint;
	side: SIDE;
	timestamp: number;
};
