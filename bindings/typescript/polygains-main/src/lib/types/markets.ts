export type FlatMarket = {
	conditionId: string;
	question: string;
	slug: string;
	tags: string[];
	category: string | null;
	active: boolean;
	closed: boolean;
	tokenCount: number;
	[key: `token_${number}_id`]: string | undefined;
	[key: `token_${number}_outcome`]: string | undefined;
	[key: `token_${number}_winner`]: boolean | undefined;
};

export type ClobToken = {
	token_id?: string;
	outcome?: string;
	winner?: unknown;
};

export type ClobMarket = {
	condition_id?: string;
	question?: string;
	description?: string;
	market_slug?: string;
	tags?: string[];
	active?: boolean;
	closed?: boolean;
	tokens?: ClobToken[];
};

export type ClobResponse = {
	data?: ClobMarket[];
	next_cursor?: string;
};
