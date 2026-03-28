// TODO MUST STAY WITH INT 32 NUMBER
// MAYBE DATAVIEW
export const hashWallet = (wallet: string): number =>
	Bun.hash.xxHash32(wallet) | 0; // Ensure signed 32-bit integer
