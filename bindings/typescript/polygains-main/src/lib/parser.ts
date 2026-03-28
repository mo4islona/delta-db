import type { DecodedEvent } from "@subsquid/pipes/evm";
import { EVENT, SIDE } from "./types";

export const parseOrder = (
	order: DecodedEvent<{
		readonly takerOrderHash: string;
		readonly takerOrderMaker: string;
		readonly makerAssetId: bigint;
		readonly takerAssetId: bigint;
		readonly makerAmountFilled: bigint;
		readonly takerAmountFilled: bigint;
	}>,
) => {
	const block = order.block.number;
	const isBuy = order.event.takerAssetId === 0n;
	const shares = isBuy
		? order.event.makerAmountFilled
		: order.event.takerAmountFilled;
	const usdc = isBuy
		? order.event.takerAmountFilled
		: order.event.makerAmountFilled;
	const assetId = isBuy ? order.event.makerAssetId : order.event.takerAssetId;
	return {
		kind: EVENT.ORDER,
		trader: order.event.takerOrderMaker,
		assetId: assetId,
		side: isBuy ? SIDE.BUY : SIDE.SELL,
		shares: shares,
		usdc: usdc,
		block: block,
		logIndex: order.rawEvent.logIndex,
		transactionIndex: order.rawEvent.transactionIndex,
		timestamp: order.timestamp,
	};
};
