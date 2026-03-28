import { evmDecoder, evmPortalSource } from "@subsquid/pipes/evm";
import { exchangeEvents } from "@/lib/abi";
import { CONTRACTS, START_BLOCK } from "./lib/const";
import { parseOrder } from "./lib/parser";
import { PolymarketPipe } from "./services/pipe";

const main = async () => {
	const startBlock = START_BLOCK;
		const polyPipe = new PolymarketPipe();
	await evmPortalSource({
		portal: { url: "https://portal.sqd.dev/datasets/polygon-mainnet" },
	})
		.pipeComposite({
			events: evmDecoder({
				range: { from: startBlock },
				contracts: [CONTRACTS.EXCHANGE],
				events: {
					OrdersMatched: exchangeEvents.OrdersMatched,
				},
			}),
		})
		.pipe(({ events }) => events.OrdersMatched.map(parseOrder))
		.pipeTo(polyPipe);
};

main();
