-- Raw Uniswap swap events with token addresses
CREATE TABLE swaps (
    block_number UInt64,
    block_time   DateTime,
    tx_hash      String,
    network      String,
    pool         String,
    token0       String,
    token1       String,
    sender       String,
    amount0      Float64,
    amount1      Float64
);

-- Shared pricing logic: stablecoin constants and USD price resolution.
-- Used by both swap_prices and wallet_pnl reducers.
CREATE MODULE pricing LANGUAGE LUA AS $$
    local M = {}

    M.USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    M.USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    M.WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    function M.is_stablecoin(addr)
        return addr == M.USDT or addr == M.USDC
    end

    -- Resolve USD pricing from a swap.
    -- Returns { target, price_usd, vol_usd, eth_usd, base_delta } or nil.
    function M.resolve(token0, token1, amount0, amount1, eth_usd)
        if amount0 == 0 then return nil end

        local ratio = math.abs(amount1 / amount0)
        local t0 = string.lower(token0)
        local t1 = string.lower(token1)
        local r = { eth_usd = eth_usd }

        if M.is_stablecoin(t1) then
            r.target = t0
            r.price_usd = ratio
            r.vol_usd = math.abs(amount1)
            r.base_delta = amount0
            if t0 == M.WETH then r.eth_usd = ratio end

        elseif M.is_stablecoin(t0) then
            r.target = t1
            r.price_usd = ratio > 0 and (1 / ratio) or 0
            r.vol_usd = math.abs(amount0)
            r.base_delta = amount1
            if t1 == M.WETH then r.eth_usd = r.price_usd end

        elseif t1 == M.WETH then
            r.target = t0
            r.price_usd = ratio * eth_usd
            r.vol_usd = math.abs(amount1) * eth_usd
            r.base_delta = amount0

        elseif t0 == M.WETH then
            r.target = t1
            r.price_usd = ratio > 0 and (eth_usd / ratio) or 0
            r.vol_usd = math.abs(amount0) * eth_usd
            r.base_delta = amount1

        else
            return nil
        end

        if r.price_usd <= 0 then return nil end
        return r
    end

    return M
$$;

-- Price oracle: computes USD price for each swap.
-- Groups by network (constant) to maintain a global ETH/USD reference.
-- Cross-prices tokens via ETH when no direct stablecoin pair exists.
CREATE REDUCER swap_prices
SOURCE swaps
GROUP BY network
STATE (
    eth_usd Float64 DEFAULT 0
)
REQUIRE pricing
LANGUAGE lua
PROCESS $$
    local p = pricing.resolve(row.token0, row.token1, row.amount0, row.amount1, state.eth_usd)
    if not p then return end

    state.eth_usd = p.eth_usd
    emit.pool = row.pool
    emit.token = p.target
    emit.block_time = row.block_time
    emit.price_usd = p.price_usd
    emit.volume_usd = p.vol_usd
$$;

-- OHLC 5-minute candles per pool, in USD
CREATE MATERIALIZED VIEW candles_5m AS
SELECT
    pool,
    toStartOfInterval(block_time, INTERVAL 5 MINUTE) AS window_start,
    first(price_usd)  AS open,
    max(price_usd)    AS high,
    min(price_usd)    AS low,
    last(price_usd)   AS close,
    sum(volume_usd)   AS volume,
    count()            AS trade_count
FROM swap_prices
GROUP BY pool, window_start;

-- Wallet PnL tracker: USD-denominated position tracking.
-- Groups by network (constant) to maintain global price + position state.
CREATE REDUCER wallet_pnl
SOURCE swaps
GROUP BY network
STATE (
    eth_usd       Float64 DEFAULT 0,
    positions    JSON    DEFAULT '{}'
)
REQUIRE pricing
LANGUAGE lua
PROCESS $$
    local p = pricing.resolve(row.token0, row.token1, row.amount0, row.amount1, state.eth_usd)
    if not p then return end

    state.eth_usd = p.eth_usd

    -- Track position and realized PnL (cost-basis accounting)
    local pos_key = row.sender .. ":" .. p.target
    local pos = state.positions[pos_key]
    if not pos then pos = { balance = 0, cost_usd = 0 } end

    local pnl = 0
    if p.base_delta > 0 then
        -- Buy: accumulate cost basis
        pos.balance = pos.balance + p.base_delta
        pos.cost_usd = pos.cost_usd + p.base_delta * p.price_usd
    elseif p.base_delta < 0 and pos.balance > 0 then
        -- Sell: realize PnL vs average cost
        local sold = math.abs(p.base_delta)
        local avg_cost = pos.cost_usd / pos.balance
        pnl = sold * (p.price_usd - avg_cost)
        pos.balance = pos.balance - sold
        pos.cost_usd = pos.cost_usd - sold * avg_cost
    end

    state.positions[pos_key] = pos

    emit.sender = row.sender
    emit.pool = row.pool
    emit.realized_pnl = pnl
    emit.position = pos.balance
$$;

-- Aggregate PnL per wallet per pool
CREATE MATERIALIZED VIEW wallet_summary AS
SELECT
    sender,
    pool,
    sum(realized_pnl) AS total_pnl,
    last(position)    AS current_position,
    count()           AS trade_count
FROM wallet_pnl
GROUP BY sender, pool;
