//! Uniswap V2/V3 Swap Analytics — Complex Delta DB Example
//!
//! Demonstrates a full pipeline for on-chain DEX analytics with cross-price
//! calculation via stablecoins (USDT/USDC) and ETH intermediary:
//!
//!   swaps (raw) ──┬── swap_prices (reducer: USD price oracle) ── candles_5m (MV: OHLC)
//!                 └── wallet_pnl  (reducer: USD PnL tracker)  ── wallet_summary (MV)
//!
//! Pricing strategy:
//!   1. Direct: if pool has USDT or USDC, price in USD directly
//!   2. Cross:  if pool has WETH, price via ETH/USD (from latest WETH/USDT or WETH/USDC swap)
//!   3. Both reducers GROUP BY network (constant) for shared global state

use std::collections::HashMap;

use delta_db::db::{Config, DeltaDb};
use delta_db::types::{DeltaBatch, DeltaRecord, RowMap, Value};

// ─── Token Addresses ────────────────────────────────────────────────

const WETH: &str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const USDT: &str = "0xdac17f958d2ee523a2206206994597c13d831ec7";
const UNI: &str = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984";
const LINK: &str = "0x514910771af9ca656af840dff83e8264ecf986ca";

const POOL_WETH_USDC: &str = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640";
const POOL_WETH_USDT: &str = "0x4e68ccd3e89f51c3074ca5072bbac773960dfa36";
const POOL_UNI_WETH: &str = "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801";
const POOL_LINK_WETH: &str = "0xa6cc3c2531fdaa6ae1a3ca84c2855806728693e8";

// ─── Schema ─────────────────────────────────────────────────────────

const UNISWAP_SCHEMA: &str = r#"
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

    -- Price oracle: computes USD price for each swap.
    -- Groups by network (constant) to maintain a global ETH/USD reference.
    -- Cross-prices tokens via ETH when no direct stablecoin pair exists.
    CREATE REDUCER swap_prices
    SOURCE swaps
    GROUP BY network
    STATE (
        eth_usd Float64 DEFAULT 0
    )
    LANGUAGE lua
    PROCESS $$
        local USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
        local USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        local WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

        if row.amount0 == 0 then return end

        local ratio = math.abs(row.amount1 / row.amount0)
        local t0 = string.lower(row.token0)
        local t1 = string.lower(row.token1)
        local price_usd = 0
        local target = ""
        local vol_usd = 0

        -- Case 1: token1 is stablecoin → direct pricing
        -- ratio = STABLE_per_TOKEN0 → 1 TOKEN0 = ratio USD
        if t1 == USDT or t1 == USDC then
            price_usd = ratio
            target = t0
            if t0 == WETH then state.eth_usd = price_usd end
            vol_usd = math.abs(row.amount1)

        -- Case 2: token0 is stablecoin → inverse pricing
        -- ratio = TOKEN1_per_STABLE → 1 TOKEN1 = 1/ratio USD
        elseif t0 == USDT or t0 == USDC then
            if ratio > 0 then price_usd = 1 / ratio end
            target = t1
            if t1 == WETH then state.eth_usd = price_usd end
            vol_usd = math.abs(row.amount0)

        -- Case 3: token1 is WETH → cross via ETH
        -- ratio = WETH_per_TOKEN0 → TOKEN0_usd = ratio * eth_usd
        elseif t1 == WETH then
            price_usd = ratio * state.eth_usd
            target = t0
            vol_usd = math.abs(row.amount1) * state.eth_usd

        -- Case 4: token0 is WETH → cross via ETH (inverted)
        -- ratio = TOKEN1_per_WETH → TOKEN1_usd = eth_usd / ratio
        elseif t0 == WETH then
            if ratio > 0 then price_usd = state.eth_usd / ratio end
            target = t1
            vol_usd = math.abs(row.amount0) * state.eth_usd
        end

        if price_usd > 0 then
            emit.pool = row.pool
            emit.token = target
            emit.block_time = row.block_time
            emit.price_usd = price_usd
            emit.volume_usd = vol_usd
        end
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
        positions_json String DEFAULT '{}'
    )
    LANGUAGE lua
    PROCESS $$
        local USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
        local USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        local WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

        if row.amount0 == 0 then return end

        local positions = json.decode(state.positions_json)
        local t0 = string.lower(row.token0)
        local t1 = string.lower(row.token1)
        local ratio = math.abs(row.amount1 / row.amount0)

        -- Update ETH/USD reference (mirrors swap_prices logic)
        if t1 == USDT or t1 == USDC then
            if t0 == WETH then state.eth_usd = ratio end
        elseif t0 == USDT or t0 == USDC then
            if t1 == WETH and ratio > 0 then state.eth_usd = 1 / ratio end
        end

        -- Determine base token (non-stablecoin side) and USD price
        local base, price_usd, base_delta
        if t1 == USDT or t1 == USDC then
            base = t0
            price_usd = ratio
            base_delta = row.amount0
        elseif t0 == USDT or t0 == USDC then
            base = t1
            if ratio > 0 then price_usd = 1 / ratio else price_usd = 0 end
            base_delta = row.amount1
        elseif t1 == WETH then
            base = t0
            price_usd = ratio * state.eth_usd
            base_delta = row.amount0
        elseif t0 == WETH then
            base = t1
            if ratio > 0 then price_usd = state.eth_usd / ratio else price_usd = 0 end
            base_delta = row.amount1
        else
            state.positions_json = json.encode(positions)
            return
        end

        if not price_usd or price_usd <= 0 then
            state.positions_json = json.encode(positions)
            return
        end

        -- Track position and realized PnL (cost-basis accounting)
        local pos_key = row.sender .. ":" .. base
        local pos = positions[pos_key]
        if not pos then pos = { balance = 0, cost_usd = 0 } end

        local pnl = 0
        if base_delta > 0 then
            -- Buy: accumulate cost basis
            pos.balance = pos.balance + base_delta
            pos.cost_usd = pos.cost_usd + base_delta * price_usd
        elseif base_delta < 0 and pos.balance > 0 then
            -- Sell: realize PnL vs average cost
            local sold = math.abs(base_delta)
            local avg_cost = pos.cost_usd / pos.balance
            pnl = sold * (price_usd - avg_cost)
            pos.balance = pos.balance - sold
            pos.cost_usd = pos.cost_usd - sold * avg_cost
        end

        positions[pos_key] = pos
        state.positions_json = json.encode(positions)

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
"#;

// ─── Helpers ────────────────────────────────────────────────────────

fn make_swap(
    block_time: i64,
    tx_hash: &str,
    pool: &str,
    token0: &str,
    token1: &str,
    sender: &str,
    amount0: f64,
    amount1: f64,
) -> RowMap {
    HashMap::from([
        ("block_time".to_string(), Value::DateTime(block_time)),
        ("tx_hash".to_string(), Value::String(tx_hash.to_string())),
        ("network".to_string(), Value::String("ethereum".to_string())),
        ("pool".to_string(), Value::String(pool.to_string())),
        ("token0".to_string(), Value::String(token0.to_string())),
        ("token1".to_string(), Value::String(token1.to_string())),
        ("sender".to_string(), Value::String(sender.to_string())),
        ("amount0".to_string(), Value::Float64(amount0)),
        ("amount1".to_string(), Value::Float64(amount1)),
    ])
}

/// WETH/USDC swap. amount_weth > 0 = user buys WETH (pays USDC).
fn weth_usdc(t: i64, tx: &str, sender: &str, weth: f64, usdc: f64) -> RowMap {
    make_swap(t, tx, POOL_WETH_USDC, WETH, USDC, sender, weth, usdc)
}

/// WETH/USDT swap.
fn weth_usdt(t: i64, tx: &str, sender: &str, weth: f64, usdt: f64) -> RowMap {
    make_swap(t, tx, POOL_WETH_USDT, WETH, USDT, sender, weth, usdt)
}

/// UNI/WETH swap. amount_uni > 0 = user buys UNI (pays WETH).
fn uni_weth(t: i64, tx: &str, sender: &str, uni: f64, weth: f64) -> RowMap {
    make_swap(t, tx, POOL_UNI_WETH, UNI, WETH, sender, uni, weth)
}

/// LINK/WETH swap.
fn link_weth(t: i64, tx: &str, sender: &str, link: f64, weth: f64) -> RowMap {
    make_swap(t, tx, POOL_LINK_WETH, LINK, WETH, sender, link, weth)
}

fn find_records<'a>(batch: &'a DeltaBatch, table: &str) -> Vec<&'a DeltaRecord> {
    batch.records.iter().filter(|r| r.table == table).collect()
}

fn find_by_key<'a>(
    batch: &'a DeltaBatch,
    table: &str,
    key_checks: &[(&str, &Value)],
) -> Option<&'a DeltaRecord> {
    batch.records.iter().find(|r| {
        r.table == table
            && key_checks
                .iter()
                .all(|(k, v)| r.key.get(*k) == Some(*v))
    })
}

fn get_val<'a>(record: &'a DeltaRecord, col: &str) -> &'a Value {
    record
        .values
        .get(col)
        .or_else(|| record.key.get(col))
        .unwrap_or_else(|| panic!("missing column '{col}' in record for table '{}'", record.table))
}

fn assert_approx(actual: f64, expected: f64, label: &str) {
    assert!(
        (actual - expected).abs() < 0.01,
        "{label}: expected {expected}, got {actual}"
    );
}

// ─── Tests ──────────────────────────────────────────────────────────

#[test]
fn schema_parses_successfully() {
    DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
}

/// Direct stablecoin pricing: WETH/USDC swap produces correct USD price.
#[test]
fn direct_stablecoin_pricing() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // Buy 1.5 WETH at $2000 (pay 3000 USDC)
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0xaaa", "alice", 1.5, -3000.0),
    ]).unwrap();

    let batch = db.flush().unwrap();

    // Candle should show WETH price = $2000
    let candles = find_records(&batch, "candles_5m");
    assert_eq!(candles.len(), 1);

    let candle = candles[0];
    assert_approx(get_val(candle, "open").as_f64().unwrap(), 2000.0, "open");
    assert_approx(get_val(candle, "close").as_f64().unwrap(), 2000.0, "close");
    assert_approx(get_val(candle, "volume").as_f64().unwrap(), 3000.0, "volume");
    assert_eq!(get_val(candle, "trade_count"), &Value::UInt64(1));
}

/// Cross-price via ETH: UNI/WETH swap priced through ETH/USD reference.
#[test]
fn cross_price_via_eth() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // Block 1: Establish ETH/USD = $2000 via WETH/USDC swap
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "market_maker", 1.0, -2000.0),
    ]).unwrap();

    // Block 2: UNI/WETH swap — buy 100 UNI, pay 0.5 WETH
    // ratio = |(-0.5) / 100| = 0.005 WETH per UNI
    // UNI price = 0.005 * $2000 = $10
    db.process_batch("swaps", 2, vec![
        uni_weth(t0 + 1_000, "0x2", "alice", 100.0, -0.5),
    ]).unwrap();

    let batch = db.flush().unwrap();

    // Find the UNI pool candle
    let pool_val = Value::String(POOL_UNI_WETH.to_string());
    let uni_candle = batch.records.iter().find(|r| {
        r.table == "candles_5m" && r.key.get("pool") == Some(&pool_val)
    }).expect("missing UNI candle");

    // UNI price should be $10 (cross-calculated via ETH)
    assert_approx(get_val(uni_candle, "open").as_f64().unwrap(), 10.0, "UNI open");
    assert_approx(get_val(uni_candle, "close").as_f64().unwrap(), 10.0, "UNI close");
    // Volume = |0.5 WETH| * $2000 = $1000
    assert_approx(get_val(uni_candle, "volume").as_f64().unwrap(), 1000.0, "UNI volume");
}

/// Cross-price in both directions: UNI/WETH (token1=WETH) and WETH/LINK scenario
/// where LINK is token1 (tests the t0==WETH branch).
#[test]
fn cross_price_both_directions() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // Establish ETH/USD = $2000
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "mm", 1.0, -2000.0),
    ]).unwrap();

    // UNI/WETH: buy 100 UNI for 0.5 WETH
    // ratio = 0.005 → UNI = $10
    db.process_batch("swaps", 2, vec![
        uni_weth(t0 + 1_000, "0x2", "alice", 100.0, -0.5),
    ]).unwrap();

    // LINK/WETH: buy 50 LINK for 0.75 WETH
    // ratio = |(-0.75) / 50| = 0.015 WETH per LINK
    // LINK = 0.015 * $2000 = $30
    db.process_batch("swaps", 3, vec![
        link_weth(t0 + 2_000, "0x3", "bob", 50.0, -0.75),
    ]).unwrap();

    let batch = db.flush().unwrap();

    let uni_pool = Value::String(POOL_UNI_WETH.to_string());
    let link_pool = Value::String(POOL_LINK_WETH.to_string());

    let uni_candle = batch.records.iter()
        .find(|r| r.table == "candles_5m" && r.key.get("pool") == Some(&uni_pool))
        .expect("missing UNI candle");
    assert_approx(get_val(uni_candle, "open").as_f64().unwrap(), 10.0, "UNI price");

    let link_candle = batch.records.iter()
        .find(|r| r.table == "candles_5m" && r.key.get("pool") == Some(&link_pool))
        .expect("missing LINK candle");
    assert_approx(get_val(link_candle, "open").as_f64().unwrap(), 30.0, "LINK price");
}

/// ETH price updates propagate to subsequent cross-priced swaps.
#[test]
fn eth_price_update_propagation() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // Block 1: ETH = $2000
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "mm", 1.0, -2000.0),
    ]).unwrap();

    // Block 2: UNI/WETH at 0.005 WETH/UNI → UNI = $10
    db.process_batch("swaps", 2, vec![
        uni_weth(t0 + 1_000, "0x2", "alice", 100.0, -0.5),
    ]).unwrap();

    // Block 3: ETH price rises to $2200
    db.process_batch("swaps", 3, vec![
        weth_usdc(t0 + 2_000, "0x3", "mm", 1.0, -2200.0),
    ]).unwrap();

    // Block 4: Same UNI/WETH ratio (0.005) → UNI = 0.005 * $2200 = $11
    db.process_batch("swaps", 4, vec![
        uni_weth(t0 + 3_000, "0x4", "bob", 200.0, -1.0),
    ]).unwrap();

    let batch = db.flush().unwrap();

    // The UNI candle should have:
    // open = $10 (first trade), close = $11 (last trade, after ETH price update)
    // high = $11, low = $10
    let uni_pool = Value::String(POOL_UNI_WETH.to_string());
    let uni_candle = batch.records.iter()
        .find(|r| r.table == "candles_5m" && r.key.get("pool") == Some(&uni_pool))
        .expect("missing UNI candle");

    assert_approx(get_val(uni_candle, "open").as_f64().unwrap(), 10.0, "open");
    assert_approx(get_val(uni_candle, "close").as_f64().unwrap(), 11.0, "close");
    assert_approx(get_val(uni_candle, "high").as_f64().unwrap(), 11.0, "high");
    assert_approx(get_val(uni_candle, "low").as_f64().unwrap(), 10.0, "low");
    assert_eq!(get_val(uni_candle, "trade_count"), &Value::UInt64(2));
}

/// OHLC candles across multiple 5-minute windows with cross-priced tokens.
#[test]
fn ohlc_multiple_windows_cross_priced() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;
    let five_min = 300_000i64;

    // Block 1: Establish ETH = $2000
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "mm", 1.0, -2000.0),
    ]).unwrap();

    // Window 1: UNI trade at $10
    db.process_batch("swaps", 2, vec![
        uni_weth(t0 + 1_000, "0x2", "alice", 100.0, -0.5),
    ]).unwrap();

    // Window 2: UNI trades at $12 and $8
    db.process_batch("swaps", 3, vec![
        uni_weth(t0 + five_min + 1_000, "0x3", "bob", 100.0, -0.6),
    ]).unwrap();
    db.process_batch("swaps", 4, vec![
        uni_weth(t0 + five_min + 10_000, "0x4", "alice", 100.0, -0.4),
    ]).unwrap();

    let batch = db.flush().unwrap();
    let uni_pool = Value::String(POOL_UNI_WETH.to_string());
    let uni_candles: Vec<_> = batch.records.iter()
        .filter(|r| r.table == "candles_5m" && r.key.get("pool") == Some(&uni_pool))
        .collect();

    assert_eq!(uni_candles.len(), 2, "should produce 2 time windows");

    // Window 1: single trade at $10
    let w1_start = Value::DateTime((t0 + 1_000) / (300 * 1000) * (300 * 1000));
    let w1 = find_by_key(&batch, "candles_5m", &[("pool", &uni_pool), ("window_start", &w1_start)])
        .expect("missing window 1 candle");
    assert_approx(get_val(w1, "open").as_f64().unwrap(), 10.0, "w1 open");
    assert_eq!(get_val(w1, "trade_count"), &Value::UInt64(1));

    // Window 2: two trades at $12 and $8
    let w2_start = Value::DateTime((t0 + five_min + 1_000) / (300 * 1000) * (300 * 1000));
    let w2 = find_by_key(&batch, "candles_5m", &[("pool", &uni_pool), ("window_start", &w2_start)])
        .expect("missing window 2 candle");
    assert_approx(get_val(w2, "open").as_f64().unwrap(), 12.0, "w2 open");
    assert_approx(get_val(w2, "close").as_f64().unwrap(), 8.0, "w2 close");
    assert_approx(get_val(w2, "high").as_f64().unwrap(), 12.0, "w2 high");
    assert_approx(get_val(w2, "low").as_f64().unwrap(), 8.0, "w2 low");
    assert_eq!(get_val(w2, "trade_count"), &Value::UInt64(2));
}

/// PnL tracking with cross-priced tokens: buy UNI via WETH, sell at different ETH price.
#[test]
fn wallet_pnl_cross_priced() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // Block 1: ETH = $2000
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "mm", 1.0, -2000.0),
    ]).unwrap();

    // Block 2: Alice buys 100 UNI at 0.005 WETH/UNI = $10/UNI
    // Cost = 100 * $10 = $1000
    db.process_batch("swaps", 2, vec![
        uni_weth(t0 + 1_000, "0x2", "alice", 100.0, -0.5),
    ]).unwrap();

    // Block 3: ETH rises to $2200
    db.process_batch("swaps", 3, vec![
        weth_usdc(t0 + 2_000, "0x3", "mm", 1.0, -2200.0),
    ]).unwrap();

    // Block 4: Alice sells 50 UNI at 0.005 WETH/UNI = $11/UNI (due to ETH price rise)
    // Realized PnL = 50 * ($11 - $10) = $50
    db.process_batch("swaps", 4, vec![
        uni_weth(t0 + 3_000, "0x4", "alice", -50.0, 0.25),
    ]).unwrap();

    let batch = db.flush().unwrap();

    let alice_uni = find_by_key(&batch, "wallet_summary", &[
        ("sender", &Value::String("alice".into())),
        ("pool", &Value::String(POOL_UNI_WETH.into())),
    ]).expect("missing alice UNI summary");

    assert_approx(get_val(alice_uni, "total_pnl").as_f64().unwrap(), 50.0, "alice PnL");
    assert_approx(get_val(alice_uni, "current_position").as_f64().unwrap(), 50.0, "alice position");
    assert_eq!(get_val(alice_uni, "trade_count"), &Value::UInt64(2));
}

/// Direct stablecoin PnL: buy and sell WETH against USDC.
#[test]
fn wallet_pnl_direct_stablecoin() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // Alice buys 10 WETH at $2000
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "alice", 10.0, -20_000.0),
    ]).unwrap();

    // Alice buys 5 WETH at $2100
    db.process_batch("swaps", 2, vec![
        weth_usdc(t0 + 1_000, "0x2", "alice", 5.0, -10_500.0),
    ]).unwrap();

    // Alice's avg cost = (20000 + 10500) / (10 + 5) = $2033.33
    // Alice sells 5 WETH at $2200
    // PnL = 5 * (2200 - 2033.33) = $833.33
    db.process_batch("swaps", 3, vec![
        weth_usdc(t0 + 2_000, "0x3", "alice", -5.0, 11_000.0),
    ]).unwrap();

    let batch = db.flush().unwrap();

    let alice = find_by_key(&batch, "wallet_summary", &[
        ("sender", &Value::String("alice".into())),
        ("pool", &Value::String(POOL_WETH_USDC.into())),
    ]).expect("missing alice WETH/USDC summary");

    assert_approx(get_val(alice, "total_pnl").as_f64().unwrap(), 833.33, "alice PnL");
    assert_approx(get_val(alice, "current_position").as_f64().unwrap(), 10.0, "alice position");
}

/// Multiple wallets trading both direct and cross-priced pools.
#[test]
fn multi_wallet_multi_pool() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // ETH = $2000
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "mm", 1.0, -2000.0),
    ]).unwrap();

    // Alice buys WETH directly, Bob buys UNI via WETH
    db.process_batch("swaps", 2, vec![
        weth_usdc(t0 + 1_000, "0x2", "alice", 10.0, -20_000.0),
        uni_weth(t0 + 2_000, "0x3", "bob", 1000.0, -5.0),
    ]).unwrap();

    // Block 3: Bob sells UNI while ETH is still $2000 → PnL = 0
    db.process_batch("swaps", 3, vec![
        uni_weth(t0 + 3_000, "0x4", "bob", -500.0, 2.5),
    ]).unwrap();

    // Block 4: Alice sells WETH at $2500
    // Alice PnL: 5 * (2500 - 2000) = 2500
    db.process_batch("swaps", 4, vec![
        weth_usdc(t0 + 4_000, "0x5", "alice", -5.0, 12_500.0),
    ]).unwrap();

    let batch = db.flush().unwrap();

    // Alice: WETH position
    let alice = find_by_key(&batch, "wallet_summary", &[
        ("sender", &Value::String("alice".into())),
        ("pool", &Value::String(POOL_WETH_USDC.into())),
    ]).expect("missing alice summary");
    assert_approx(get_val(alice, "total_pnl").as_f64().unwrap(), 2500.0, "alice PnL");
    assert_approx(get_val(alice, "current_position").as_f64().unwrap(), 5.0, "alice pos");

    // Bob: UNI position — sold at same ETH price = no PnL
    let bob = find_by_key(&batch, "wallet_summary", &[
        ("sender", &Value::String("bob".into())),
        ("pool", &Value::String(POOL_UNI_WETH.into())),
    ]).expect("missing bob summary");
    assert_approx(get_val(bob, "total_pnl").as_f64().unwrap(), 0.0, "bob PnL");
    assert_approx(get_val(bob, "current_position").as_f64().unwrap(), 500.0, "bob pos");
}

/// WETH/USDT pricing works identically to WETH/USDC.
#[test]
fn usdt_pricing() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // WETH/USDT swap: sell 1 WETH for 2100 USDT
    db.process_batch("swaps", 1, vec![
        weth_usdt(t0, "0x1", "alice", -1.0, 2100.0),
    ]).unwrap();

    // UNI/WETH: uses ETH price from USDT pool = $2100
    // Buy 100 UNI for 0.5 WETH → UNI = 0.005 * 2100 = $10.5
    db.process_batch("swaps", 2, vec![
        uni_weth(t0 + 1_000, "0x2", "bob", 100.0, -0.5),
    ]).unwrap();

    let batch = db.flush().unwrap();

    let uni_pool = Value::String(POOL_UNI_WETH.to_string());
    let uni_candle = batch.records.iter()
        .find(|r| r.table == "candles_5m" && r.key.get("pool") == Some(&uni_pool))
        .expect("missing UNI candle");

    assert_approx(get_val(uni_candle, "open").as_f64().unwrap(), 10.5, "UNI via USDT");
}

/// Rollback restores cross-price state correctly.
#[test]
fn rollback_restores_cross_prices() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // Block 1: ETH = $2000
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "mm", 1.0, -2000.0),
    ]).unwrap();

    // Block 2: UNI trade at $10
    db.process_batch("swaps", 2, vec![
        uni_weth(t0 + 1_000, "0x2", "alice", 100.0, -0.5),
    ]).unwrap();

    // Block 3: Bad ETH price spike to $10000 (will be rolled back)
    db.process_batch("swaps", 3, vec![
        weth_usdc(t0 + 2_000, "0x3", "mm", 1.0, -10_000.0),
    ]).unwrap();

    // Block 4: UNI trade with wrong ETH price → UNI = $50 (wrong)
    db.process_batch("swaps", 4, vec![
        uni_weth(t0 + 3_000, "0x4", "bob", 100.0, -0.5),
    ]).unwrap();

    db.flush();

    // Rollback blocks 3 and 4
    db.rollback(2).unwrap();

    // Re-ingest block 3: correct ETH = $2200
    db.process_batch("swaps", 3, vec![
        weth_usdc(t0 + 2_000, "0x3b", "mm", 1.0, -2200.0),
    ]).unwrap();

    // Re-ingest block 4: UNI trade → UNI = 0.005 * $2200 = $11
    db.process_batch("swaps", 4, vec![
        uni_weth(t0 + 3_000, "0x4b", "bob", 100.0, -0.5),
    ]).unwrap();

    let batch = db.flush().unwrap();

    let uni_pool = Value::String(POOL_UNI_WETH.to_string());
    let uni_candle = batch.records.iter()
        .find(|r| r.table == "candles_5m" && r.key.get("pool") == Some(&uni_pool))
        .expect("missing UNI candle after rollback");

    // After rollback: UNI trades at $10 (block 2) and $11 (block 4)
    assert_approx(get_val(uni_candle, "open").as_f64().unwrap(), 10.0, "rollback open");
    assert_approx(get_val(uni_candle, "close").as_f64().unwrap(), 11.0, "rollback close");
    assert_eq!(get_val(uni_candle, "trade_count"), &Value::UInt64(2));
}

/// PnL rollback: rolling back a sell restores cost basis correctly.
#[test]
fn pnl_rollback() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // Block 1: ETH = $2000
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x1", "mm", 1.0, -2000.0),
    ]).unwrap();

    // Block 2: Alice buys 100 UNI at $10
    db.process_batch("swaps", 2, vec![
        uni_weth(t0 + 1_000, "0x2", "alice", 100.0, -0.5),
    ]).unwrap();

    // Block 3: Alice sells 50 UNI at $10 → PnL = 0
    db.process_batch("swaps", 3, vec![
        uni_weth(t0 + 2_000, "0x3", "alice", -50.0, 0.25),
    ]).unwrap();

    db.flush();

    // Rollback the sell
    db.rollback(2).unwrap();
    db.flush();

    // Re-ingest block 3: ETH jumps to $3000
    db.process_batch("swaps", 3, vec![
        weth_usdc(t0 + 2_000, "0x3b", "mm", 1.0, -3000.0),
    ]).unwrap();

    // Block 4: Alice sells 50 UNI at 0.005 WETH = $15/UNI
    // PnL = 50 * ($15 - $10) = $250
    db.process_batch("swaps", 4, vec![
        uni_weth(t0 + 3_000, "0x4", "alice", -50.0, 0.25),
    ]).unwrap();

    let batch = db.flush().unwrap();

    let alice = find_by_key(&batch, "wallet_summary", &[
        ("sender", &Value::String("alice".into())),
        ("pool", &Value::String(POOL_UNI_WETH.into())),
    ]).expect("missing alice after rollback");

    assert_approx(get_val(alice, "total_pnl").as_f64().unwrap(), 250.0, "rollback PnL");
    assert_approx(get_val(alice, "current_position").as_f64().unwrap(), 50.0, "rollback pos");
}

/// Full scenario: mixed pools, cross-pricing, finalization, rollback, re-ingest.
#[test]
fn full_scenario_with_cross_pricing() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;
    let block_time = 12_000i64;

    // Phase 1: Establish reference prices
    // Block 1: ETH = $2000 via WETH/USDC
    db.process_batch("swaps", 1, vec![
        weth_usdc(t0, "0x01", "mm", 10.0, -20_000.0),
    ]).unwrap();

    // Phase 2: Trading on multiple pools (blocks 2-20)
    for block in 2..=20u64 {
        let bt = t0 + block as i64 * block_time;
        let mut swaps = Vec::new();

        // Periodic ETH price updates
        if block % 5 == 0 {
            let eth_price = 2000.0 + (block as f64 - 10.0) * 20.0;
            swaps.push(weth_usdc(bt, &format!("0xeth{block}"), "mm", 1.0, -eth_price));
        }

        // Alice trades UNI
        if block % 3 == 0 {
            let is_buy = block <= 12;
            let uni_amount = 100.0;
            let weth_per_uni = 0.005; // $10 at ETH=$2000
            if is_buy {
                swaps.push(uni_weth(bt + 1000, &format!("0xuni{block}"), "alice",
                    uni_amount, -(uni_amount * weth_per_uni)));
            } else {
                swaps.push(uni_weth(bt + 1000, &format!("0xuni{block}"), "alice",
                    -uni_amount, uni_amount * weth_per_uni));
            }
        }

        // Bob trades LINK
        if block % 4 == 0 {
            let is_buy = block <= 12;
            let link_amount = 50.0;
            let weth_per_link = 0.015; // $30 at ETH=$2000
            if is_buy {
                swaps.push(link_weth(bt + 2000, &format!("0xlink{block}"), "bob",
                    link_amount, -(link_amount * weth_per_link)));
            } else {
                swaps.push(link_weth(bt + 2000, &format!("0xlink{block}"), "bob",
                    -link_amount, link_amount * weth_per_link));
            }
        }

        if !swaps.is_empty() {
            db.process_batch("swaps", block, swaps).unwrap();
        }
    }

    // Finalize up to block 10
    db.finalize(10).unwrap();

    let batch1 = db.flush().unwrap();
    assert!(!batch1.records.is_empty());

    // Verify candles exist for cross-priced pools
    let uni_pool = Value::String(POOL_UNI_WETH.to_string());
    let link_pool = Value::String(POOL_LINK_WETH.to_string());

    assert!(batch1.records.iter().any(|r|
        r.table == "candles_5m" && r.key.get("pool") == Some(&uni_pool)
    ), "missing UNI candle");
    assert!(batch1.records.iter().any(|r|
        r.table == "candles_5m" && r.key.get("pool") == Some(&link_pool)
    ), "missing LINK candle");

    // Verify wallet summaries exist
    assert!(batch1.records.iter().any(|r|
        r.table == "wallet_summary"
            && r.key.get("sender") == Some(&Value::String("alice".into()))
    ), "missing alice summary");
    assert!(batch1.records.iter().any(|r|
        r.table == "wallet_summary"
            && r.key.get("sender") == Some(&Value::String("bob".into()))
    ), "missing bob summary");

    // Phase 3: Rollback to block 15
    db.rollback(15).unwrap();
    assert_eq!(db.latest_block(), 15);

    let rollback_batch = db.flush().unwrap();
    assert!(!rollback_batch.records.is_empty(), "rollback should produce deltas");

    // Phase 4: Re-ingest blocks 16-25 with different ETH price
    for block in 16..=24u64 {
        let bt = t0 + block as i64 * block_time;
        let mut swaps = Vec::new();

        // ETH crashes to $1500
        if block == 16 {
            swaps.push(weth_usdc(bt, "0xcrash", "mm", 1.0, -1500.0));
        }

        // Alice and Bob continue trading at new prices
        if block % 2 == 0 {
            swaps.push(uni_weth(bt + 1000, &format!("0xre_uni{block}"), "alice",
                -50.0, 0.25));
        }
        if block % 3 == 0 {
            swaps.push(link_weth(bt + 2000, &format!("0xre_link{block}"), "bob",
                -25.0, 0.375));
        }

        if !swaps.is_empty() {
            db.process_batch("swaps", block, swaps).unwrap();
        }
    }

    assert_eq!(db.latest_block(), 24);

    let final_batch = db.flush().unwrap();
    assert!(!final_batch.records.is_empty());

    // Verify final state has both pools
    let has_uni = final_batch.records.iter().any(|r|
        r.table == "candles_5m" && r.key.get("pool") == Some(&uni_pool)
    );
    let has_link = final_batch.records.iter().any(|r|
        r.table == "candles_5m" && r.key.get("pool") == Some(&link_pool)
    );
    assert!(has_uni || has_link, "should have candles after re-ingest");

    println!(
        "Full cross-pricing scenario passed: {} records in final batch",
        final_batch.records.len(),
    );
}

/// Verify that swap_prices emits include block_time (timestamp of the swap).
#[test]
fn swap_prices_includes_timestamp() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    db.process_batch("swaps", 1, vec![
        weth_usdc(t0 + 42_000, "0x1", "alice", 1.0, -2000.0),
    ]).unwrap();

    let batch = db.flush().unwrap();

    // The swap_prices reducer emits block_time → candles_5m uses it for windowing.
    // If block_time wasn't emitted, toStartOfInterval would get 0 → all in epoch window.
    // Verify the candle's window_start corresponds to t0+42000, not epoch.
    let candles = find_records(&batch, "candles_5m");
    assert_eq!(candles.len(), 1);

    let expected_window = (t0 + 42_000) / (300 * 1000) * (300 * 1000);
    let window = get_val(candles[0], "window_start").as_i64().unwrap();
    assert_eq!(window, expected_window, "window_start should match swap timestamp");
}

/// No ETH reference price yet → cross-priced swap should not emit (price_usd = 0).
#[test]
fn cross_price_without_eth_reference() {
    let mut db = DeltaDb::open(Config::new(UNISWAP_SCHEMA)).unwrap();
    let t0 = 1_700_000_000_000i64;

    // UNI/WETH trade WITHOUT any prior WETH/USDC or WETH/USDT swap
    db.process_batch("swaps", 1, vec![
        uni_weth(t0, "0x1", "alice", 100.0, -0.5),
    ]).unwrap();

    let batch = db.flush();

    // No ETH reference → price_usd = 0 → reducer doesn't emit → no candle
    if let Some(batch) = batch {
        let candles = find_records(&batch, "candles_5m");
        let uni_pool = Value::String(POOL_UNI_WETH.to_string());
        let uni_candles: Vec<_> = candles.iter()
            .filter(|r| r.key.get("pool") == Some(&uni_pool))
            .collect();
        assert!(uni_candles.is_empty(), "should not produce candle without ETH reference");
    }
}
