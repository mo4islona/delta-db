use std::collections::HashMap;

use mlua::{Lua, Result as LuaResult, RegistryKey, Value as LuaValue};

use crate::types::{RowMap, Value};

use super::ReducerRuntime;

/// Lua runtime for reducer process functions.
///
/// The Lua VM is created once and reused across all `process()` calls.
/// The user script is pre-compiled into a Lua function at init time.
/// Per call, only the `state`, `row`, and `emit` globals are updated.
pub struct LuaRuntime {
    lua: Lua,
    func_key: RegistryKey,
}

// Safety: Each LuaRuntime is owned by exactly one ReducerEngine.
// ReducerEngine::process_block takes &mut self, guaranteeing that
// process() is never called concurrently on the same LuaRuntime.
// The Lua VM is a self-contained C state that is safe to move between
// threads as long as it is not accessed concurrently.
unsafe impl Send for LuaRuntime {}
unsafe impl Sync for LuaRuntime {}

impl LuaRuntime {
    pub fn new(script: &str) -> Self {
        let lua = Lua::new();

        // Sandbox: remove dangerous globals
        sandbox(&lua).expect("failed to sandbox Lua");

        // Provide json module
        register_json_module(&lua).expect("failed to register json module");

        // Pre-compile script into a function
        let func = lua
            .load(script)
            .into_function()
            .expect("failed to compile Lua script");
        let func_key = lua
            .create_registry_value(func)
            .expect("failed to store Lua function in registry");

        Self { lua, func_key }
    }
}

impl ReducerRuntime for LuaRuntime {
    fn process(&self, state: &mut HashMap<String, Value>, row: &RowMap) -> Option<RowMap> {
        // Set up state table
        let state_table =
            value_map_to_lua_table(&self.lua, state).expect("failed to create state table");
        self.lua
            .globals()
            .set("state", state_table)
            .expect("failed to set state");

        // Set up row table
        let row_table =
            value_map_to_lua_table(&self.lua, row).expect("failed to create row table");
        self.lua
            .globals()
            .set("row", row_table)
            .expect("failed to set row");

        // Set up emit table (fresh each call)
        let emit_table = self.lua.create_table().expect("failed to create emit table");
        self.lua
            .globals()
            .set("emit", emit_table)
            .expect("failed to set emit");

        // Call pre-compiled function
        let func: mlua::Function = self
            .lua
            .registry_value(&self.func_key)
            .expect("failed to load Lua function from registry");
        func.call::<()>(()).expect("Lua script execution failed");

        // Read back state
        let state_table: mlua::Table = self
            .lua
            .globals()
            .get("state")
            .expect("failed to read state");
        *state = lua_table_to_value_map(&state_table).expect("failed to convert state");

        // Read back emit
        let emit_table: mlua::Table = self
            .lua
            .globals()
            .get("emit")
            .expect("failed to read emit");
        let emit_map = lua_table_to_value_map(&emit_table).expect("failed to convert emit");

        if emit_map.is_empty() {
            None
        } else {
            Some(emit_map)
        }
    }
}

fn sandbox(lua: &Lua) -> LuaResult<()> {
    let globals = lua.globals();
    // Remove dangerous modules/functions
    for name in &[
        "os",
        "io",
        "debug",
        "loadfile",
        "dofile",
        "load",
        "rawget",
        "rawset",
    ] {
        globals.set(*name, mlua::Value::Nil)?;
    }
    Ok(())
}

fn register_json_module(lua: &Lua) -> LuaResult<()> {
    let json_table = lua.create_table()?;

    // json.encode(value) -> string
    let encode = lua.create_function(|lua, val: LuaValue| {
        let s = lua_value_to_json_string(lua, &val);
        Ok(s)
    })?;
    json_table.set("encode", encode)?;

    // json.decode(string) -> value
    let decode = lua.create_function(|lua, s: String| {
        let val: serde_json::Value =
            serde_json::from_str(&s).map_err(|e| mlua::Error::external(e))?;
        json_value_to_lua(lua, &val)
    })?;
    json_table.set("decode", decode)?;

    lua.globals().set("json", json_table)?;
    Ok(())
}

fn value_map_to_lua_table(lua: &Lua, map: &HashMap<String, Value>) -> LuaResult<mlua::Table> {
    let table = lua.create_table()?;
    for (k, v) in map {
        table.set(k.as_str(), value_to_lua(lua, v)?)?;
    }
    Ok(table)
}

fn lua_table_to_value_map(table: &mlua::Table) -> LuaResult<HashMap<String, Value>> {
    let mut map = HashMap::new();
    for pair in table.pairs::<String, LuaValue>() {
        let (k, v) = pair?;
        map.insert(k, lua_to_value(&v));
    }
    Ok(map)
}

fn value_to_lua(lua: &Lua, val: &Value) -> LuaResult<LuaValue> {
    match val {
        Value::UInt64(v) => Ok(LuaValue::Number(*v as f64)),
        Value::Int64(v) => Ok(LuaValue::Number(*v as f64)),
        Value::Float64(v) => Ok(LuaValue::Number(*v)),
        Value::String(v) => Ok(LuaValue::String(lua.create_string(v)?)),
        Value::DateTime(v) => Ok(LuaValue::Number(*v as f64)),
        Value::Boolean(v) => Ok(LuaValue::Boolean(*v)),
        Value::Null => Ok(LuaValue::Nil),
        Value::Bytes(v) => Ok(LuaValue::String(lua.create_string(v)?)),
        Value::Uint256(v) => Ok(LuaValue::String(lua.create_string(v.as_slice())?)),
        Value::Base58(v) => Ok(LuaValue::String(lua.create_string(v)?)),
    }
}

fn lua_to_value(val: &LuaValue) -> Value {
    match val {
        LuaValue::Number(n) => {
            // If it's a whole number, store as Float64 for consistency
            Value::Float64(*n)
        }
        LuaValue::Integer(n) => Value::Int64(*n),
        LuaValue::String(s) => Value::String(lua_string_to_rust(s)),
        LuaValue::Boolean(b) => Value::Boolean(*b),
        LuaValue::Nil => Value::Null,
        _ => Value::Null,
    }
}

fn lua_string_to_rust(s: &mlua::String) -> String {
    s.to_str().map(|s| s.to_string()).unwrap_or_default()
}

fn lua_value_to_json_string(_lua: &Lua, val: &LuaValue) -> String {
    match val {
        LuaValue::Number(n) => serde_json::to_string(n).unwrap(),
        LuaValue::Integer(n) => serde_json::to_string(n).unwrap(),
        LuaValue::String(s) => serde_json::to_string(&lua_string_to_rust(s)).unwrap(),
        LuaValue::Boolean(b) => serde_json::to_string(b).unwrap(),
        LuaValue::Nil => "null".to_string(),
        LuaValue::Table(t) => {
            // Check if it's an array or object
            if is_lua_array(t) {
                let mut arr = Vec::new();
                for pair in t.clone().sequence_values::<LuaValue>() {
                    if let Ok(v) = pair {
                        arr.push(lua_value_to_json_string(_lua, &v));
                    }
                }
                format!("[{}]", arr.join(","))
            } else {
                let mut obj = Vec::new();
                for pair in t.clone().pairs::<String, LuaValue>() {
                    if let Ok((k, v)) = pair {
                        obj.push(format!(
                            "{}:{}",
                            serde_json::to_string(&k).unwrap(),
                            lua_value_to_json_string(_lua, &v)
                        ));
                    }
                }
                format!("{{{}}}", obj.join(","))
            }
        }
        _ => "null".to_string(),
    }
}

fn json_value_to_lua(lua: &Lua, val: &serde_json::Value) -> LuaResult<LuaValue> {
    match val {
        serde_json::Value::Null => Ok(LuaValue::Nil),
        serde_json::Value::Bool(b) => Ok(LuaValue::Boolean(*b)),
        serde_json::Value::Number(n) => Ok(LuaValue::Number(n.as_f64().unwrap_or(0.0))),
        serde_json::Value::String(s) => Ok(LuaValue::String(lua.create_string(s)?)),
        serde_json::Value::Array(arr) => {
            let table = lua.create_table()?;
            for (i, v) in arr.iter().enumerate() {
                table.set(i + 1, json_value_to_lua(lua, v)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        serde_json::Value::Object(obj) => {
            let table = lua.create_table()?;
            for (k, v) in obj {
                table.set(k.as_str(), json_value_to_lua(lua, v)?)?;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn is_lua_array(table: &mlua::Table) -> bool {
    let mut has_int_keys = false;
    let mut has_str_keys = false;
    for pair in table.clone().pairs::<LuaValue, LuaValue>() {
        if let Ok((k, _)) = pair {
            match k {
                LuaValue::Integer(_) | LuaValue::Number(_) => has_int_keys = true,
                _ => has_str_keys = true,
            }
        }
    }
    has_int_keys && !has_str_keys
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trade(side: &str, amount: f64, price: f64) -> RowMap {
        HashMap::from([
            ("side".to_string(), Value::String(side.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
            ("price".to_string(), Value::Float64(price)),
        ])
    }

    #[test]
    fn simple_lua_reducer() {
        let runtime = LuaRuntime::new(
            r#"
            state.count = state.count + 1
            emit.total = state.count
            "#,
        );

        let mut state = HashMap::from([("count".to_string(), Value::Float64(0.0))]);
        let row: RowMap = RowMap::new();

        let out1 = runtime.process(&mut state, &row).unwrap();
        assert_eq!(out1.get("total"), Some(&Value::Float64(1.0)));

        let out2 = runtime.process(&mut state, &row).unwrap();
        assert_eq!(out2.get("total"), Some(&Value::Float64(2.0)));
    }

    #[test]
    fn lua_reads_row_values() {
        let runtime = LuaRuntime::new(
            r#"
            emit.doubled = row.amount * 2
            "#,
        );

        let mut state = HashMap::new();
        let row: RowMap = HashMap::from([("amount".to_string(), Value::Float64(5.0))]);

        let out = runtime.process(&mut state, &row).unwrap();
        assert_eq!(out.get("doubled"), Some(&Value::Float64(10.0)));
    }

    #[test]
    fn lua_pnl_tracker() {
        // Simplified PnL: buy increases position, sell computes pnl
        let runtime = LuaRuntime::new(
            r#"
            if row.side == "buy" then
                state.quantity = state.quantity + row.amount
                state.cost_basis = state.cost_basis + row.amount * row.price
                emit.trade_pnl = 0
            else
                local avg_cost = state.cost_basis / state.quantity
                emit.trade_pnl = row.amount * (row.price - avg_cost)
                state.quantity = state.quantity - row.amount
                state.cost_basis = state.cost_basis - row.amount * avg_cost
            end
            emit.position_size = state.quantity
            "#,
        );

        let mut state = HashMap::from([
            ("quantity".to_string(), Value::Float64(0.0)),
            ("cost_basis".to_string(), Value::Float64(0.0)),
        ]);

        // BUY 10 @ 2000
        let out1 = runtime
            .process(&mut state, &make_trade("buy", 10.0, 2000.0))
            .unwrap();
        assert_eq!(out1.get("trade_pnl"), Some(&Value::Int64(0)));
        assert_eq!(out1.get("position_size"), Some(&Value::Float64(10.0)));

        // BUY 5 @ 2100
        let out2 = runtime
            .process(&mut state, &make_trade("buy", 5.0, 2100.0))
            .unwrap();
        assert_eq!(out2.get("position_size"), Some(&Value::Float64(15.0)));

        // SELL 8 @ 2200
        let out3 = runtime
            .process(&mut state, &make_trade("sell", 8.0, 2200.0))
            .unwrap();
        let pnl = out3.get("trade_pnl").unwrap().as_f64().unwrap();
        // 8 * (2200 - 2033.33) ≈ 1333.33
        assert!((pnl - 1333.33).abs() < 0.01);
        assert_eq!(out3.get("position_size"), Some(&Value::Float64(7.0)));
    }

    #[test]
    fn lua_json_roundtrip() {
        // Test the json module with FIFO-like lot tracking
        let runtime = LuaRuntime::new(
            r#"
            local lots = json.decode(state.lots)
            if row.side == "buy" then
                table.insert(lots, { qty = row.amount, price = row.price })
                emit.trade_pnl = 0
            else
                local remaining = row.amount
                local pnl = 0
                while remaining > 0 and #lots > 0 do
                    local lot = lots[1]
                    local used = math.min(remaining, lot.qty)
                    pnl = pnl + used * (row.price - lot.price)
                    lot.qty = lot.qty - used
                    remaining = remaining - used
                    if lot.qty <= 0 then table.remove(lots, 1) end
                end
                emit.trade_pnl = pnl
            end
            state.lots = json.encode(lots)

            local total_qty = 0
            for _, lot in ipairs(lots) do
                total_qty = total_qty + lot.qty
            end
            emit.position_size = total_qty
            "#,
        );

        let mut state = HashMap::from([("lots".to_string(), Value::String("[]".to_string()))]);

        // BUY 10 @ 100
        let out1 = runtime
            .process(&mut state, &make_trade("buy", 10.0, 100.0))
            .unwrap();
        assert_eq!(out1.get("trade_pnl"), Some(&Value::Int64(0)));
        assert_eq!(out1.get("position_size"), Some(&Value::Float64(10.0)));

        // BUY 5 @ 200
        let out2 = runtime
            .process(&mut state, &make_trade("buy", 5.0, 200.0))
            .unwrap();
        assert_eq!(out2.get("position_size"), Some(&Value::Float64(15.0)));

        // SELL 12 @ 150 (FIFO: 10 @ cost=100, 2 @ cost=200)
        let out3 = runtime
            .process(&mut state, &make_trade("sell", 12.0, 150.0))
            .unwrap();
        let pnl = out3.get("trade_pnl").unwrap().as_f64().unwrap();
        // 10*(150-100) + 2*(150-200) = 500 + (-100) = 400
        assert!((pnl - 400.0).abs() < 0.01);
        // Remaining: 3 lots @ 200
        assert_eq!(out3.get("position_size"), Some(&Value::Float64(3.0)));
    }

    #[test]
    fn lua_empty_emit_returns_none() {
        let runtime = LuaRuntime::new("-- do nothing");
        let mut state = HashMap::new();
        let row: RowMap = RowMap::new();
        assert!(runtime.process(&mut state, &row).is_none());
    }

    #[test]
    fn lua_sandbox_blocks_os() {
        let runtime = LuaRuntime::new(
            r#"
            if os == nil then
                emit.sandboxed = 1
            end
            "#,
        );
        let mut state = HashMap::new();
        let row: RowMap = RowMap::new();
        let out = runtime.process(&mut state, &row).unwrap();
        assert_eq!(out.get("sandboxed"), Some(&Value::Int64(1)));
    }

    #[test]
    fn lua_string_comparison() {
        let runtime = LuaRuntime::new(
            r#"
            if row.side == "buy" then
                emit.matched = 1
            else
                emit.matched = 0
            end
            "#,
        );
        let mut state = HashMap::new();
        let row: RowMap = HashMap::from([("side".to_string(), Value::String("buy".to_string()))]);
        let out = runtime.process(&mut state, &row).unwrap();
        assert_eq!(out.get("matched"), Some(&Value::Int64(1)));
    }
}
