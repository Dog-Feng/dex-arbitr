use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use rust_decimal::Decimal;
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct ExecRecord {
    pub ts: i64,
    pub pair_id: String,
    pub action: String,
    pub buy_venue: String,
    pub sell_venue: String,
    pub qty: Decimal,
    pub net_pct: Option<Decimal>,
    pub result: String,
    pub detail: String,
    /// 成交前格子。开仓从 0，减仓从当前格。
    pub grid_from: Option<i32>,
    /// 成交后格子（有符号 STEP）。
    pub grid_to: Option<i32>,
}

impl ExecRecord {
    /// 执行带只展示真实成交，撤单/超时/介入不进列表。
    pub fn is_fill(&self) -> bool {
        matches!(
            self.result.as_str(),
            "both_filled" | "filled" | "emergency_closed"
        )
    }
}

pub struct ExecJournal {
    conn: Connection,
}

impl ExecJournal {
    pub fn open(path: &str) -> Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(path).with_context(|| format!("open journal {path}"))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                pair_id TEXT NOT NULL,
                action TEXT NOT NULL,
                buy_venue TEXT,
                sell_venue TEXT,
                qty TEXT,
                net_pct TEXT,
                result TEXT,
                detail TEXT
            );",
        )?;
        Ok(Self { conn })
    }

    pub fn append(&self, r: &ExecRecord) -> Result<()> {
        self.conn.execute(
            "INSERT INTO executions (ts, pair_id, action, buy_venue, sell_venue, qty, net_pct, result, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                r.ts,
                r.pair_id,
                r.action,
                r.buy_venue,
                r.sell_venue,
                r.qty.to_string(),
                r.net_pct.map(|v| v.to_string()),
                r.result,
                r.detail,
            ],
        )?;
        Ok(())
    }

    pub fn recent(&self, limit: usize) -> Result<Vec<ExecRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, pair_id, action, buy_venue, sell_venue, qty, net_pct, result, detail
             FROM executions ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            Ok(ExecRecord {
                ts: row.get(0)?,
                pair_id: row.get(1)?,
                action: row.get(2)?,
                buy_venue: row.get(3)?,
                sell_venue: row.get(4)?,
                qty: Decimal::from_str(&row.get::<_, String>(5)?).unwrap_or(Decimal::ZERO),
                net_pct: row
                    .get::<_, Option<String>>(6)?
                    .and_then(|s| Decimal::from_str(&s).ok()),
                result: row.get(7)?,
                detail: row.get(8)?,
                grid_from: None,
                grid_to: None,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

pub fn now_ts() -> i64 {
    Utc::now().timestamp()
}
