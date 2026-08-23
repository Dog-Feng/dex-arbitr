use anyhow::{Context, Result};
use rust_decimal::Decimal;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::HistoryConfig;

#[derive(Debug, Clone)]
pub struct NaturalSpread {
    pub value: Decimal,
    pub points: usize,
    pub computed_at: Instant,
}

pub struct HistoryStore {
    cfg: HistoryConfig,
    conn: Mutex<Connection>,
    last_sample: Mutex<HashMap<String, Instant>>,
    cache: Mutex<HashMap<String, NaturalSpread>>,
}

impl HistoryStore {
    pub fn open(cfg: HistoryConfig) -> Result<Self> {
        if let Some(dir) = Path::new(&cfg.db_path).parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
            }
        }
        let conn = Connection::open(&cfg.db_path)
            .with_context(|| format!("open sqlite {}", cfg.db_path))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS spread_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                pair_id TEXT NOT NULL,
                buy TEXT NOT NULL,
                sell TEXT NOT NULL,
                raw_pct TEXT NOT NULL,
                net_pct TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_spread_lookup
                ON spread_samples(pair_id, buy, sell, ts);",
        )?;
        Ok(Self {
            cfg,
            conn: Mutex::new(conn),
            last_sample: Mutex::new(HashMap::new()),
            cache: Mutex::new(HashMap::new()),
        })
    }

    pub fn maybe_sample(
        &self,
        pair_id: &str,
        buy: &str,
        sell: &str,
        raw_pct: Decimal,
        net_pct: Decimal,
    ) -> Result<Option<NaturalSpread>> {
        if !self.cfg.enabled {
            return Ok(None);
        }
        let key = sample_key(pair_id, buy, sell);
        if self.cfg.sample_interval_secs > 0 {
            let interval = Duration::from_secs(self.cfg.sample_interval_secs);
            let mut last = self.last_sample.lock().expect("history last_sample");
            if let Some(at) = last.get(&key) {
                if at.elapsed() < interval {
                    return Ok(self.cached_if_fresh(&key));
                }
            }
            last.insert(key.clone(), Instant::now());
        }

        let ts = now_secs();
        {
            let conn = self.conn.lock().expect("history db");
            conn.execute(
                "INSERT INTO spread_samples(ts, pair_id, buy, sell, raw_pct, net_pct)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    ts as i64,
                    pair_id,
                    buy,
                    sell,
                    raw_pct.to_string(),
                    net_pct.to_string()
                ],
            )?;
        }
        Ok(self.refresh_natural(pair_id, buy, sell)?)
    }

    pub fn natural(&self, pair_id: &str, buy: &str, sell: &str) -> Option<NaturalSpread> {
        let key = sample_key(pair_id, buy, sell);
        if let Some(hit) = self.cached_if_fresh(&key) {
            return Some(hit);
        }
        self.refresh_natural(pair_id, buy, sell).ok().flatten()
    }

    /// 当前 24h 窗口内该方向已写入的毛价差样本数（含尚未凑够 min_points 的预热期）。
    pub fn window_points(&self, pair_id: &str, buy: &str, sell: &str) -> usize {
        let cutoff = now_secs().saturating_sub(self.cfg.window_hours.max(1) * 3600);
        let conn = self.conn.lock().expect("history db");
        conn.query_row(
            "SELECT COUNT(*) FROM spread_samples
             WHERE pair_id = ?1 AND buy = ?2 AND sell = ?3 AND ts >= ?4",
            rusqlite::params![pair_id, buy, sell, cutoff as i64],
            |row| row.get::<_, i64>(0),
        )
        .ok()
        .map(|n| n as usize)
        .unwrap_or(0)
    }

    fn cached_if_fresh(&self, key: &str) -> Option<NaturalSpread> {
        let cache = self.cache.lock().expect("history cache");
        let hit = cache.get(key)?.clone();
        if hit.computed_at.elapsed() <= Duration::from_secs(self.cfg.max_age_secs.max(1)) {
            Some(hit)
        } else {
            None
        }
    }

    fn refresh_natural(
        &self,
        pair_id: &str,
        buy: &str,
        sell: &str,
    ) -> Result<Option<NaturalSpread>> {
        let cutoff = now_secs().saturating_sub(self.cfg.window_hours.max(1) * 3600);
        let conn = self.conn.lock().expect("history db");
        let mut stmt = conn.prepare(
            "SELECT raw_pct FROM spread_samples
             WHERE pair_id = ?1 AND buy = ?2 AND sell = ?3 AND ts >= ?4",
        )?;
        let rows = stmt.query_map(rusqlite::params![pair_id, buy, sell, cutoff as i64], |row| {
            let s: String = row.get(0)?;
            Ok(s)
        })?;
        let mut values = Vec::new();
        for row in rows {
            let s = row?;
            if let Ok(v) = Decimal::from_str(&s) {
                values.push(v);
            }
        }
        values.sort();
        if values.len() < self.cfg.min_points {
            return Ok(None);
        }
        let nat = NaturalSpread {
            value: median(&values),
            points: values.len(),
            computed_at: Instant::now(),
        };
        self.cache
            .lock()
            .expect("history cache")
            .insert(sample_key(pair_id, buy, sell), nat.clone());
        Ok(Some(nat))
    }
}

fn sample_key(pair_id: &str, buy: &str, sell: &str) -> String {
    format!("{pair_id}|{buy}|{sell}")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Midpoint of a sorted slice. Caller must sort or pass already ordered values.
pub fn median(sorted: &[Decimal]) -> Decimal {
    let n = sorted.len();
    if n == 0 {
        return Decimal::ZERO;
    }
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / Decimal::from(2)
    }
}

/// 真实套利空间 = 净边 − max(天然价差, 0)。天然为负时当作 0。
pub fn residual_net(net_pct: Decimal, natural: Decimal) -> Decimal {
    net_pct - natural.max(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn median_odd_and_even() {
        assert_eq!(median(&[dec!(1), dec!(2), dec!(3)]), dec!(2));
        assert_eq!(median(&[dec!(1), dec!(2), dec!(3), dec!(4)]), dec!(2.5));
    }

    #[test]
    fn residual_ignores_negative_natural() {
        assert_eq!(residual_net(dec!(0.05), dec!(0.02)), dec!(0.03));
        assert_eq!(residual_net(dec!(0.05), dec!(-0.01)), dec!(0.05));
    }

    #[test]
    fn sqlite_median_after_enough_points() {
        let path = std::env::temp_dir().join(format!("dex-arbitr-hist-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let store = HistoryStore::open(HistoryConfig {
            enabled: true,
            db_path: path.to_string_lossy().into(),
            sample_interval_secs: 0,
            window_hours: 24,
            min_points: 3,
            max_age_secs: 200,
        })
        .unwrap();
        store
            .maybe_sample("BTC-USD-PERP", "lighter", "lighter_rh", dec!(0.02), dec!(0.01))
            .unwrap();
        store
            .maybe_sample("BTC-USD-PERP", "lighter", "lighter_rh", dec!(0.04), dec!(0.03))
            .unwrap();
        let third = store
            .maybe_sample("BTC-USD-PERP", "lighter", "lighter_rh", dec!(0.03), dec!(0.02))
            .unwrap()
            .unwrap();
        assert_eq!(third.points, 3);
        assert_eq!(third.value, dec!(0.03));
        let _ = std::fs::remove_file(&path);
    }
}
