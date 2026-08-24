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
                ON spread_samples(pair_id, buy, sell, ts);
            CREATE TABLE IF NOT EXISTS natural_spreads (
                pair_id TEXT NOT NULL,
                buy TEXT NOT NULL,
                sell TEXT NOT NULL,
                value TEXT NOT NULL,
                points INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                PRIMARY KEY (pair_id, buy, sell)
            );",
        )?;
        let cache = load_snapshots(&conn)?;
        let store = Self {
            cfg,
            conn: Mutex::new(conn),
            last_sample: Mutex::new(HashMap::new()),
            cache: Mutex::new(cache),
        };
        if store.snapshot_count() == 0 {
            store.backfill_from_samples()?;
        }
        Ok(store)
    }

    pub fn snapshot_count(&self) -> usize {
        self.cache.lock().expect("history cache").len()
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
                    return Ok(self.cached(&key));
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
        if self.refresh_due(&key) {
            Ok(self.refresh_natural(pair_id, buy, sell)?)
        } else {
            Ok(self.cached(&key))
        }
    }

    /// 库里有快照就直接用；没有则用当前窗口样本现算（满 min_points）。
    pub fn natural(&self, pair_id: &str, buy: &str, sell: &str) -> Option<NaturalSpread> {
        let key = sample_key(pair_id, buy, sell);
        if let Some(hit) = self.cached(&key) {
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

    fn cached(&self, key: &str) -> Option<NaturalSpread> {
        self.cache.lock().expect("history cache").get(key).cloned()
    }

    fn refresh_due(&self, key: &str) -> bool {
        match self.cached(key) {
            None => true,
            Some(hit) => {
                hit.computed_at.elapsed() >= Duration::from_secs(self.cfg.refresh_interval_secs.max(1))
            }
        }
    }

    fn backfill_from_samples(&self) -> Result<()> {
        let keys = {
            let conn = self.conn.lock().expect("history db");
            let mut stmt = conn.prepare(
                "SELECT DISTINCT pair_id, buy, sell FROM spread_samples",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            let mut keys = Vec::new();
            for row in rows {
                keys.push(row?);
            }
            keys
        };
        for (pair_id, buy, sell) in keys {
            let _ = self.refresh_natural(&pair_id, &buy, &sell)?;
        }
        Ok(())
    }

    fn refresh_natural(
        &self,
        pair_id: &str,
        buy: &str,
        sell: &str,
    ) -> Result<Option<NaturalSpread>> {
        let cutoff = now_secs().saturating_sub(self.cfg.window_hours.max(1) * 3600);
        let values = {
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
            values
        };
        let mut sorted = values;
        sorted.sort();
        if sorted.len() < self.cfg.min_points {
            return Ok(self.cached(&sample_key(pair_id, buy, sell)));
        }
        let nat = NaturalSpread {
            value: median(&sorted),
            points: sorted.len(),
            computed_at: Instant::now(),
        };
        self.persist_snapshot(pair_id, buy, sell, &nat)?;
        self.cache
            .lock()
            .expect("history cache")
            .insert(sample_key(pair_id, buy, sell), nat.clone());
        Ok(Some(nat))
    }

    fn persist_snapshot(
        &self,
        pair_id: &str,
        buy: &str,
        sell: &str,
        nat: &NaturalSpread,
    ) -> Result<()> {
        let conn = self.conn.lock().expect("history db");
        conn.execute(
            "INSERT INTO natural_spreads(pair_id, buy, sell, value, points, updated_ts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(pair_id, buy, sell) DO UPDATE SET
                value = excluded.value,
                points = excluded.points,
                updated_ts = excluded.updated_ts",
            rusqlite::params![
                pair_id,
                buy,
                sell,
                nat.value.to_string(),
                nat.points as i64,
                now_secs() as i64
            ],
        )?;
        Ok(())
    }
}

fn load_snapshots(conn: &Connection) -> Result<HashMap<String, NaturalSpread>> {
    let mut stmt = conn.prepare(
        "SELECT pair_id, buy, sell, value, points FROM natural_spreads",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;
    let mut cache = HashMap::new();
    let now = Instant::now();
    for row in rows {
        let (pair_id, buy, sell, value, points) = row?;
        let Ok(value) = Decimal::from_str(&value) else {
            continue;
        };
        cache.insert(
            sample_key(&pair_id, &buy, &sell),
            NaturalSpread {
                value,
                points: points.max(0) as usize,
                computed_at: now,
            },
        );
    }
    Ok(cache)
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

    fn cfg(path: &Path, min_points: usize, refresh_secs: u64) -> HistoryConfig {
        HistoryConfig {
            enabled: true,
            db_path: path.to_string_lossy().into(),
            sample_interval_secs: 0,
            window_hours: 24,
            min_points,
            max_age_secs: 200,
            refresh_interval_secs: refresh_secs,
        }
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dex-arbitr-hist-{}-{}-{name}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

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
        let path = tmp("enough");
        let _ = std::fs::remove_file(&path);
        let store = HistoryStore::open(cfg(&path, 3, 300)).unwrap();
        store
            .maybe_sample("BTC-USD-PERP", "lighter", "sodex", dec!(0.02), dec!(0.01))
            .unwrap();
        store
            .maybe_sample("BTC-USD-PERP", "lighter", "sodex", dec!(0.04), dec!(0.03))
            .unwrap();
        let third = store
            .maybe_sample("BTC-USD-PERP", "lighter", "sodex", dec!(0.03), dec!(0.02))
            .unwrap()
            .unwrap();
        assert_eq!(third.points, 3);
        assert_eq!(third.value, dec!(0.03));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reopen_uses_persisted_snapshot() {
        let path = tmp("reopen");
        let _ = std::fs::remove_file(&path);
        {
            let store = HistoryStore::open(cfg(&path, 3, 300)).unwrap();
            for v in [dec!(0.02), dec!(0.04), dec!(0.03)] {
                store
                    .maybe_sample("ETH-USD-PERP", "sodex", "lighter", v, v)
                    .unwrap();
            }
            assert_eq!(store.snapshot_count(), 1);
        }
        let again = HistoryStore::open(cfg(&path, 3, 300)).unwrap();
        let nat = again
            .natural("ETH-USD-PERP", "sodex", "lighter")
            .expect("persisted nat");
        assert_eq!(nat.value, dec!(0.03));
        assert_eq!(nat.points, 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn backfill_from_old_samples_table() {
        let path = tmp("backfill");
        let _ = std::fs::remove_file(&path);
        {
            let store = HistoryStore::open(cfg(&path, 3, 300)).unwrap();
            for v in [dec!(0.10), dec!(0.20), dec!(0.30)] {
                store
                    .maybe_sample("SOL-USD-PERP", "sodex", "lighter", v, v)
                    .unwrap();
            }
            // 模拟旧库：只有样本、没有快照表行。
            store
                .conn
                .lock()
                .unwrap()
                .execute("DELETE FROM natural_spreads", [])
                .unwrap();
        }
        let again = HistoryStore::open(cfg(&path, 3, 300)).unwrap();
        let nat = again
            .natural("SOL-USD-PERP", "sodex", "lighter")
            .expect("backfilled");
        assert_eq!(nat.value, dec!(0.20));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn keeps_snapshot_when_window_too_thin() {
        let path = tmp("keep");
        let _ = std::fs::remove_file(&path);
        let store = HistoryStore::open(cfg(&path, 3, 0)).unwrap();
        for v in [dec!(0.02), dec!(0.04), dec!(0.03)] {
            store
                .maybe_sample("BTC-USD-PERP", "sodex", "lighter", v, v)
                .unwrap();
        }
        store
            .conn
            .lock()
            .unwrap()
            .execute("DELETE FROM spread_samples", [])
            .unwrap();
        let still = store
            .refresh_natural("BTC-USD-PERP", "sodex", "lighter")
            .unwrap()
            .expect("keep old snapshot");
        assert_eq!(still.value, dec!(0.03));
        let _ = std::fs::remove_file(&path);
    }
}
