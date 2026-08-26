//! 交易所错误指数退避。对齐参考 `ErrorBackoffController`。
//!
//! 针对的是三类**重试无用**的错误：
//! - `21104` invalid nonce：Lighter 的 nonce 缓存和链上不同步。立刻重发只会
//!   再撞一次，而且每次失败都可能把 nonce 推得更远。
//! - `429` / `23000` Too Many Requests：已经被限流了，继续打只会延长限流。
//!
//! 和 [`super::reduce_only`] 的区别：那个是交易所对**市场状态**的判定
//! （二值、按 pair、探针解除），这个是对**请求速率/序号**的判定
//! （计数、按 venue、按时间自愈）。所以退避是指数的：同一个所连续报错
//! 说明上一次的等待还不够长。
//!
//! ## 一处刻意偏离参考
//!
//! 参考在退避期「暂停开仓和平仓」（`影响: ⏸️ 暂停开仓和平仓`）。这里
//! **只挡开仓**，理由和 `reduce_only` 里写的同一条：退避期仓位还裸在
//! 交易所，挡掉平仓等于把它锁死，比开不了仓危险得多。止损尤其不能挡——
//! 一个 nonce 错误换来 2 分钟不能止损，这笔账不划算。
//!
//! 参考能挡平仓是因为它的平仓也走同一套限流敏感的重试；我们的平仓由格子
//! 信号驱动，频率低，不构成限流压力。
//!
//! 状态纯内存，重启即清空——和参考一致。重启后第一次请求会立刻重新触发。

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// 需要退避的错误类型。对齐参考 `ErrorType`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffError {
    /// Lighter nonce 失序。
    InvalidNonce,
    /// 通用限流。
    RateLimit,
    /// L1 地址级限流。
    RateLimitL1,
}

impl BackoffError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidNonce => "21104",
            Self::RateLimit => "429",
            Self::RateLimitL1 => "23000",
        }
    }
}

/// 从错误文本里认出退避类错误。认不出返回 `None`——绝大多数执行失败
/// （滑点、深度不足、被拒）不该触发退避。
pub fn classify(msg: &str) -> Option<BackoffError> {
    let low = msg.to_lowercase();
    if low.contains("21104") || low.contains("invalid nonce") {
        return Some(BackoffError::InvalidNonce);
    }
    if low.contains("23000") {
        return Some(BackoffError::RateLimitL1);
    }
    // `too many requests` 要连着 429 一起认：单独的 "429" 可能是价格或
    // 数量里的数字，比如 "qty=429"。
    if low.contains("429") || low.contains("too many requests") || low.contains("rate limit") {
        return Some(BackoffError::RateLimit);
    }
    None
}

#[derive(Debug, Clone)]
pub struct VenueState {
    pub error: BackoffError,
    /// 连续错误次数。决定下次退避多久。
    pub count: u32,
    pub last_error: Instant,
    pub pause_until: Instant,
    pub pause_for: Duration,
    /// 恢复日志只打一次，避免每轮刷屏。
    recovery_logged: bool,
}

impl VenueState {
    pub fn remaining(&self, now: Instant) -> Duration {
        self.pause_until.saturating_duration_since(now)
    }
}

/// 退避参数。默认值取自参考的类常量。
#[derive(Debug, Clone, Copy)]
pub struct BackoffParams {
    pub min: Duration,
    pub max: Duration,
    pub multiplier: u32,
    /// 多久没错误就重置计数。
    pub reset_after: Duration,
}

impl Default for BackoffParams {
    fn default() -> Self {
        Self {
            min: Duration::from_secs(120),
            max: Duration::from_secs(3600),
            multiplier: 2,
            reset_after: Duration::from_secs(1800),
        }
    }
}

#[derive(Debug, Default)]
pub struct BackoffController {
    venues: HashMap<String, VenueState>,
    params: BackoffParams,
}

impl BackoffController {
    pub fn new(params: BackoffParams) -> Self {
        Self {
            venues: HashMap::new(),
            params,
        }
    }

    /// 热改退避参数，不清已有计数。
    pub fn set_params(&mut self, params: BackoffParams) {
        self.params = params;
    }

    pub fn is_empty(&self) -> bool {
        self.venues.is_empty()
    }

    pub fn get(&self, venue: &str) -> Option<&VenueState> {
        self.venues.get(&venue.to_lowercase())
    }

    /// 登记一次错误，返回本次退避时长。已在退避期内再报错会**累加计数并
    /// 延长**——这是指数退避的关键，不是「刷新同样长的窗口」。
    pub fn register(&mut self, venue: &str, err: BackoffError, now: Instant) -> Duration {
        let key = venue.to_lowercase();
        let count = match self.venues.get(&key) {
            // 距上次错误够久 → 视为新一轮，计数归零。
            Some(s) if now.duration_since(s.last_error) > self.params.reset_after => 1,
            Some(s) => s.count + 1,
            None => 1,
        };
        let pause_for = self.pause_for(count);
        self.venues.insert(
            key,
            VenueState {
                error: err,
                count,
                last_error: now,
                pause_until: now + pause_for,
                pause_for,
                recovery_logged: false,
            },
        );
        pause_for
    }

    /// `min × multiplier^(count−1)`，夹到 `max`。用 checked 幂避免
    /// 连续错误几十次时溢出。
    fn pause_for(&self, count: u32) -> Duration {
        let exp = count.saturating_sub(1);
        let factor = self
            .params
            .multiplier
            .checked_pow(exp.min(32))
            .unwrap_or(u32::MAX);
        self.params
            .min
            .saturating_mul(factor.max(1))
            .min(self.params.max)
    }

    /// 是否仍在退避期。顺带处理「刚恢复」的一次性日志。
    ///
    /// 恢复后**不删状态**：计数要留着给下一次退避加倍，只有超过
    /// `reset_after` 才归零。和参考同一条注释。
    pub fn is_paused(&mut self, venue: &str, now: Instant) -> bool {
        let key = venue.to_lowercase();
        let Some(state) = self.venues.get_mut(&key) else {
            return false;
        };
        if now < state.pause_until {
            return true;
        }
        if !state.recovery_logged {
            state.recovery_logged = true;
            tracing::info!(
                venue = %venue,
                code = state.error.code(),
                count = state.count,
                paused_secs = state.pause_for.as_secs(),
                "错误退避期结束，恢复开仓"
            );
        }
        false
    }

    /// 面板用：当前处于退避期的所及剩余秒数。
    pub fn paused_venues(&self, now: Instant) -> Vec<(String, &'static str, u64)> {
        let mut out: Vec<_> = self
            .venues
            .iter()
            .filter(|(_, s)| now < s.pause_until)
            .map(|(v, s)| (v.clone(), s.error.code(), s.remaining(now).as_secs()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctl() -> BackoffController {
        BackoffController::new(BackoffParams::default())
    }

    #[test]
    fn classifies_only_backoff_worthy_errors() {
        assert_eq!(classify("code=21104 invalid nonce"), Some(BackoffError::InvalidNonce));
        assert_eq!(classify("HTTP 429 Too Many Requests"), Some(BackoffError::RateLimit));
        assert_eq!(classify("code 23000"), Some(BackoffError::RateLimitL1));
        // 普通执行失败不该触发退避，否则一次滑点就停 2 分钟开仓。
        assert_eq!(classify("insufficient depth"), None);
        assert_eq!(classify("invalid reduce only mode"), None);
    }

    #[test]
    fn pause_grows_exponentially_and_clamps() {
        let c = ctl();
        assert_eq!(c.pause_for(1), Duration::from_secs(120));
        assert_eq!(c.pause_for(2), Duration::from_secs(240));
        assert_eq!(c.pause_for(3), Duration::from_secs(480));
        // 夹到上限，且极大计数不能溢出 panic。
        assert_eq!(c.pause_for(99), Duration::from_secs(3600));
    }

    #[test]
    fn repeat_error_extends_rather_than_refreshes() {
        let mut c = ctl();
        let t0 = Instant::now();
        assert_eq!(c.register("lighter", BackoffError::InvalidNonce, t0), Duration::from_secs(120));
        // 仍在退避期内又报错：计数累加，窗口加倍，不是原地刷新。
        let second = c.register("lighter", BackoffError::InvalidNonce, t0 + Duration::from_secs(10));
        assert_eq!(second, Duration::from_secs(240));
        assert_eq!(c.get("lighter").unwrap().count, 2);
    }

    #[test]
    fn quiet_period_resets_count() {
        let mut c = ctl();
        let t0 = Instant::now();
        c.register("lighter", BackoffError::RateLimit, t0);
        let later = t0 + Duration::from_secs(1801);
        assert_eq!(c.register("lighter", BackoffError::RateLimit, later), Duration::from_secs(120));
        assert_eq!(c.get("lighter").unwrap().count, 1);
    }

    #[test]
    fn pause_expires_but_count_survives() {
        let mut c = ctl();
        let t0 = Instant::now();
        c.register("lighter", BackoffError::InvalidNonce, t0);
        assert!(c.is_paused("lighter", t0 + Duration::from_secs(60)));
        let after = t0 + Duration::from_secs(121);
        assert!(!c.is_paused("lighter", after));
        // 计数保留：下一次错误要从 240s 起跳，而不是退回 120s。
        assert_eq!(c.register("lighter", BackoffError::InvalidNonce, after), Duration::from_secs(240));
    }

    #[test]
    fn venue_key_is_case_insensitive() {
        let mut c = ctl();
        let t0 = Instant::now();
        c.register("Lighter", BackoffError::RateLimit, t0);
        assert!(c.is_paused("lighter", t0));
    }

    #[test]
    fn unknown_venue_is_never_paused() {
        let mut c = ctl();
        assert!(!c.is_paused("sodex", Instant::now()));
    }
}
