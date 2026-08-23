use rust_decimal::Decimal;
use std::io::{self, Write};
use std::time::{Duration, Instant};

#[derive(Debug, Default, Clone)]
pub struct IntentStats {
    pub hold: u64,
    pub open: u64,
    pub close: u64,
    pub skip_send: u64,
    pub skip_stale: u64,
    pub skip_thin: u64,
    pub skip_invalid: u64,
    pub skip_spread: u64,
    pub skip_depeg: u64,
    pub skip_wait: u64,
    pub cancel_gone: u64,
    pub cancel_timeout: u64,
    pub late_hedge: u64,
}

impl IntentStats {
    pub fn bump_intent(&mut self, label: &str) {
        match label {
            "open" => self.open += 1,
            "close" => self.close += 1,
            _ => self.hold += 1,
        }
    }

    pub fn bump_skip(&mut self, reason: &str) {
        match reason {
            "stale" => self.skip_stale += 1,
            "thin_book" => self.skip_thin += 1,
            "invalid_bbo" => self.skip_invalid += 1,
            "no_spread" => self.skip_spread += 1,
            "depeg" => self.skip_depeg += 1,
            _ => self.skip_wait += 1,
        }
    }

    fn line(&self) -> String {
        format!(
            "intent  hold={:<6} open={:<4} close={:<4} skip_send={:<4} skip_stale={:<4} skip_thin={:<4} skip_spread={:<4} cancel_gone={:<4} cancel_to={:<4} late_hedge={:<4}",
            self.hold,
            self.open,
            self.close,
            self.skip_send,
            self.skip_stale,
            self.skip_thin,
            self.skip_spread,
            self.cancel_gone,
            self.cancel_timeout,
            self.late_hedge
        )
    }
}

/// 固定行数原地刷新。Windows 写 CONOUT$，避免 cargo 管道把回退光标吃掉。
pub struct LivePanel {
    enabled: bool,
    rows: Vec<String>,
    pub stats: IntentStats,
    last_paint: Option<Instant>,
    origin_y: Option<i16>,
    painted: bool,
    #[cfg(windows)]
    conout: Option<std::fs::File>,
}

impl LivePanel {
    pub fn new(rows: usize) -> Self {
        #[cfg(windows)]
        let conout = win_con::open();
        #[cfg(windows)]
        let enabled = conout.is_some();
        #[cfg(not(windows))]
        let enabled = io::IsTerminal::is_terminal(&io::stdout());

        if enabled {
            let _ = write_raw("\x1b[?25l");
        }
        Self {
            enabled,
            rows: vec![String::new(); rows],
            stats: IntentStats::default(),
            last_paint: None,
            origin_y: None,
            painted: false,
            #[cfg(windows)]
            conout,
        }
    }

    pub fn set(&mut self, idx: usize, line: String) {
        if let Some(slot) = self.rows.get_mut(idx) {
            *slot = line;
        }
    }

    pub fn flush(&mut self) {
        if !self.enabled {
            return;
        }
        if self
            .last_paint
            .is_some_and(|t| t.elapsed() < Duration::from_millis(250))
        {
            return;
        }
        self.last_paint = Some(Instant::now());

        let mut block = Vec::with_capacity(self.rows.len() + 2);
        block.push(self.stats.line());
        block.push(String::new());
        for row in &self.rows {
            block.push(row.clone());
        }

        #[cfg(windows)]
        {
            if let Some(file) = self.conout.as_mut() {
                if self.origin_y.is_none() {
                    self.origin_y = win_con::cursor_y(file);
                }
                if let Some(y) = self.origin_y {
                    win_con::paint(file, y, &block);
                    return;
                }
            }
        }

        paint_ansi(&block, self.painted);
        self.painted = true;
    }
}

impl Drop for LivePanel {
    fn drop(&mut self) {
        if self.enabled {
            let _ = write_raw("\x1b[?25h");
        }
    }
}

fn paint_ansi(lines: &[String], painted: bool) {
    let mut out = io::stdout();
    if painted {
        let _ = write!(out, "\x1b[{}A", lines.len());
    }
    for line in lines {
        let _ = write!(out, "\x1b[2K{}\n", pad_line(line));
    }
    let _ = out.flush();
}

fn pad_line(line: &str) -> String {
    format!("{line:<160}")
}

fn write_raw(s: &str) {
    #[cfg(windows)]
    if let Some(mut f) = win_con::open() {
        let _ = f.write_all(s.as_bytes());
        let _ = f.flush();
        return;
    }
    let _ = io::stdout().write_all(s.as_bytes());
    let _ = io::stdout().flush();
}

pub fn fmt_pct(v: Decimal) -> String {
    let v = v.round_dp(6);
    if v < Decimal::ZERO {
        format!("{v:.6}%")
    } else {
        format!("+{v:.6}%")
    }
}

fn kv(key: &str, val: &str, width: usize) -> String {
    format!("{key}={val:<width$}")
}

pub fn book_line(
    venue: &str,
    pair: &str,
    bid: Decimal,
    ask: Decimal,
    bid_qty: Decimal,
    ask_qty: Decimal,
) -> String {
    [
        kv("venue", venue, 10),
        kv("pair", pair, 12),
        kv("bid", &format!("{bid}"), 12),
        kv("ask", &format!("{ask}"), 12),
        kv("bid_qty", &format!("{bid_qty}"), 10),
        kv("ask_qty", &format!("{ask_qty}"), 10),
    ]
    .join("  ")
}

pub fn spread_line(
    pair: &str,
    buy: &str,
    sell: &str,
    raw: Decimal,
    net: Decimal,
    slip: Decimal,
    natural: Option<Decimal>,
    residual: Decimal,
    points: usize,
    min_points: usize,
    intent: &str,
) -> String {
    let nat = natural.map(fmt_pct).unwrap_or_else(|| "--".to_string());
    let pts = if natural.is_some() {
        points.to_string()
    } else {
        format!("{points}/{min_points}")
    };
    [
        kv("pair", pair, 12),
        kv("buy", buy, 10),
        kv("sell", sell, 10),
        kv("raw", &fmt_pct(raw), 11),
        kv("slip", &fmt_pct(slip), 11),
        kv("net", &fmt_pct(net), 11),
        kv("nat", &nat, 11),
        kv("res", &fmt_pct(residual), 11),
        kv("pts", &pts, 7),
        kv("intent", intent, 8),
    ]
    .join("  ")
}

pub fn skip_line(pair: &str, reason: &str) -> String {
    format!(
        "{}  {}  {}",
        kv("pair", pair, 12),
        kv("intent", "skip", 8),
        kv("note", reason, 12)
    )
}

#[cfg(windows)]
mod win_con {
    use super::pad_line;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;

    const FILE_SHARE_READ: u32 = 1;
    const FILE_SHARE_WRITE: u32 = 2;
    const ENABLE_VIRTUAL_TERMINAL_PROCESSING: u32 = 0x0004;

    #[repr(C)]
    struct Coord {
        x: i16,
        y: i16,
    }

    #[repr(C)]
    struct SmallRect {
        left: i16,
        top: i16,
        right: i16,
        bottom: i16,
    }

    #[repr(C)]
    struct ConsoleScreenBufferInfo {
        size: Coord,
        cursor: Coord,
        attrs: u16,
        window: SmallRect,
        max: Coord,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GetConsoleMode(h: isize, mode: *mut u32) -> i32;
        fn SetConsoleMode(h: isize, mode: u32) -> i32;
        fn GetConsoleScreenBufferInfo(h: isize, info: *mut ConsoleScreenBufferInfo) -> i32;
        fn SetConsoleCursorPosition(h: isize, pos: Coord) -> i32;
    }

    pub fn open() -> Option<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .open(r"\\.\CONOUT$")
            .ok()?;
        unsafe {
            let h = file.as_raw_handle() as isize;
            let mut mode = 0u32;
            if GetConsoleMode(h, &mut mode) != 0 {
                let _ = SetConsoleMode(h, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
            }
        }
        Some(file)
    }

    pub fn cursor_y(file: &File) -> Option<i16> {
        unsafe {
            let h = file.as_raw_handle() as isize;
            let mut info = std::mem::zeroed();
            if GetConsoleScreenBufferInfo(h, &mut info) == 0 {
                return None;
            }
            Some(info.cursor.y)
        }
    }

    pub fn paint(file: &mut File, origin_y: i16, lines: &[String]) {
        unsafe {
            let h = file.as_raw_handle() as isize;
            let _ = SetConsoleCursorPosition(h, Coord { x: 0, y: origin_y });
        }
        for line in lines {
            let _ = write!(file, "{}\r\n", pad_line(line));
        }
        let _ = file.flush();
    }
}
