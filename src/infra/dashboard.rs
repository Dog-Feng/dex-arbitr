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
    pub skip_wait: u64,
    /// 单所自身点差过宽（报价不可信 / 流动性差）。
    pub skip_wide: u64,
    /// 定仓算不出可下数量（保证金 / 深度 / 精度不够）。
    pub skip_size: u64,
    /// 拿不到第一腿 baseline 持仓，放弃本轮实盘执行。
    pub skip_baseline: u64,
    pub cancel_gone: u64,
    pub cancel_timeout: u64,
    pub late_hedge: u64,
}

impl IntentStats {
    pub fn bump_intent(&mut self, label: &str) {
        match label {
            "open" => self.open += 1,
            "close" | "scalp_tp" => self.close += 1,
            _ => self.hold += 1,
        }
    }

    pub fn bump_skip(&mut self, reason: &str) {
        match reason {
            "stale" => self.skip_stale += 1,
            "thin_book" => self.skip_thin += 1,
            "invalid_bbo" => self.skip_invalid += 1,
            "no_spread" => self.skip_spread += 1,
            "wide_book" => self.skip_wide += 1,
            "no_size" | "no_min_qty" | "no_margin" | "no_capacity" => self.skip_size += 1,
            "no_baseline" => self.skip_baseline += 1,
            _ => self.skip_wait += 1,
        }
    }

    fn lines(&self) -> [String; 2] {
        [
            format!(
                "decide  hold={}  open={}  close={}  skip_send={}",
                self.hold, self.open, self.close, self.skip_send
            ),
            format!(
                "filter  stale={}  thin={}  wide={}  spread={}  size={}  base={}  cancel_gone={}  cancel_to={}",
                self.skip_stale,
                self.skip_thin,
                self.skip_wide,
                self.skip_spread,
                self.skip_size,
                self.skip_baseline,
                self.cancel_gone,
                self.cancel_timeout,
            ),
        ]
    }
}

/// 交互终端进备用屏，每帧从顶部重绘。不依赖 ESC[s / 光标回退（很多 Linux 终端不认）。
pub struct LivePanel {
    enabled: bool,
    rows: Vec<String>,
    pub stats: IntentStats,
    pub scan_mode: bool,
    pub scan_line: String,
    pub skip_line: String,
    last_paint: Option<Instant>,
    #[cfg(windows)]
    conout: Option<std::fs::File>,
}

impl LivePanel {
    pub fn new(rows: usize) -> Self {
        if rows == 0 {
            return Self {
                enabled: false,
                rows: Vec::new(),
                stats: IntentStats::default(),
                scan_mode: false,
                scan_line: String::new(),
                skip_line: String::new(),
                last_paint: None,
                #[cfg(windows)]
                conout: None,
            };
        }

        #[cfg(windows)]
        let conout = win_con::open();
        #[cfg(windows)]
        let enabled = conout.is_some();
        #[cfg(not(windows))]
        let enabled = io::IsTerminal::is_terminal(&io::stdout());

        if enabled {
            // 备用屏 + 关自动折行 + 藏光标。退出时在 Drop 里还原。
            let _ = write_raw("\x1b[?1049h\x1b[?7l\x1b[?25l");
        }
        Self {
            enabled,
            rows: vec![String::new(); rows],
            stats: IntentStats::default(),
            scan_mode: false,
            scan_line: String::new(),
            skip_line: String::new(),
            last_paint: None,
            #[cfg(windows)]
            conout,
        }
    }

    pub fn set(&mut self, idx: usize, line: String) {
        if let Some(slot) = self.rows.get_mut(idx) {
            *slot = line;
        }
    }

    pub fn resize(&mut self, rows: usize) {
        self.rows.resize(rows, String::new());
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

        let mut block = Vec::with_capacity(self.rows.len() + 3);
        if self.scan_mode {
            block.push(self.scan_line.clone());
            block.push(self.skip_line.clone());
        } else {
            block.extend(self.stats.lines());
        }
        block.push(String::new());
        for row in &self.rows {
            block.push(row.clone());
        }

        #[cfg(windows)]
        {
            if let Some(file) = self.conout.as_mut() {
                win_con::paint(file, &block);
                return;
            }
        }

        paint_ansi(&block);
    }
}

impl Drop for LivePanel {
    fn drop(&mut self) {
        if self.enabled {
            let _ = write_raw("\x1b[?25h\x1b[?7h\x1b[?1049l");
        }
    }
}

fn paint_ansi(lines: &[String]) {
    let cols = term_cols();
    let mut out = io::stdout();
    // 备用屏左上角清空再画，不会往主屏下面追加。
    let _ = write!(out, "\x1b[H\x1b[J");
    for line in lines {
        let _ = writeln!(out, "{}", fit_line(line, cols));
    }
    let _ = out.flush();
}

fn term_cols() -> usize {
    #[cfg(windows)]
    {
        if let Some(n) = win_con::width() {
            return n;
        }
    }
    #[cfg(unix)]
    {
        if let Some(n) = unix_cols() {
            return n;
        }
    }
    120
}

fn fit_line(line: &str, cols: usize) -> String {
    let max = cols.saturating_sub(1).max(20);
    let n = line.chars().count();
    if n <= max {
        return line.to_string();
    }
    let mut s: String = line.chars().take(max.saturating_sub(1)).collect();
    s.push('…');
    s
}

#[cfg(unix)]
fn unix_cols() -> Option<usize> {
    #[repr(C)]
    struct Winsize {
        row: u16,
        col: u16,
        x: u16,
        y: u16,
    }
    extern "C" {
        fn ioctl(fd: i32, req: std::os::raw::c_ulong, ws: *mut Winsize) -> i32;
    }
    // Linux TIOCGWINSZ. macOS uses a different request; COLUMNS 作兜底。
    const TIOCGWINSZ: std::os::raw::c_ulong = 0x5413;
    let mut ws = Winsize {
        row: 0,
        col: 0,
        x: 0,
        y: 0,
    };
    unsafe {
        if ioctl(1, TIOCGWINSZ, &mut ws) == 0 && ws.col > 0 {
            return Some(ws.col as usize);
        }
    }
    std::env::var("COLUMNS").ok()?.parse().ok()
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
    let v = v.round_dp(4);
    if v < Decimal::ZERO {
        format!("{v:.4}%")
    } else {
        format!("+{v:.4}%")
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
    format!("{venue:<10} {pair:<12} bid={bid}  ask={ask}  bq={bid_qty}  aq={ask_qty}")
}

/// 价差拆成两短行，避免 80 列终端折行后把旧的 intent 留在屏幕上。
pub fn spread_lines(
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
) -> [String; 2] {
    let nat = natural.map(fmt_pct).unwrap_or_else(|| "--".to_string());
    let pts = if natural.is_some() {
        points.to_string()
    } else {
        format!("{points}/{min_points}")
    };
    [
        format!(
            "{}  {}  {}  {}  {}",
            kv("pair", pair, 12),
            kv("buy", buy, 10),
            kv("sell", sell, 10),
            kv("intent", intent, 8),
            kv("pts", &pts, 7)
        ),
        format!(
            "{}  {}  {}  {}  {}",
            kv("raw", &fmt_pct(raw), 10),
            kv("slip", &fmt_pct(slip), 10),
            kv("net", &fmt_pct(net), 10),
            kv("nat", &nat, 10),
            kv("res", &fmt_pct(residual), 10)
        ),
    ]
}

pub fn scan_header(universe: usize, ready: usize, opp: usize, min_raw: Decimal) -> String {
    format!(
        "scan  universe={}  ready={}  opp={}  min_raw={:.2}%",
        universe,
        ready,
        opp,
        min_raw
    )
}

pub fn scan_skip_line(wait: usize, stale: usize, invalid: usize, cross_hold: usize) -> String {
    format!("skip  wait={wait}  stale={stale}  invalid={invalid}  cross_hold={cross_hold}")
}

/// 控制台和文件共用的代币关键行。
pub fn token_key_line(
    pair: &str,
    buy: &str,
    sell: &str,
    raw: Decimal,
    nat: Option<Decimal>,
    residual: Decimal,
    cross_dex: bool,
    age_secs: f64,
) -> String {
    if cross_dex {
        format!(
            "pair={} buy={} sell={} raw={} nat={} res={} age={:.1}s",
            pair,
            buy,
            sell,
            fmt_pct(raw),
            nat.map(fmt_pct).unwrap_or_else(|| "-".into()),
            fmt_pct(residual),
            age_secs
        )
    } else {
        format!(
            "pair={} buy={} sell={} raw={} age={:.1}s",
            pair,
            buy,
            sell,
            fmt_pct(raw),
            age_secs
        )
    }
}

pub fn token_gone_line(pair: &str, buy: &str, sell: &str) -> String {
    format!("pair={pair} buy={buy} sell={sell} gone")
}

pub fn scan_opp_line(
    rank: usize,
    pair: &str,
    buy: &str,
    sell: &str,
    raw: Decimal,
    nat: Option<Decimal>,
    residual: Decimal,
    cross_dex: bool,
    age_secs: f64,
) -> String {
    if cross_dex {
        format!(
            "{rank:<2}  pair={:<14}  buy={:<12}  sell={:<12}  raw={}  nat={}  res={}  age={:.1}s",
            pair,
            buy,
            sell,
            fmt_pct(raw),
            nat.map(|v| fmt_pct(v)).unwrap_or_else(|| "-".into()),
            fmt_pct(residual),
            age_secs
        )
    } else {
        format!(
            "{rank:<2}  pair={:<14}  buy={:<12}  sell={:<12}  raw={}  age={:.1}s",
            pair,
            buy,
            sell,
            fmt_pct(raw),
            age_secs
        )
    }
}

pub fn skip_lines(pair: &str, reason: &str) -> [String; 2] {
    [
        format!(
            "{}  {}  {}",
            kv("pair", pair, 12),
            kv("intent", "skip", 8),
            kv("note", reason, 12)
        ),
        String::new(),
    ]
}

#[cfg(windows)]
mod win_con {
    use super::fit_line;
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

    pub fn width() -> Option<usize> {
        let file = open()?;
        let info = info(&file)?;
        let cols = info.size.x as usize;
        (cols > 0).then_some(cols)
    }

    fn info(file: &File) -> Option<ConsoleScreenBufferInfo> {
        unsafe {
            let h = file.as_raw_handle() as isize;
            let mut info = std::mem::zeroed();
            if GetConsoleScreenBufferInfo(h, &mut info) == 0 {
                return None;
            }
            Some(info)
        }
    }

    pub fn paint(file: &mut File, lines: &[String]) {
        unsafe {
            let h = file.as_raw_handle() as isize;
            let _ = SetConsoleCursorPosition(h, Coord { x: 0, y: 0 });
        }
        let cols = info(file).map(|i| i.size.x as usize).unwrap_or(120);
        let _ = write!(file, "\x1b[H\x1b[J");
        for line in lines {
            let _ = writeln!(file, "{}", fit_line(line, cols));
        }
        let _ = file.flush();
    }
}
