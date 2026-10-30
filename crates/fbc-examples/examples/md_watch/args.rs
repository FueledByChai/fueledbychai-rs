//! md_watch's command line, parsed by hand (no argument-parsing dependency): which markets, how
//! to connect, how long to run and how much of each book to print.

use fbc_runtime::ProxyConfig;
use rust_decimal::Decimal;

/// The help text `--help` prints.
pub const USAGE: &str = "\
md_watch: prints live Paradex, Binance USD-M and Hibachi touches and top-of-book through
fbc-runtime. It never sends an order and never reads a credential.

Usage: cargo run -p fbc-examples --example md_watch -- [OPTIONS]

Examples:
  cargo run -p fbc-examples --example md_watch -- --paradex BTC-USD-PERP --binance BTCUSDT
  cargo run -p fbc-examples --example md_watch -- --hibachi ETH/USDT-P --binance ETHUSDT

Markets (at least one):
  --paradex <MARKET>      Paradex market, e.g. BTC-USD-PERP: its bbo, trades and deltas book
  --binance <SYMBOL>      Binance USD-M symbol, e.g. BTCUSDT: its bookTicker and diff-depth book
  --hibachi <SYMBOL>      Hibachi contract, e.g. ETH/USDT-P: its ask_bid_price touch, trades and
                          orderbook book from the anonymous /ws/market feed; its tick, step,
                          book granularity, status and session calendar are read first from
                          GET /market/exchange-info on --hibachi-rest

Options:
  --socks5 <HOST:PORT>    connect through this SOCKS5 proxy, which resolves the venues' names
                          (default: connect directly)
  --paradex-touch <WHICH> the Paradex market's touch: bbo (default, the bbo.<MARKET>
                          channel), interactive (bbo.<MARKET>.interactive, the touch
                          including RPI orders, printed as bbo.interactive) or both; the
                          interactive touch has a connection of its own
  --seconds <N>           stop after N seconds (default: run until Ctrl-C)
  --top <N>               under each book line, print the book's best N levels per side,
                          0 to 15 (default 0: the touch line only)
  --paradex-url <URL>     Paradex public WebSocket URL without a query
                          (default wss://ws.api.prod.paradex.trade/v1;
                          testnet: wss://ws.api.testnet.paradex.trade/v1)
  --binance-ws <URL>      Binance USD-M WebSocket origin (default wss://fstream.binance.com)
  --binance-rest <URL>    Binance USD-M REST origin, for the diff-depth book's snapshot
                          (default https://fapi.binance.com)
  --paradex-tick <DEC>    price grid the Paradex market is decoded on (default 0.00000001)
  --paradex-step <DEC>    size step the Paradex market is decoded on (default 0.00000001)
  --binance-tick <DEC>    price grid the Binance symbol is decoded on (default 0.00000001)
  --binance-step <DEC>    size step the Binance symbol is decoded on (default 0.00000001)
  --hibachi-rest <URL>    Hibachi market-data REST origin, read first for the contract and
                          then by the status poll every --hibachi-status
                          (default https://data-api.hibachi.xyz)
  --hibachi-ws <URL>      Hibachi market-data WebSocket origin; md_watch connects to /ws/market
                          on it (default wss://data-api.hibachi.xyz)
  --hibachi-timeout <MS>  how long the contract's REST read waits, in milliseconds
                          (default 5000)
  --hibachi-depth <N>     levels per side the orderbook subscription asks for, 1 to 100
                          (default 15, the deepest --top prints)
  --hibachi-granularity <DEC>
                          the orderbook subscription's granularity, in place of the contract's
                          quote grid (its finest listed orderbookGranularity at or above its
                          tick, or the tick): a whole number of its ticks, sent in its shortest
                          spelling (0.10 as 0.1); for checking that Hibachi honours the field
  --hibachi-retry <MS>    how long after finding the orderbook broken the codec subscribes it
                          again, and then waits for its Snapshot before reconnecting, in
                          milliseconds (default 2000)
  --hibachi-status <S>    how often the status poll re-reads GET /market/exchange-info for the
                          contract's trading status, in seconds (default 60)
  --hibachi-auction-halt <S>
                          how long the status poll reports the contract halted after each
                          reopening it knows of, for the call auction Hibachi may run then, in
                          seconds (default 900, the 15 minutes the call-auction docs' example
                          runs)
  -h, --help              print this help

A market is spelled as the venue spells it: a Paradex market in ASCII letters, digits and
'-', as typed (BTC-USD-PERP, kBONK-USD-PERP), a Binance symbol in capitals, digits and '-'
(BTCUSDT), a Hibachi contract in ASCII letters, digits, '/' and '-', as typed (ETH/USDT-P); a
frame for any other spelling would be refused, so md_watch refuses the spelling instead. A
price or size off its grid is refused as a decode error, and its frame prints nothing. The
default Paradex and Binance grids are the finest either venue publishes (8 decimal places), so
every market decodes without looking its grid up; giving the market's own tick and step only
makes the refusal stricter. A Hibachi contract's grids are its own, as Hibachi lists it, and a
contract it does not list is refused before anything connects.

Lines:
  TOUCH <venue> <market> <channel> bid <px> x <size> ask <px> x <size> mid <px> spread <bps>bps
  BOOK <venue> <market> <channel> ...   the book's touch, once per frame that changed what is shown
    <rank> bid <px> x <size> | ask <px> x <size>   under it, the book's best levels with --top
  TRADE <venue> <market> <buy|sell|unknown> <size> @ <px>
  HEALTH <venue> <market> <channel> <live|gap|stale|refused>
  MARKET <venue> <market> <open|closed ...>    a Hibachi contract's state: once its read is
                                               in, then whenever its session calendar opens or
                                               closes it (whether a closed contract's feed
                                               falls silent, and so reads stale, is not known
                                               yet: see the crate's README); its listed
                                               status is the start read's, so a contract
                                               listed closed then (PAUSED, or the exchange in
                                               maintenance) reads closed here all run, even
                                               once Hibachi reopens it: the STATUS lines show
                                               each change the status poll reads after it
  COUNTS <venue> <market> <counter> <n> ...    what the Hibachi codec counted on the current
                                               connection (frames and sides it passed over or
                                               found wrong, resubscribes, reconnects): a kind
                                               as it is first counted, then all of them before
                                               each RECONNECT or CLOSED line of the /ws/market
                                               connection (none when nothing was counted)
  STATUS <venue> <market> <trading|post-only|reduce-only|cancel-only|halted|delisted>
                                               the market's trading status, once each time
                                               it changes: for Hibachi, as the status poll
                                               reports it from its reads every
                                               --hibachi-status, halted between reads at a
                                               published maintenance window's begin, a
                                               session's end or an advertised close, and
                                               held halted for --hibachi-auction-halt after
                                               each reopening it knows of
  RECONNECT <venue> conn <n> epoch <e> ended   the connection dropped; its books are invalid
                                               until their next snapshot
  CLOSED <venue> conn <n> epoch <e> ended      md_watch stopped and closed the connection

md_watch stops once standard output is closed, so `md_watch ... | head` ends; any other
error writing a line also stops it, and is reported with a failing exit status.
";

/// The finest grid either venue publishes: Paradex's SBE prices and sizes are mantissas of
/// 10^-8, and Binance's decimal strings carry at most eight places.
pub const FINEST: &str = "0.00000001";

pub const PARADEX_URL: &str = "wss://ws.api.prod.paradex.trade/v1";
pub const BINANCE_WS: &str = "wss://fstream.binance.com";
pub const BINANCE_REST: &str = "https://fapi.binance.com";
/// Hibachi's market-data origins: the host its Market API spec names as its server, and the
/// origin its WebSocket market docs give `/ws/market` on (`fbc-venue-hibachi`'s config keys).
pub const HIBACHI_REST: &str = "https://data-api.hibachi.xyz";
pub const HIBACHI_WS: &str = "wss://data-api.hibachi.xyz";
/// How long the contract's REST read waits, as md_watch waits for Binance's snapshot.
pub const HIBACHI_TIMEOUT_MS: u64 = 5_000;
/// The orderbook subscription's depth: the deepest `--top`, so every level `--top` can print is
/// in the book.
pub const HIBACHI_DEPTH: u16 = 15;
/// The deepest orderbook subscription Hibachi takes (`fbc-venue-hibachi`'s
/// `hibachi.orderbook_depth`).
pub const HIBACHI_MAX_DEPTH: u16 = 100;
/// How long a broken orderbook waits for its resubscribe, as md_watch's Binance snapshot retry.
pub const HIBACHI_RETRY_MS: u64 = 2_000;
/// How often the status poll re-reads the contract's trading status (`fbc-venue-hibachi`'s
/// `hibachi.status_interval`, decision 0130): one request a minute against Hibachi's per-IP REST
/// limit.
pub const HIBACHI_STATUS_S: u64 = 60;
/// How long the status poll holds the contract halted after each reopening it knows of
/// (`hibachi.auction_halt`, decision 0130): the 15 minutes of the call auction in Hibachi's
/// call-auction docs' example (auction mode from 21:45 UTC, the uncross at 22:00), since no source
/// says which status a contract shows during one (FBC-vg2n).
pub const HIBACHI_AUCTION_HALT_S: u64 = 900;

/// The deepest `--top`: Paradex's book channel carries 15 levels per side.
pub const MAX_TOP: usize = 15;

/// One venue's market and the grid it is decoded on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Market {
    pub symbol: String,
    pub tick: Decimal,
    pub step: Decimal,
}

/// Which of a Paradex market's touch channels to watch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParadexTouch {
    /// `bbo.{market}`, the public book's touch.
    Bbo,
    /// `bbo.{market}.interactive`, the touch including RPI orders.
    Interactive,
    /// Both, each printed under its own channel name.
    Both,
}

/// A Hibachi contract and how its market data is read: everything `fbc-venue-hibachi`'s
/// configuration requires, and the orderbook granularity when the command line overrides the
/// contract's own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HibachiOpts {
    pub symbol: String,
    pub rest: String,
    pub ws: String,
    pub timeout_ms: u64,
    pub depth: u16,
    pub granularity: Option<Decimal>,
    pub retry_ms: u64,
    pub status_s: u64,
    pub auction_halt_s: u64,
}

/// What to watch and how.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    pub paradex: Option<Market>,
    pub paradex_touch: ParadexTouch,
    pub binance: Option<Market>,
    pub hibachi: Option<HibachiOpts>,
    pub proxy: ProxyConfig,
    pub seconds: Option<u64>,
    pub top: usize,
    pub paradex_url: String,
    pub binance_ws: String,
    pub binance_rest: String,
}

/// What the command line asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parsed {
    Help,
    Watch(Box<Options>),
}

/// Parses the arguments after the program name; an error says which argument is wrong.
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Parsed, String> {
    let mut paradex = None;
    let mut paradex_touch = ParadexTouch::Bbo;
    let mut binance = None;
    let mut proxy = ProxyConfig::Direct;
    let mut seconds = None;
    let mut top = 0;
    let mut paradex_url = PARADEX_URL.to_owned();
    let mut binance_ws = BINANCE_WS.to_owned();
    let mut binance_rest = BINANCE_REST.to_owned();
    let mut hibachi = None;
    let mut hibachi_rest = HIBACHI_REST.to_owned();
    let mut hibachi_ws = HIBACHI_WS.to_owned();
    let mut hibachi_timeout_ms = HIBACHI_TIMEOUT_MS;
    let mut hibachi_depth = HIBACHI_DEPTH;
    let mut hibachi_granularity = None;
    let mut hibachi_retry_ms = HIBACHI_RETRY_MS;
    let mut hibachi_status_s = HIBACHI_STATUS_S;
    let mut hibachi_auction_halt_s = HIBACHI_AUCTION_HALT_S;
    let finest: Decimal = FINEST.parse().expect("a decimal");
    let mut grids = [finest; 4];
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        if flag == "-h" || flag == "--help" {
            return Ok(Parsed::Help);
        }
        // A value that starts with `--` is the next flag: the value was left out.
        let mut value = || {
            args.next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--paradex" => paradex = Some(symbol(&flag, value()?, Spelling::Paradex)?),
            "--binance" => binance = Some(symbol(&flag, value()?, Spelling::Binance)?),
            "--hibachi" => hibachi = Some(symbol(&flag, value()?, Spelling::Hibachi)?),
            "--paradex-touch" => {
                paradex_touch = match value()?.as_str() {
                    "bbo" => ParadexTouch::Bbo,
                    "interactive" => ParadexTouch::Interactive,
                    "both" => ParadexTouch::Both,
                    other => {
                        return Err(format!(
                            "--paradex-touch {other}: not bbo, interactive or both"
                        ));
                    }
                }
            }
            "--socks5" => proxy = socks5(&value()?)?,
            "--seconds" => {
                let n = value()?;
                match n.parse::<u64>() {
                    Ok(n) if n > 0 => seconds = Some(n),
                    _ => return Err(format!("--seconds {n}: not a positive whole number")),
                }
            }
            "--top" => {
                let n = value()?;
                match n.parse::<usize>() {
                    Ok(n) if n <= MAX_TOP => top = n,
                    _ => return Err(format!("--top {n}: not a whole number from 0 to {MAX_TOP}")),
                }
            }
            "--paradex-url" => paradex_url = value()?,
            "--binance-ws" => binance_ws = value()?,
            "--binance-rest" => binance_rest = value()?,
            "--paradex-tick" => grids[0] = positive(&flag, &value()?)?,
            "--paradex-step" => grids[1] = positive(&flag, &value()?)?,
            "--binance-tick" => grids[2] = positive(&flag, &value()?)?,
            "--binance-step" => grids[3] = positive(&flag, &value()?)?,
            "--hibachi-rest" => hibachi_rest = value()?,
            "--hibachi-ws" => hibachi_ws = value()?,
            "--hibachi-timeout" => hibachi_timeout_ms = millis(&flag, &value()?)?,
            "--hibachi-depth" => {
                let n = value()?;
                match n.parse::<u16>() {
                    Ok(n) if (1..=HIBACHI_MAX_DEPTH).contains(&n) => hibachi_depth = n,
                    _ => {
                        return Err(format!(
                            "--hibachi-depth {n}: not a whole number from 1 to {HIBACHI_MAX_DEPTH}"
                        ));
                    }
                }
            }
            "--hibachi-granularity" => hibachi_granularity = Some(positive(&flag, &value()?)?),
            "--hibachi-retry" => hibachi_retry_ms = millis(&flag, &value()?)?,
            "--hibachi-status" => hibachi_status_s = secs(&flag, &value()?)?,
            "--hibachi-auction-halt" => hibachi_auction_halt_s = secs(&flag, &value()?)?,
            _ => return Err(format!("unknown argument {flag}; --help lists them")),
        }
    }
    if paradex.is_none() && binance.is_none() && hibachi.is_none() {
        return Err(
            "name a market: --paradex <MARKET>, --binance <SYMBOL>, --hibachi <SYMBOL>, or more \
             than one"
                .to_owned(),
        );
    }
    let market =
        |symbol: Option<String>, tick, step| symbol.map(|symbol| Market { symbol, tick, step });
    let hibachi = hibachi.map(|symbol| HibachiOpts {
        symbol,
        rest: hibachi_rest,
        ws: hibachi_ws,
        timeout_ms: hibachi_timeout_ms,
        depth: hibachi_depth,
        granularity: hibachi_granularity,
        retry_ms: hibachi_retry_ms,
        status_s: hibachi_status_s,
        auction_halt_s: hibachi_auction_halt_s,
    });
    Ok(Parsed::Watch(Box::new(Options {
        paradex: market(paradex, grids[0], grids[1]),
        paradex_touch,
        binance: market(binance, grids[2], grids[3]),
        hibachi,
        proxy,
        seconds,
        top,
        paradex_url,
        binance_ws,
        binance_rest,
    })))
}

/// How a venue spells its markets on the wire.
#[derive(Clone, Copy)]
enum Spelling {
    /// ASCII letters, digits and `-`, case-sensitive and passed through as typed: some
    /// markets carry a lowercase prefix, such as `kBONK-USD-PERP`.
    Paradex,
    /// Capitals, digits and `-`. Binance's frames name the symbol in capitals whatever the
    /// subscription said, and the spec table matches the name exactly, so `btcusdt` would
    /// subscribe and then have every frame refused unseen.
    Binance,
    /// ASCII letters, digits, `/` and `-`, passed through as typed: Hibachi spells every
    /// perpetual `<underlying>/<quote>-P` (`ETH/USDT-P`), and the contract's read refuses a
    /// spelling it does not list before anything subscribes.
    Hibachi,
}

/// A market as its venue spells it on the wire, or why it is not.
fn symbol(flag: &str, value: String, spelling: Spelling) -> Result<String, String> {
    let (letter, rule): (fn(&u8) -> bool, &str) = match spelling {
        Spelling::Paradex => (
            u8::is_ascii_alphabetic,
            "letters, digits and '-', e.g. BTC-USD-PERP or kBONK-USD-PERP",
        ),
        Spelling::Binance => (
            u8::is_ascii_uppercase,
            "capitals, digits and '-', e.g. BTCUSDT",
        ),
        Spelling::Hibachi => (
            |b: &u8| b.is_ascii_alphabetic() || *b == b'/',
            "letters, digits, '/' and '-', e.g. ETH/USDT-P",
        ),
    };
    if value
        .bytes()
        .all(|b| letter(&b) || b.is_ascii_digit() || b == b'-')
    {
        Ok(value)
    } else {
        Err(format!(
            "{flag} {value}: not a market as the venue spells it ({rule})"
        ))
    }
}

/// `host:port`, the port from 1 to 65535.
fn socks5(value: &str) -> Result<ProxyConfig, String> {
    let bad = || format!("--socks5 {value}: not host:port");
    let (host, port) = value.rsplit_once(':').ok_or_else(bad)?;
    let port = port
        .parse::<u16>()
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(bad)?;
    if host.is_empty() {
        return Err(bad());
    }
    Ok(ProxyConfig::Socks5 {
        host: host.to_owned(),
        port,
    })
}

/// A positive decimal, for a grid.
fn positive(flag: &str, value: &str) -> Result<Decimal, String> {
    match value.parse::<Decimal>() {
        Ok(d) if d > Decimal::ZERO => Ok(d),
        _ => Err(format!("{flag} {value}: not a positive decimal")),
    }
}

/// A positive whole number of milliseconds.
fn millis(flag: &str, value: &str) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(ms) if ms > 0 => Ok(ms),
        _ => Err(format!(
            "{flag} {value}: not a positive whole number of milliseconds"
        )),
    }
}

/// A positive whole number of seconds.
fn secs(flag: &str, value: &str) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(s) if s > 0 => Ok(s),
        _ => Err(format!(
            "{flag} {value}: not a positive whole number of seconds"
        )),
    }
}
