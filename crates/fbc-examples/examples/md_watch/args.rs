//! md_watch's command line, parsed by hand (no argument-parsing dependency): which markets, how
//! to connect, how long to run and how much of each book to print.

use fbc_runtime::ProxyConfig;
use rust_decimal::Decimal;

/// The help text `--help` prints.
pub const USAGE: &str = "\
md_watch: prints live Paradex and Binance USD-M touches and top-of-book through fbc-runtime.
It never sends an order and never reads a credential.

Usage: cargo run -p fbc-examples --example md_watch -- [OPTIONS]

Markets (at least one):
  --paradex <MARKET>      Paradex market, e.g. BTC-USD-PERP: its bbo, trades and deltas book
  --binance <SYMBOL>      Binance USD-M symbol, e.g. BTCUSDT: its bookTicker and diff-depth book

Options:
  --socks5 <HOST:PORT>    connect through this SOCKS5 proxy, which resolves the venues' names
                          (default: connect directly)
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
  -h, --help              print this help

A market is spelled as the venue spells it: a Paradex market in ASCII letters, digits and
'-', as typed (BTC-USD-PERP, kBONK-USD-PERP), a Binance symbol in capitals, digits and '-'
(BTCUSDT); a frame for any other spelling would be refused, so md_watch refuses the spelling
instead. A price or size off its grid is refused as a decode error, and its frame prints
nothing. The default grids are the finest either venue publishes (8 decimal places), so every
market decodes without looking its grid up; giving the market's own tick and step only makes
the refusal stricter.

Lines:
  TOUCH <venue> <market> <channel> bid <px> x <size> ask <px> x <size> mid <px> spread <bps>bps
  BOOK <venue> <market> <channel> ...   the book's touch, once per frame that changed what is shown
    <rank> bid <px> x <size> | ask <px> x <size>   under it, the book's best levels with --top
  TRADE <venue> <market> <buy|sell|unknown> <size> @ <px>
  HEALTH <venue> <market> <channel> <live|gap|stale|refused>
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

/// The deepest `--top`: Paradex's book channel carries 15 levels per side.
pub const MAX_TOP: usize = 15;

/// One venue's market and the grid it is decoded on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Market {
    pub symbol: String,
    pub tick: Decimal,
    pub step: Decimal,
}

/// What to watch and how.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    pub paradex: Option<Market>,
    pub binance: Option<Market>,
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
    let mut binance = None;
    let mut proxy = ProxyConfig::Direct;
    let mut seconds = None;
    let mut top = 0;
    let mut paradex_url = PARADEX_URL.to_owned();
    let mut binance_ws = BINANCE_WS.to_owned();
    let mut binance_rest = BINANCE_REST.to_owned();
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
            _ => return Err(format!("unknown argument {flag}; --help lists them")),
        }
    }
    if paradex.is_none() && binance.is_none() {
        return Err("name a market: --paradex <MARKET>, --binance <SYMBOL> or both".to_owned());
    }
    let market =
        |symbol: Option<String>, tick, step| symbol.map(|symbol| Market { symbol, tick, step });
    Ok(Parsed::Watch(Box::new(Options {
        paradex: market(paradex, grids[0], grids[1]),
        binance: market(binance, grids[2], grids[3]),
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
