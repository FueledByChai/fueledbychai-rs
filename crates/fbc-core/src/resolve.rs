//! Instrument resolution: from what a venue lists to the core's [`InstrumentId`] (decisions 0003
//! and 0004, design §4.4).
//!
//! A venue's discovery ([`VenueFactory::discover`](crate::VenueFactory::discover)) states each
//! instrument it lists as an [`InstrumentSpecDraft`]: every field of an [`InstrumentSpec`] the
//! venue states, and what the instrument is a contract on as an [`AssetKey`] (base, quote, kind)
//! in the venue's own spelling. The draft's fields are all mandatory, like the spec's, so a
//! draft missing one does not compile; a discovery parser that finds a required field missing
//! from the venue's answer refuses the whole answer, naming the field
//! ([`PlanError::Missing`](crate::PlanError::Missing)). Nothing is read from a symbol's prefix
//! and nothing has a default.
//!
//! The core numbers instruments; venues do not. An [`InstrumentResolver`] holds the consumer's
//! [`Listing`]s by venue and asset key, and resolves `(VenueId, AssetKey)` to an
//! [`InstrumentId`] through an explicit [`AliasTable`]: both the listed key and the key asked
//! for are read with every asset replaced by its canonical name, so `BTC/USDT` and `BTC/USDC`
//! name the `BTC/USD` listing under the seeded table (`USDT=USD`, `USDC=USD`, `GOLD=XAU`), which
//! the consumer's configuration overrides.
//!
//! The Java-era ticker values (`BTC/USDT`) reach the same key through each venue's
//! [`parse_fbc_common_symbol`](crate::VenueFactory::parse_fbc_common_symbol), which reads FBC's
//! per-venue rule; [`common_symbol_parts`] splits the common form every rule starts from.

use core::fmt;
use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::fee::FeeSchedule;
use crate::grid::PriceGrid;
use crate::ids::{InstrumentId, UnderlyingId, VenueId, VenueSymbol};
use crate::instrument::{
    FundingSpec, InstrumentKind, InstrumentSpec, SizeStep, TradingStatus, VenueNativeId,
};
use crate::time::WallNs;
use crate::units::{AssetSym, Bps, Lots, Money};

/// What an instrument is a contract on, independent of how its venue spells it: a base asset,
/// a quote asset and a kind. A venue states it in its own spelling (`USDC`, `USDT`); the
/// [`AliasTable`] relates the spellings.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct AssetKey {
    pub base: AssetSym,
    pub quote: AssetSym,
    pub kind: InstrumentKind,
}

impl AssetKey {
    /// The key with its base and quote replaced by their canonical names under `aliases`.
    pub fn canonical(self, aliases: &AliasTable) -> AssetKey {
        AssetKey {
            base: aliases.canonical(self.base),
            quote: aliases.canonical(self.quote),
            kind: self.kind,
        }
    }
}

/// Why a Java-era ticker could not be read as an asset key.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SymbolError {
    /// Not in FBC's common form, `BASE/QUOTE`, with both assets of one to eight printable ASCII
    /// characters.
    NotCommonForm,
    /// In the common form, but the venue's FBC rule maps it to no instrument (Paradex reads only
    /// `X/USDT`).
    Unmapped,
    /// The venue has no FBC common-symbol rule: the Java library never traded it.
    NoRule,
}

impl fmt::Display for SymbolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SymbolError::NotCommonForm => "the ticker is not in FBC's BASE/QUOTE form",
            SymbolError::Unmapped => "the venue's FBC rule maps the ticker to no instrument",
            SymbolError::NoRule => "the venue has no FBC common-symbol rule",
        })
    }
}

impl std::error::Error for SymbolError {}

/// The base and quote of a ticker in FBC's common form, `BASE/QUOTE` (`BTC/USDT`), as written:
/// the form every venue's FBC rule starts from. Refused unless it has exactly one `/` and both
/// sides are valid asset symbols.
pub fn common_symbol_parts(ticker: &str) -> Result<(AssetSym, AssetSym), SymbolError> {
    let (base, quote) = ticker.split_once('/').ok_or(SymbolError::NotCommonForm)?;
    match (AssetSym::new(base), AssetSym::new(quote)) {
        (Some(base), Some(quote)) if !quote.as_str().contains('/') => Ok((base, quote)),
        _ => Err(SymbolError::NotCommonForm),
    }
}

/// Which asset names mean the same asset: each alias maps to its canonical name. One hop only:
/// a canonical name is never itself an alias, so reading a name twice gives what reading it
/// once gave.
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct AliasTable {
    to: HashMap<AssetSym, AssetSym>,
}

/// Why an alias was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum AliasError {
    /// A name aliased to itself.
    ToItself,
    /// The alias would make a chain: its target is itself an alias, or its name is already the
    /// target of another alias.
    Chain,
}

impl fmt::Display for AliasError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AliasError::ToItself => "an asset cannot be an alias of itself",
            AliasError::Chain => "an alias's target cannot itself be an alias",
        })
    }
}

impl std::error::Error for AliasError {}

impl AliasTable {
    /// No aliases: every name is canonical.
    pub fn empty() -> AliasTable {
        AliasTable::default()
    }

    /// The defaults design §4.4 seeds: `USDT=USD`, `USDC=USD`, `GOLD=XAU`. The consumer's
    /// configuration overrides them ([`set`](AliasTable::set), [`remove`](AliasTable::remove)).
    pub fn seeded() -> AliasTable {
        let mut table = AliasTable::empty();
        for (from, to) in [("USDT", "USD"), ("USDC", "USD"), ("GOLD", "XAU")] {
            let pair = AssetSym::new(from).zip(AssetSym::new(to));
            let (from, to) = pair.expect("the seeded names are valid asset symbols");
            table.to.insert(from, to);
        }
        table
    }

    /// Makes `from` an alias of `to`, returning the name it replaced. Refused when `from` is
    /// `to`, `to` is an alias, or `from` is the target of another alias.
    pub fn set(&mut self, from: AssetSym, to: AssetSym) -> Result<Option<AssetSym>, AliasError> {
        if from == to {
            return Err(AliasError::ToItself);
        }
        if self.to.contains_key(&to) || self.to.values().any(|target| *target == from) {
            return Err(AliasError::Chain);
        }
        Ok(self.to.insert(from, to))
    }

    /// Makes `from` canonical again, returning the name it was an alias of.
    pub fn remove(&mut self, from: AssetSym) -> Option<AssetSym> {
        self.to.remove(&from)
    }

    /// The canonical name of `sym`: its target when it is an alias, otherwise itself.
    pub fn canonical(&self, sym: AssetSym) -> AssetSym {
        self.to.get(&sym).copied().unwrap_or(sym)
    }
}

/// The consumer's numbering of one instrument: its id, and the underlying it nets against on
/// other venues.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Listing {
    pub id: InstrumentId,
    pub underlying: UnderlyingId,
}

/// Why an asset key or a draft could not be resolved, or a listing was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ResolveError {
    /// Nothing is listed on `venue` under `key`'s canonical form: the consumer did not list the
    /// instrument, and none is guessed.
    NotListed { venue: VenueId, key: AssetKey },
    /// `venue` already lists an instrument under `key`'s canonical form.
    AlreadyListed { venue: VenueId, key: AssetKey },
    /// `venue` already lists `id` under another key.
    IdTaken { venue: VenueId, id: InstrumentId },
}

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pair = |key: &AssetKey| format!("{}/{}", key.base.as_str(), key.quote.as_str());
        match self {
            ResolveError::NotListed { venue, key } => write!(
                f,
                "venue {} lists no {} {:?}",
                venue.get(),
                pair(key),
                key.kind
            ),
            ResolveError::AlreadyListed { venue, key } => write!(
                f,
                "venue {} already lists {} {:?}",
                venue.get(),
                pair(key),
                key.kind
            ),
            ResolveError::IdTaken { venue, id } => write!(
                f,
                "venue {} already lists instrument {} under another key",
                venue.get(),
                id.get()
            ),
        }
    }
}

impl std::error::Error for ResolveError {}

/// Resolves `(VenueId, AssetKey)` to the consumer's [`Listing`] through an [`AliasTable`] fixed
/// when it is built, so a key listed and a key asked for are always read under the same
/// aliases.
#[derive(Clone, Debug)]
pub struct InstrumentResolver {
    aliases: AliasTable,
    listed: HashMap<(VenueId, AssetKey), Listing>,
}

impl InstrumentResolver {
    /// A resolver with no listings, reading keys under `aliases`.
    pub fn new(aliases: AliasTable) -> InstrumentResolver {
        InstrumentResolver {
            aliases,
            listed: HashMap::new(),
        }
    }

    /// The aliases keys are read under.
    pub fn aliases(&self) -> &AliasTable {
        &self.aliases
    }

    /// Lists `listing` on `venue` under `key`'s canonical form. Refused when the venue already
    /// lists that form, or lists the listing's id under another.
    pub fn list(
        &mut self,
        venue: VenueId,
        key: AssetKey,
        listing: Listing,
    ) -> Result<(), ResolveError> {
        let key = key.canonical(&self.aliases);
        if self.listed.contains_key(&(venue, key)) {
            return Err(ResolveError::AlreadyListed { venue, key });
        }
        let taken = self
            .listed
            .iter()
            .any(|(&(v, _), l)| v == venue && l.id == listing.id);
        if taken {
            return Err(ResolveError::IdTaken {
                venue,
                id: listing.id,
            });
        }
        self.listed.insert((venue, key), listing);
        Ok(())
    }

    /// The listing on `venue` of `key`'s canonical form.
    pub fn listing(&self, venue: VenueId, key: AssetKey) -> Result<Listing, ResolveError> {
        let key = key.canonical(&self.aliases);
        let listing = self.listed.get(&(venue, key)).copied();
        listing.ok_or(ResolveError::NotListed { venue, key })
    }

    /// The instrument `venue` lists under `key`'s canonical form.
    pub fn resolve(&self, venue: VenueId, key: AssetKey) -> Result<InstrumentId, ResolveError> {
        self.listing(venue, key).map(|listing| listing.id)
    }

    /// The spec of the instrument `draft` describes on `venue`: its listing's id and underlying,
    /// the draft's fields, and `version` and `fetched_at` from whoever ran the discovery (a
    /// parser reads no clock). Refused when the venue lists nothing under the draft's key.
    pub fn spec(
        &self,
        venue: VenueId,
        draft: InstrumentSpecDraft,
        version: u32,
        fetched_at: WallNs,
    ) -> Result<InstrumentSpec, ResolveError> {
        let listing = self.listing(venue, draft.asset)?;
        Ok(InstrumentSpec {
            id: listing.id,
            venue,
            venue_symbol: draft.venue_symbol,
            native_id: draft.native_id,
            underlying: listing.underlying,
            kind: draft.asset.kind,
            price_grid: draft.price_grid,
            quote_grid: draft.quote_grid,
            size_step: draft.size_step,
            min_size: draft.min_size,
            min_notional: draft.min_notional,
            max_order_size: draft.max_order_size,
            position_limit: draft.position_limit,
            price_band: draft.price_band,
            max_open_orders: draft.max_open_orders,
            multiplier: draft.multiplier,
            quote_ccy: draft.asset.quote,
            settle_ccy: draft.settle_ccy,
            funding: draft.funding,
            public_fees: draft.public_fees,
            status: draft.status,
            version,
            fetched_at,
        })
    }
}

/// One instrument as its venue's discovery states it: an [`InstrumentSpec`] without what the
/// core assigns (id, venue, underlying, version, fetch time), and with what it is a contract on
/// as an [`AssetKey`], whose quote and kind are the spec's quote currency and kind.
///
/// No `Default`, and every field mandatory: a draft missing a field does not compile, and a
/// value the venue does not state is said explicitly (`None`, [`FundingSpec::Unknown`]).
/// Its [`VenueSymbol`] comes only from a [`DecodeScope`](crate::DecodeScope), so a draft is
/// built only inside one.
#[derive(Clone, PartialEq, Debug)]
pub struct InstrumentSpecDraft {
    /// What the instrument is a contract on, in the venue's spelling.
    pub asset: AssetKey,
    pub venue_symbol: VenueSymbol,
    pub native_id: Option<VenueNativeId>,
    pub price_grid: PriceGrid,
    pub quote_grid: Option<Decimal>,
    pub size_step: SizeStep,
    pub min_size: Lots,
    pub min_notional: Option<Money>,
    pub max_order_size: Option<Lots>,
    pub position_limit: Option<Lots>,
    pub price_band: Option<Bps>,
    pub max_open_orders: Option<u32>,
    pub multiplier: Decimal,
    pub settle_ccy: AssetSym,
    pub funding: FundingSpec,
    pub public_fees: Option<FeeSchedule>,
    pub status: TradingStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym(s: &str) -> AssetSym {
        AssetSym::new(s).unwrap()
    }

    fn perp(base: &str, quote: &str) -> AssetKey {
        AssetKey {
            base: sym(base),
            quote: sym(quote),
            kind: InstrumentKind::Perpetual,
        }
    }

    const VENUE: VenueId = VenueId::new(3);

    fn listing(n: u32) -> Listing {
        Listing {
            id: InstrumentId::new(n),
            underlying: UnderlyingId::new(n + 100),
        }
    }

    #[test]
    fn the_common_form_is_base_slash_quote_with_valid_assets() {
        assert_eq!(
            common_symbol_parts("BTC/USDT"),
            Ok((sym("BTC"), sym("USDT")))
        );
        for bad in [
            "BTCUSDT",
            "BTC/",
            "/USDT",
            "A/B/C",
            "BTC/TOOLONGQUOTE",
            "B TC/USD",
            "",
        ] {
            assert_eq!(
                common_symbol_parts(bad),
                Err(SymbolError::NotCommonForm),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_seeded_table_maps_the_stablecoins_to_usd_and_gold_to_xau() {
        let table = AliasTable::seeded();
        for (from, to) in [
            ("USDT", "USD"),
            ("USDC", "USD"),
            ("GOLD", "XAU"),
            ("BTC", "BTC"),
        ] {
            assert_eq!(table.canonical(sym(from)), sym(to), "{from}");
        }
        assert_eq!(AliasTable::empty().canonical(sym("USDT")), sym("USDT"));
    }

    #[test]
    fn aliases_are_overridable_and_never_chain() {
        let mut table = AliasTable::seeded();
        // An override replaces the seeded target; a removal makes the name canonical again.
        assert_eq!(table.set(sym("GOLD"), sym("PAXG")), Ok(Some(sym("XAU"))));
        assert_eq!(table.canonical(sym("GOLD")), sym("PAXG"));
        assert_eq!(table.remove(sym("USDT")), Some(sym("USD")));
        assert_eq!(table.remove(sym("USDT")), None);
        assert_eq!(table.canonical(sym("USDT")), sym("USDT"));
        assert_eq!(table.set(sym("EURC"), sym("EUR")), Ok(None));
        // No name aliases itself, and no alias points at an alias or is pointed at by one.
        assert_eq!(table.set(sym("USD"), sym("USD")), Err(AliasError::ToItself));
        assert_eq!(table.set(sym("USDT"), sym("USDC")), Err(AliasError::Chain));
        assert_eq!(table.set(sym("USD"), sym("DOLLAR")), Err(AliasError::Chain));
        assert_eq!(table.canonical(sym("USDC")), sym("USD"));
    }

    #[test]
    fn keys_resolve_through_the_aliases_to_the_one_listing() {
        let mut resolver = InstrumentResolver::new(AliasTable::seeded());
        assert_eq!(resolver.aliases(), &AliasTable::seeded());
        resolver
            .list(VENUE, perp("BTC", "USD"), listing(1))
            .unwrap();
        for quote in ["USD", "USDT", "USDC"] {
            assert_eq!(
                resolver.resolve(VENUE, perp("BTC", quote)),
                Ok(InstrumentId::new(1)),
                "{quote}"
            );
        }
        // Another venue, another kind or another base is not listed, and is not guessed.
        let spot = AssetKey {
            kind: InstrumentKind::Spot,
            ..perp("BTC", "USDT")
        };
        let other = VenueId::new(4);
        for (venue, key) in [
            (other, perp("BTC", "USD")),
            (VENUE, spot),
            (VENUE, perp("ETH", "USD")),
        ] {
            let canonical = key.canonical(resolver.aliases());
            assert_eq!(
                resolver.resolve(venue, key),
                Err(ResolveError::NotListed {
                    venue,
                    key: canonical
                })
            );
        }
    }

    #[test]
    fn a_key_or_an_id_is_listed_once_per_venue() {
        let mut resolver = InstrumentResolver::new(AliasTable::seeded());
        resolver
            .list(VENUE, perp("BTC", "USDC"), listing(1))
            .unwrap();
        // USDT reads as USD, as USDC does: the same listing.
        assert_eq!(
            resolver.list(VENUE, perp("BTC", "USDT"), listing(2)),
            Err(ResolveError::AlreadyListed {
                venue: VENUE,
                key: perp("BTC", "USD")
            })
        );
        assert_eq!(
            resolver.list(VENUE, perp("ETH", "USD"), listing(1)),
            Err(ResolveError::IdTaken {
                venue: VENUE,
                id: InstrumentId::new(1)
            })
        );
        // Another venue numbers its own.
        resolver
            .list(VenueId::new(4), perp("BTC", "USD"), listing(1))
            .unwrap();
        assert_eq!(resolver.listing(VENUE, perp("BTC", "USD")), Ok(listing(1)));
    }

    #[test]
    fn errors_say_what_was_refused() {
        let key = perp("BTC", "USD");
        let cases: [(&dyn std::error::Error, &str); 8] = [
            (&SymbolError::NotCommonForm, "BASE/QUOTE"),
            (&SymbolError::Unmapped, "maps the ticker to no instrument"),
            (&SymbolError::NoRule, "no FBC common-symbol rule"),
            (&AliasError::ToItself, "alias of itself"),
            (&AliasError::Chain, "cannot itself be an alias"),
            (
                &ResolveError::NotListed { venue: VENUE, key },
                "venue 3 lists no BTC/USD Perpetual",
            ),
            (
                &ResolveError::AlreadyListed { venue: VENUE, key },
                "venue 3 already lists BTC/USD Perpetual",
            ),
            (
                &ResolveError::IdTaken {
                    venue: VENUE,
                    id: InstrumentId::new(9),
                },
                "instrument 9 under another key",
            ),
        ];
        for (err, text) in cases {
            assert!(err.to_string().contains(text), "{err}");
        }
    }
}
