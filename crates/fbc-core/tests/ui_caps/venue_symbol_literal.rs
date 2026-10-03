use fbc_core::VenueSymbol;

fn main() {
    // A venue symbol is built only by an adapter, through DecodeScope::venue_symbol.
    let _ = VenueSymbol("ETH-USD-PERP".into());
}
