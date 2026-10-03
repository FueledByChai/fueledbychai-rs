use fbc_core::{ClientIdFormat, DecodeScope, Namespace};

fn main() {
    let fmt = ClientIdFormat::Uuid;
    let scope = DecodeScope { fmt: &fmt, own: Namespace::new(1) };
    let _ = scope.venue_order_id("1");
}
