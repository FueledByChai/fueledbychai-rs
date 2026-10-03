use fbc_core::{ClientOrderId, Namespace};

fn main() {
    let _ = ClientOrderId { ns: Namespace::new(1), seq: 7 };
}
