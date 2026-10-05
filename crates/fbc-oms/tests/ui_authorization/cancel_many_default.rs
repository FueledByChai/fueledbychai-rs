// A cancel-many cannot be authorized from an empty or default authorization either.
use fbc_oms::Authorization;

fn forge() -> Authorization {
    Authorization::default()
}

fn main() {}
