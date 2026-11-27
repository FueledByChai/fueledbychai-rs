// Nor is one built by hand around the refusal of zero (decision 0157 item 2).
use std::time::Duration;

use fbc_oms::RetentionConfig;

fn main() {
    let _ = RetentionConfig {
        max_ended: 0,
        max_age: Duration::ZERO,
        resync_within: Duration::ZERO,
    };
}
