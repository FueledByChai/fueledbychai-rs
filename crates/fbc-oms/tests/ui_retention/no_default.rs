// The retention's count, age and resync bound are the consumer's: there is no default
// (decision 0157 item 2).
use fbc_oms::RetentionConfig;

fn main() {
    let _ = RetentionConfig::default();
}
