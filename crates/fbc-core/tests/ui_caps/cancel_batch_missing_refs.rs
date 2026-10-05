use fbc_core::CancelBatch;

fn main() {
    // A batch cancel that does not say which references its items can name does not compile:
    // no reference kind is assumed (decision 0003, FBC-6c9).
    let _ = CancelBatch { max_items: 20 };
}
