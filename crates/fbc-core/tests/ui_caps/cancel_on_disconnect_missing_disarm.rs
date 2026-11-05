use fbc_core::CancelOnDisconnect;

fn main() {
    // Protection that does not say whether the venue offers a disarm does not compile: no
    // disarm is assumed, offered or not (decision 0141, FBC-3ymj).
    let _ = CancelOnDisconnect::AccountWide {};
    let _ = CancelOnDisconnect::PerConnection {
        rearm_on_reconnect: true,
        covers_open_orders: false,
    };
}
