// Decision 0152: only the handed form runs the check at submit, so no gateway checks a command,
// and so writes one, while its authorization could still release its orders on its drop.
use fbc_oms::Authorization;

fn checked(auth: &Authorization) -> bool {
    auth.check_at_submit().is_ok()
}

fn main() {}
