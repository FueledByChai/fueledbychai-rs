// What a permit builds is spent once: it cannot be cloned to authorize a second command from
// one permit (Codex r4186718666).
use fbc_oms::PermittedCommand;

fn twice(cmd: PermittedCommand) -> (PermittedCommand, PermittedCommand) {
    (cmd.clone(), cmd)
}

fn main() {}
