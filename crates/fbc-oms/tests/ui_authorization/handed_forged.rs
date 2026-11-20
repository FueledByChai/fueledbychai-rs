// Decision 0152: a handed authorization comes only from `Authorization::hand_over`; it cannot be
// built, or cloned so that one copy is written and the other given back.
use fbc_oms::Handed;

fn forged() -> Handed {
    Handed {}
}

fn twice(handed: &Handed) -> (Handed, Handed) {
    (handed.clone(), Clone::clone(handed))
}

fn main() {}
