use bumpalo::{collections::Vec, vec, Bump};
use std::cell::Cell;

pub fn push_a_bunch_of_items() {
    let b = Bump::new();
    let mut v = Vec::new_in(&b);
    for x in 0..10_000 {
        v.push(x);
    }
}