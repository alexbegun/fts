use std::collections::HashMap;
use std::fs::File;
use std::io::{self, prelude::*, BufReader};
use crate::word_hash;
    
//Maps regular word hash to common word hash code
pub fn map_to(com_words:& HashMap<u128, u8>, w: &u128) -> u8
{
    match com_words.get(w) {
        Some(v) => *v,
        None => 255
    }
}

pub fn load(file_name:&str, com_words: &mut HashMap<u128, u8>)->  io::Result<()>
{
    let file = File::open(file_name)?;
    let reader = BufReader::new(file);
    let mut index = 0;
    for line in reader.lines() {
        let word = line?;
        com_words.insert(word_hash::hash_word_to_u128(&word), index);
        index = index + 1;
    }

    Ok(())
}
