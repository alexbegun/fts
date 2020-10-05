use unroll::unroll_for_loops;
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

pub fn map_v_to(com_words:& HashMap<Vec<u8>, u8>, word: &Vec<u8>) -> u8
{
    match com_words.get(word) {
        Some(v) => *v,
        None => 255
    }
}


#[unroll_for_loops]
pub fn map_to_vec(com_words:& Vec<u128>, w: &u128) -> u8
{
    for i in 0..64
    {
        if com_words[i] == *w
        {
            return i as u8;
        }
    }
    255
}



pub fn load_vec(file_name:&str, com_words: &mut Vec<u128>)->  io::Result<()>
{
    let file = File::open(file_name)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let word = line?;
        com_words.push(word_hash::hash_word_to_u128(&word));
    }
    Ok(())
}


pub fn load(file_name:&str, com_words: &mut HashMap<Vec<u8>, u8>)->  io::Result<()>
{
    let file = File::open(file_name)?;
    let reader = BufReader::new(file);
    let mut index = 0;
    for line in reader.lines() {
        let word = line?;
        com_words.insert(word.into_bytes(), index);
        index = index + 1;
    }

    Ok(())
}
