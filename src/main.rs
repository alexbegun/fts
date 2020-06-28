mod indexer;
mod word_hash;
mod common_words;

use std::io::{self};

fn main()-> io::Result<()>  {
    //let path: &'static str = "C:\\Dev\\books\\samples";
    let path: &'static str = "C:\\Dev\\books\\lib";
    let common_word_path: &'static str = "C:\\Dev\\numbers\\src\\top64.txt";
    indexer::index_files(path, common_word_path);
    //let hw = hash_word_to_u128("abcdefghijklmnop");
    //let w = unhash_word(hw);
    //println!("'{}'", w);

    Ok(())        
}
