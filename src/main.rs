mod indexer;
mod index_writer;
mod word_hash;
mod common_words;
mod indexer_diagnostics;

use std::io::{self};

fn main()-> io::Result<()>  {
    //let path: &'static str = "C:\\Dev\\books\\samples";
    let path: &'static str = "C:\\Dev\\rust\\fts\\data\\samples";
    let common_word_path: &'static str = "C:\\Dev\\rust\\fts\\src\\top64.txt";
    indexer::index_files(path, common_word_path);


    //let hw = hash_word_to_u128("abcdefghijklmnop");
    //let w = unhash_word(hw);
    //println!("'{}'", w);

    Ok(())        
}
