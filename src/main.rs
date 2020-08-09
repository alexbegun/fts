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

    let wad_file = "C:\\Dev\\rust\\fts\\data\\wad.bin";
    let word_block = "C:\\Dev\\rust\\fts\\data\\wordblock.bin";

    indexer::index_files(wad_file, word_block, path, common_word_path);


    //let hw = hash_word_to_u128("abcdefghijklmnop");
    //let w = unhash_word(hw);
    //println!("'{}'", w);

    Ok(())        
}
