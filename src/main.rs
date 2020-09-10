mod distance;
mod indexer;
mod index_writer;
mod word_hash;
mod common_words;
mod indexer_diagnostics;

use std::io::{self};


fn test_dist()
{
    let mut vec: Vec<u128> = Vec::new();

    vec.push(0b_0001_0001_0000_0000);
    vec.push(0b_1000_0100_0010_0000);
    vec.push(0b_0100_1000_0000_0101);

    let (d,o) = distance::find_smallest_distance(&vec);
    println!("smallest distance: {} is ordered: {}",d,o);

}

fn main()-> io::Result<()>  {

    test_dist();

    //let path: &'static str = "C:\\Dev\\books\\samples";


    let path: &'static str = "C:\\Dev\\rust\\fts\\data\\samples";
    let common_word_path: &'static str = "C:\\Dev\\rust\\fts\\src\\top64.txt";

    let wad_file = "C:\\Dev\\rust\\fts\\data\\wad.bin";
    let word_block = "C:\\Dev\\rust\\fts\\data\\wordblock.bin";

    //indexer::index_files(wad_file, word_block, path, common_word_path);


    //let hw = hash_word_to_u128("abcdefghijklmnop");
    //let w = unhash_word(hw);
    //println!("'{}'", w);

    Ok(())        
}
