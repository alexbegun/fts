extern crate tantivy;
mod distance;
mod indexer;
mod index_writer;
mod word_hash;
mod common_words;
mod indexer_diagnostics;
mod rocks_db;
mod tantivy_test;

use std::time::{Instant};
use std::io::{self};


fn test_dist()
{

    let s = Instant::now();


    let mut vec: Vec<u128> = Vec::new();

    vec.push(0b_0100_0000_0000_0000);
    vec.push(0b_1000_0000_0010_0000);
    vec.push(0b_0000_0000_0000_0001);
    vec.push(0b_0010_0000_1000_0000);
    let (d,o) = distance::find_smallest_distance(&vec);
    println!("smallest distance: {} is ordered: {}",d,o);

    let e = s.elapsed();
    println!("time: {:?}", e);

    
    //let (d,o) = distance::find_smallest_distance(&vec);
    
}


fn main()-> io::Result<()>  {

    tantivy_test::index("C:\\Dev\\rust\\fts\\tantivy","C:\\Dev\\books\\lib");
 

    //let h = metro::hash64(b"hello world\xff");

    //assert_eq!(hash.get(&1000), Some(&"1000"));

    //rocks_db::test_rocksdb();
    //test_dist();

    /*
    let path: &'static str = "C:\\Dev\\books\\lib";

    //let path: &'static str = "C:\\Dev\\rust\\fts\\data\\samples";
    let common_word_path: &'static str = "C:\\Dev\\rust\\fts\\src\\top64.txt";

    //let wad_file = "C:\\Dev\\rust\\fts\\data\\wad.bin";
    //let word_block = "C:\\Dev\\rust\\fts\\data\\wordblock.bin";

    indexer::index_all(path, common_word_path, 6, 10);
  */
    //indexer::index_files(wad_file, word_block, path, common_word_path);
    
    //let hw = hash_word_to_u128("abcdefghijklmnop");
    //let w = unhash_word(hw);
    //println!("'{}'", w);

    Ok(())        
}
