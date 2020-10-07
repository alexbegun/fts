extern crate tantivy;
mod distance;
mod indexer;
mod index_writer;
mod word_hash;
mod common_words;
mod indexer_diagnostics;
mod tantivy_test;
mod input_file_set;

use std::time::{Instant};
use std::io::{self,BufRead};
use std::env;
use std::collections::HashMap;

fn bloom_test()
{
    /*
    let hf = common_words::load_mphf("C:\\Dev\\rust\\fts\\src\\top64.txt");

    let the =  String::from("the").into_bytes();
    let i =  String::from("i").into_bytes();

    println!("{}",hf.hash(&the));
    println!("{}",hf.hash(&i));
    */

    let mut cw = common_words::CwMap::new();
    cw.load("C:\\Dev\\rust\\fts\\src\\top64.txt").unwrap();


    let the =  String::from("the").into_bytes();
    let i =  String::from("i").into_bytes();

    
    println!("{}",cw.map_to(&the));
    println!("{}",cw.map_to(&i));
   
   /*
    for _ in 0..5000000 
    {
        ht.get(&the);
        ht.get(&i);
        ht.get(&crap);
        ht.get(&wow);
        ht.get(&dude);
    }
    */




    
}


fn main()-> io::Result<()>  
{
    //bloom_test();
 
    println!("10/5/2020 10:00PM");
    let args: Vec<String> = env::args().collect();

    if args.len() >= 7
    {
        let source_path = args[1].clone();
        let common_words_path = args[2].clone();
        let index_path = args[3].clone();
        let collection_count = args[4].parse().unwrap_or(0);
        let worker_count = args[5].parse().unwrap_or(0);
        let limit = args[6].parse().unwrap_or(0);
        indexer::index_all(source_path, common_words_path, index_path, collection_count, worker_count, limit)?;
    }
    else if args.len() == 1
    {
        println!("no args.. using defaults..");
        let path = String::from("C:\\Dev\\books\\lib");
        let common_word_path = String::from("C:\\Dev\\rust\\fts\\src\\top64.txt");
        let index_path = String::from("C:\\Dev\\rust\\fts\\data\\index");
        indexer::index_all(path, common_word_path,index_path, 4, 6, 0)?;
    }
    else
    {
        println!("missing arguments: provided: {}", args.len());
        println!("syntax: fts [source_path] [common_words_path] [index_path] [collection_count] [worker_count] [limit]");
    }
 
    //indexer::index_files(wad_file, word_block, path, common_word_path);
    
    //let hw = hash_word_to_u128("abcdefghijklmnop");
    //let w = unhash_word(hw);
    //println!("'{}'", w);


    Ok(())        
}
