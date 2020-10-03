#![cfg_attr(feature = "collections",)]

extern crate tantivy;
mod distance;
mod indexer;
mod index_writer;
mod word_hash;
mod common_words;
mod indexer_diagnostics;
mod tantivy_test;
mod bumo_feat;

use std::time::{Instant};
use std::io::{self,BufRead};
use std::env;





fn main()-> io::Result<()>  
{

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
