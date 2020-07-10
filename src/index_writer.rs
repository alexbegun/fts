use std::fs::File;
use std::fs::OpenOptions;

use std::collections::HashMap;
use crate::indexer;


use std::io::{self, prelude::*, BufWriter};
use std::fs::{self};


use std::io::SeekFrom;

pub fn write_test()
{
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(65);
    buffer.push(66);
    buffer.push(67);

    
    //let f = File::with_options().write(true).read(true).open("C:\\Dev\\fts\\src\\foo.txt");
    let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("C:\\Dev\\fts\\src\\foo.txt").unwrap();

    //File::with_options().read(true).open("foo.txt")

    f.seek(std::io::SeekFrom::Start(42)).expect("Unable to seek");
    f.write_all(&buffer).expect("Unable to write bytes");
}

fn write(file: &str, hm:& HashMap<u128,indexer::WordBlock>)-> io::Result<()>
{


    Ok(())
}