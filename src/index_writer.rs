extern crate libc;
use std::fs::File;
use std::fs::OpenOptions;

use std::collections::HashMap;
use crate::indexer;


use std::io::{self, prelude::*, BufWriter};
use std::fs::{self};
use std::collections::BTreeMap;


use std::io::SeekFrom;


use std::mem;

//word address directory value
pub struct WadValue {
    capacity:u32,
    address:u32,
    position:u32    
}


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

fn compute_capacity(block_length: u32, fill_factor: u8) -> u32
{
    
    let cap = ((block_length as f32)*100.0/(fill_factor as f32)) as u32;
    //println!("{} {} {}",block_length,fill_factor,cap);
    cap
}

fn build_wad(hm:& HashMap<u128,indexer::WordBlock>, fill_factor: u8) -> BTreeMap<u128,WadValue>
{
    let key_v = hm.keys().cloned().collect::<Vec<u128>>();
    let mut wad_map:BTreeMap<u128,WadValue> = BTreeMap::new();

    for key in key_v 
    {
        match hm.get(&key) 
        {
            Some(v) =>  
                        {
                            let wv = WadValue {capacity:compute_capacity(v.buffer.len() as u32,fill_factor),position:0, address:0};
                            wad_map.insert(key, wv);
                        }, 

            None => panic!("key not found.")
        }
    }

    /*
    for (k, _) in &wad_map 
    {
        println!("{}", k);
    }
    */

    wad_map
}

pub fn write_new(wad_file: &str, block_file: & str, hm:& HashMap<u128,indexer::WordBlock>,  fill_factor: u8)-> io::Result<()>
{

        
    //let mut wad_map:BTreeMap<u128,WadValue> = build_wad(hm, fill_factor);
    let mut wad_map:BTreeMap<u128,WadValue> = BTreeMap::new();
    let mut address = 0;
    let mut bfh = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(block_file).unwrap();

    let mut count = 0;
    let mut zero_count = 0;
    
    for (key, v) in hm.iter() 
    {
        let len = v.buffer.len() as u32;
        let cap = compute_capacity(len,fill_factor);
        let wv = WadValue {capacity:cap,position:len - 1, address:address};
        address = address + cap;
        wad_map.insert(*key, wv);
        bfh.write_all(&v.buffer)?; //write block
       
        let pad_size = (cap - len) as usize;
        let mut pad_buffer:Vec<u8> = Vec::with_capacity(pad_size);
        pad_buffer.resize(pad_size, 0);

        bfh.write_all(&pad_buffer)?; //write padding

        if len == 0
        {
            zero_count = zero_count + 1;
        }

        count = count + 1;
    }

    println!("{} {}",count, zero_count);

    Ok(())
}