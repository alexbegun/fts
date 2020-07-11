use byteorder::{ByteOrder, BigEndian};
use std::fs::OpenOptions;

use std::collections::HashMap;
use crate::indexer;


use std::io::{self, prelude::*, BufWriter};
use std::fs::{self};
use std::collections::BTreeMap;


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

    //Write to block file and fill wad_map
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

    }

    let mut wadh = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(wad_file).unwrap();

    //Now write wad_map to wad_file
    for (key, v) in &wad_map 
    {
        let mut key_bytes = [0; 16];
        BigEndian::write_uint128(&mut key_bytes, *key, 16);
        wadh.write_all(&key_bytes)?;

        let mut capacity = [0; 4];
        BigEndian::write_u32(&mut capacity, v.capacity);
        wadh.write_all(&capacity)?;

        let mut address = [0; 4];
        BigEndian::write_u32(&mut address, v.address);
        wadh.write_all(&address)?;

        let mut position = [0; 4];
        BigEndian::write_u32(&mut position, v.position);
        wadh.write_all(&position)?;
    }
    Ok(())
}