use byteorder::{ByteOrder, BigEndian};
use std::fs::OpenOptions;

use std::collections::HashMap;
use crate::indexer;
use crate::word_hash;

use std::io::{self, prelude::*};
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






pub fn write_existing(wad_file: &str, block_file: & str, hm:& HashMap<u128,indexer::WordBlock>,  fill_factor: u8)-> io::Result<()>
{

    let mut main_hm:HashMap<u128,indexer::WordBlock> = HashMap::new();

    //first read existing wad file and put it into main_hm
    {
        let mut wadh = OpenOptions::new()
        .read(true)
        .open(wad_file)?;

        let mut wadh_bytes =  Vec::new();
        wadh.read_to_end(&mut wadh_bytes)?;

        println!("read {} wad bytes from existing wad file.",wadh_bytes.len());

        let mut i = 0;
        while i<wadh_bytes.len() 
        {
            let key_bytes = BigEndian::read_uint128(&wadh_bytes[i..i+16], 16);
            i =  i + 16;
            let capacity = BigEndian::read_u32(&wadh_bytes[i..i+4]);
            i =  i + 4;
            let address = BigEndian::read_u32(&wadh_bytes[i..i+4]);
            i =  i + 4;
            let position = BigEndian::read_u32(&wadh_bytes[i..i+4]);
            i =  i + 4;

            main_hm.entry(key_bytes).or_insert_with(|| indexer::WordBlock {buffer:Vec::with_capacity(64),latest_doc_id:0,latest_index:0,word_count:0,capacity:capacity,address:address,position:position});
        }

    }


    //now open block file and start reading chunks from it.
    {
     
        let mut bfh = OpenOptions::new()
        .read(true)
        .write(true)
        .open(block_file)?;

        let mut count = 0;
        loop
        {

            //First read the word 
            let mut word_bytes = [0; 16];
            let word_bytes_read = bfh.read(&mut word_bytes)?;
            if word_bytes_read == 0 //Get out if nothing read
            {
                break;
            }
            let word_key = BigEndian::read_uint128(&word_bytes, 16);

            //now get the WordBlock info from main_hm
            if let Some(wb) = main_hm.get(&word_key) 
            {
                match hm.get(&word_key) 
                {
                    //now check if this word is found in new hash map
                    Some(v) => merge_block(word_key, &mut bfh, wb, v), 
                    //if not then fast forward to next word block
                    None => skip_block(word_key, &mut bfh, wb.capacity as usize)
                }
            }
            else
            {
                panic!("key not found.");
            }
        }

    }
    Ok(())
}

fn merge_block(word_key:u128, bfh:&mut std::fs::File, old_block: & indexer::WordBlock, new_block: & indexer::WordBlock )
{
    println!("mergin word: {} ",word_hash::unhash_word(word_key));

    skip_block(word_key, bfh, old_block.capacity as usize);
}

fn skip_block(word_key:u128, bfh:&mut std::fs::File, size: usize)
{
    println!("skipping word: {} ",word_hash::unhash_word(word_key));

    let mut pad_buffer:Vec<u8> = Vec::with_capacity(size);
    pad_buffer.resize(size, 0);
    bfh.read(&mut pad_buffer);
}


pub fn write_new(wad_file: &str, block_file: & str, hm:& HashMap<u128,indexer::WordBlock>,  fill_factor: u8)-> io::Result<()>
{
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
        
        let mut key_bytes = [0; 16];
        BigEndian::write_uint128(&mut key_bytes, *key, 16);
        bfh.write_all(&key_bytes)?; //write key, because this will help later with retrieval
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

    let mut total_count = 0;
    //Now write wad_map to wad_filegi
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

        total_count = total_count + 1;
    }

    println!("total word count written: {}", total_count);

    Ok(())
}