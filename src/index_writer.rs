
use byteorder::{ByteOrder, BigEndian};
use std::fs::OpenOptions;

use std::collections::HashMap;
use std::collections::HashSet;
use crate::indexer;
use crate::word_hash;

use std::io::SeekFrom;
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




fn load_wad_map(wad_file: &str, main_wad_map: &mut BTreeMap<u128,WadValue>)-> io::Result<()>
{
    //first read existing wad file and put it into main_hm
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

        let wv = WadValue {capacity:capacity,position:position, address:address};
            
        //main_hm.entry(key_bytes).or_insert_with(|| indexer::WordBlock {buffer:Vec::with_capacity(64),latest_doc_id:0,latest_index:0,word_count:0,capacity:capacity,address:address,position:position});
        main_wad_map.insert(key_bytes, wv);
    }
    
    Ok(())
}


fn append_wad_map_and_block_file(wad_map: &mut BTreeMap<u128,WadValue>, block_file: & str, hm:& HashMap<u128,indexer::WordBlock>, fill_factor: u8)-> io::Result<()>
{
    let key_v = hm.keys().cloned().collect::<Vec<u128>>();

    let mut bfh = OpenOptions::new()
    .append(true)
    .open(block_file)?;

    let mut address = 0;

    let pos = bfh.seek(SeekFrom::End(0))?;
    println!("end position is: {}",pos);

    for key in key_v 
    {
        if !wad_map.contains_key(&key) 
        {

            println!("appending word: {} ",word_hash::unhash_word(key));

            //Update the main_wad_map
            let wb = hm.get(&key).unwrap();
            let len = wb.buffer.len() as u32;
            let cap = compute_capacity(len,fill_factor);
            let wv = WadValue {capacity:cap,position:len - 1, address:address};
            address = address + cap;
            wad_map.insert(key, wv);


            //Write to block file
            let mut key_bytes = [0; 16];
            BigEndian::write_uint128(&mut key_bytes, key, 16);
            bfh.write_all(&key_bytes)?; //write key, because this will help later with retrieval
            bfh.write_all(&wb.buffer)?; //write block
            
            let pad_size = (cap - len) as usize;
            let mut pad_buffer:Vec<u8> = Vec::with_capacity(pad_size);
            pad_buffer.resize(pad_size, 0);
            bfh.write_all(&pad_buffer)?; //write padding
        }
    }

    Ok(())
}

fn update_wad_map_and_block_file(wad_map: &mut BTreeMap<u128,WadValue>, block_file: & str, hm:& HashMap<u128,indexer::WordBlock>, overflow_map: &mut HashMap<u128,Vec<u8>>)-> io::Result<()>
{
    let mut bfh = OpenOptions::new()
    .read(true)
    .write(true)
    .open(block_file)?;

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
        if let Some(wb) = wad_map.get(&word_key) 
        {
            match hm.get(&word_key)
            {
                //now check if this word is found in new hash map
                Some(v) => merge_block(word_key, &mut bfh, overflow_map, wb, v)?, 
                //if not then fast forward to next word block
                None => skip_block(word_key, &mut bfh, wb.capacity as usize)?
            }
        }
        else
        {
            panic!("key not found.");
        }
    }

    Ok(())
}




//Merges the old block with the new block. and if possible overwrites it in the Block File, if too big then append to the end of block file.
fn merge_block(word_key:u128, bfh:&mut std::fs::File, overflow_map: &mut HashMap<u128,Vec<u8>>, old_wad: & WadValue, new_block: & indexer::WordBlock )-> io::Result<()>
{
    let mut old_block_buffer:Vec<u8> = Vec::with_capacity(old_wad.capacity as usize);

    //remember previous position
    let prev_pos = bfh.seek(SeekFrom::Current(0))?;
   

    let _ = bfh.read(&mut old_block_buffer);
    old_block_buffer.truncate((old_wad.position + 1) as usize); //Truncate the vector to remove padding

    let merged_bytes = merge_block_data(&old_block_buffer, &new_block.buffer);

    //check to see to make sure the merged block is less than the old capacity
    if merged_bytes.len() < old_wad.capacity as usize
    {
        println!("merging word: {} ",word_hash::unhash_word(word_key));
        //rewind to previous position
        bfh.seek(SeekFrom::Start(prev_pos))?;
        bfh.write_all(&merged_bytes)?; //write block
      
        let pad_size = old_wad.capacity as usize - merged_bytes.len();
        let mut pad_buffer:Vec<u8> = Vec::with_capacity(pad_size);
        pad_buffer.resize(pad_size, 0);
        bfh.write_all(&pad_buffer)?; //write padding

    }
    else
    {
        println!("merged word block too big for old block: {} ",word_hash::unhash_word(word_key));
        overflow_map.insert(word_key, merged_bytes);
    }

    Ok(())
}



struct DocPos {
    doc_id:u32,
    init_pos:u32,
    offset:u32
}


//merges two word blocks.. assumes that documents are sorted in ascending order within the block
pub fn merge_block_data(left: &Vec<u8>, right: &Vec<u8>) -> Vec<u8>
{

    let mut output =  Vec::new();
    let mut left_doc_pos = read_doc_id_data(0,left,true);
    let mut right_doc_pos = read_doc_id_data(0,right,true);
    while left_doc_pos.doc_id!=0 && right_doc_pos.doc_id!=0
    {
        if left_doc_pos.doc_id == right_doc_pos.doc_id
        {
            write_doc_id_data(right, &mut output, right_doc_pos.init_pos, right_doc_pos.offset);
            left_doc_pos = read_doc_id_data(left_doc_pos.offset,left,true);
            right_doc_pos = read_doc_id_data(right_doc_pos.offset,right,true);
        }
        else if left_doc_pos.doc_id < right_doc_pos.doc_id
        {
            write_doc_id_data(left, &mut output, left_doc_pos.init_pos, left_doc_pos.offset);
            left_doc_pos = read_doc_id_data(left_doc_pos.offset,left,true);
        }
        else // if left_doc_id > right_doc_id
        {
            write_doc_id_data(right, &mut output, right_doc_pos.init_pos, right_doc_pos.offset);
            right_doc_pos = read_doc_id_data(right_doc_pos.offset,right,true);
        }
     }
    output
}

fn write_doc_id_data(source: & Vec<u8>, dest: &mut Vec<u8>, start_pos: u32, end_pos: u32)
{
    dest.extend(source[start_pos as usize .. end_pos as usize].iter().cloned());
}


//returns a tuple containing docId, old offset, new offset
fn read_doc_id_data(offset: u32, block_data: &Vec<u8>, emit: bool) -> DocPos
{
    let mut i = offset as usize;
      
    //Is it time to leave?
    if i >= block_data.len()
    {
        return DocPos{doc_id:0,init_pos:0, offset: 0};
    }

    let doc_id = unsafe { std::mem::transmute::<[u8; 4], u32>([block_data[i],block_data[i + 1], block_data[i + 2],block_data[i + 3]]) }.to_be();
    i = i + 4;

    loop
    {
        let raw_first_byte = block_data[i];
        let address_first_byte = block_data[i] & 0b01111111;
        let address_second_byte = block_data[i + 1];
        let address = unsafe { std::mem::transmute::<[u8; 2], u16>([address_first_byte, block_data[i + 1]]) }.to_be();
        
        if emit
        {
            print!("   {}-{} ({}) ", format!("{:08b}", raw_first_byte), format!("{:08b}", block_data[i + 1]),address);
        }
        
       
        i = i + 2;

        //Check if extended address

        //This means end of document bytes are reached for this document
        if address == 0x7fff && raw_first_byte & 0b10000000 == 0
        {
            if emit
            {
                println!(" end of doc.");
            }
            return DocPos{doc_id:doc_id,init_pos:offset, offset: i as u32};
        }
        else 
        {
            if emit
            {
                println!();
            }
        }

        
        let more_bit = raw_first_byte & 0x80 == 0x80;

        if more_bit
        {
            let more_type = block_data[i] >> 6;
            let aw = block_data[i] & 0b00111111;
            if more_type == 1 //only law is present
            {
                if emit
                {
                    println!("    raw:{}", format!("{:08b}", aw));
                }
                i = i + 1;
            }
            else if more_type == 2 //only raw is present
            {
                if emit
                {
                    println!("    law:{}", format!("{:08b}", aw));
                }
                i = i + 1;
            }
            else if more_type == 3 //both law & raw present
            {
                if emit
                {
                    println!("    law:{}", format!("{:08b}", aw));
                }

                i = i + 1;
                if emit
                {
                    println!("    raw:{}", format!("{:08b}", block_data[i]));
                }
                i = i + 1;
            }
            else if more_type == 0 //extended address
            {
                let b2 = address_second_byte;
                let mut b1 = address_first_byte;
                if block_data[i] & 0b1 == 0b1 //if the least bit in the overflow byte is set then set the high bit in the extended address
                {
                    b1 = b1 | 0b10000000; 
                }
                let overflow_bits = (block_data[i] >> 1) & 0b00001111; //shift everyone down by 1
                let address = unsafe { std::mem::transmute::<[u8; 4], u32>([0,overflow_bits, b1, b2]) }.to_be();
                if emit
                {
                    println!("    {}-{}-{} ext. ({})", format!("{:04b}", overflow_bits), format!("{:08b}", b1), format!("{:08b}", b2),address);
                }
                
                let mut ext_more_bit = false;

                if emit
                {
                    println!("ext address byte: {}",format!("{:08b}", block_data[i]));
                }

                
                //Check extended more bit
                if block_data[i] & 0b00100000 == 0b00100000
                {
                    ext_more_bit = true;
                }
                i = i + 1;

                if ext_more_bit
                {
                    let ext_more_type = block_data[i] >> 6;
                    let ext_aw = block_data[i] & 0b00111111;
        
                    if ext_more_type == 1 //only law is present
                    {
                        if emit
                        {
                            println!("    rawe:{}", format!("{:08b}", ext_aw));
                        }
                        i = i + 1;
                    }
                    else if ext_more_type == 2 //only raw is present
                    {
                        if emit
                        {
                            println!("    lawe:{}", format!("{:08b}", ext_aw));
                        }
                        i = i + 1;
                    }
                    else if ext_more_type == 3 //both law & raw present
                    {
                        if emit
                        {
                            println!("    lawe:{}", format!("{:08b}", ext_aw));
                        }
                        i = i + 1;
                        if emit
                        {
                            println!("    rawe:{}", format!("{:08b}", block_data[i]));
                        }
                        i = i + 1;
                    }
                    else
                    {
                        panic!("ext_more_type must be greater than 0");
                    }

                }
               
            }
            else
            {
                panic!("more_type may not be greater than 3");
            }

        }

    }
}

fn skip_block(word_key:u128, bfh:&mut std::fs::File, size: usize)-> io::Result<()>
{
    println!("skipping word: {} ",word_hash::unhash_word(word_key));

    let mut pad_buffer:Vec<u8> = Vec::with_capacity(size);
    pad_buffer.resize(size, 0);
    let _ = bfh.read(&mut pad_buffer);
    Ok(())
}


pub fn write_existing(wad_file: &str, block_file: & str, hm:& HashMap<u128,indexer::WordBlock>,  fill_factor: u8)-> io::Result<()>
{
    let mut wad_map:BTreeMap<u128,WadValue> = BTreeMap::new();
    let mut overflow_map:HashMap<u128,Vec<u8>> = HashMap::new();

    load_wad_map(wad_file,&mut wad_map)?;

    //now open block file and start reading chunks from it.
    update_wad_map_and_block_file(&mut wad_map,block_file,hm,&mut overflow_map)?;

    //After this append all new words, that is words that are not found in the wad map
    append_wad_map_and_block_file(&mut wad_map,block_file,hm,fill_factor)?;

    //Last Step is to rewrite the wad file
    rewrite_wad(wad_file, wad_map)?;

    Ok(())
}



pub fn rewrite_wad(wad_file: &str, wad_map:BTreeMap<u128,WadValue>)-> io::Result<()>
{
    let mut wadh = OpenOptions::new()
        .write(true)
        .create(true)
        .open(wad_file).unwrap();

    let mut total_count = 0;
    //Now write wad_map to wad_filegi
    for (key, v) in &wad_map 
    {
        println!("writing wad word: {} ",word_hash::unhash_word(*key));

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

    rewrite_wad(wad_file,wad_map)?;

    Ok(())
}