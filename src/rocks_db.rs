use rocksdb::{Options, DB, MergeOperands};
use crate::indexer;
use std::collections::HashMap;
use byteorder::{ByteOrder, BigEndian};
// NB: db is automatically closed at end of lifetime


fn concat_merge(new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands) -> Option<Vec<u8>> 
{

    let mut result: Vec<u8> = Vec::with_capacity(operands.size_hint().0);
    existing_val.map(|v| {
        for e in v 
        {
            result.push(*e)
        }
    });
    for op in operands {
        for e in op {
            result.push(*e)
        }
    }
    Some(result)
}


pub fn save(db: &mut rocksdb::DB, hm:& HashMap<u128,indexer::WordBlock>, doc_collection: u32)
{

    /*
    opts.set_merge_operator("test operator", concat_merge, None);
    {
        let db = DB::open(&opts, path).unwrap();
        let p = db.put(b"k1", b"a");
        db.merge(b"k1", b"b");
        db.merge(b"k1", b"c");
        db.merge(b"k1", b"d");
        db.merge(b"k1", b"efg");
        let r = db.get(b"k1");
        assert_eq!(r.unwrap().unwrap(), b"abcdefg");
    }
    */

    //opts.set_merge_operator("mergeop", concat_merge, None);
    


    //Write to block file and fill wad_map
    for (key, v) in hm.iter() 
    {  
        
        let mut key_bytes = [0; 20];
        BigEndian::write_uint128(&mut key_bytes, *key, 16);
        let mut key_bytes_2 = [0; 4];
        BigEndian::write_u32(&mut key_bytes_2, doc_collection);

        key_bytes[16] = key_bytes_2[0];
        key_bytes[17] = key_bytes_2[1];
        key_bytes[18] = key_bytes_2[2];
        key_bytes[19] = key_bytes_2[3];
        
        
        let _ = db.put(key_bytes, &v.buffer);
    
        /*
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
        */
    }
    


}


