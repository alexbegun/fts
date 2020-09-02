use std::collections::HashMap;
    use crate::indexer;
    use crate::word_hash;

    use byteorder::{ByteOrder, BigEndian};
    use std::fs::OpenOptions;
    use std::io;
    use std::io::prelude::*;
    
    //Used for statistics
    #[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct InstanceCount {
        count:u32,
        word:u128
    }
    

    pub fn print_vec(buffer: &Vec<u8>)
    {
        for b in buffer.iter() 
        {
            println!("{}",format!("{:08b}", b));
        }
    }





    //merges two word blocks.. assumes that documents are sorted in ascending order within the block
    pub fn merge_blocks(left: &Vec<u8>, right: &Vec<u8>) -> Vec<u8>
    {

        let mut output =  Vec::new();

        let  (mut left_doc_id, mut s_left_pos, mut left_pos) = read_doc_id_data(0,left,true);
        let  (mut right_doc_id, mut s_right_pos, mut right_pos) = read_doc_id_data(0,right,true);
        
        while left_doc_id!=0 && right_doc_id!=0
        {
            if left_doc_id == right_doc_id
            {
                write_doc_id_data(right, &mut output, s_right_pos, right_pos);

                let  (mut left_doc_id, mut s_left_pos, mut left_pos) = read_doc_id_data(left_pos,left,true);
                let  (mut right_doc_id, mut s_right_pos, mut right_pos) = read_doc_id_data(right_pos,right,true);
            }
            else if left_doc_id < right_doc_id
            {
                write_doc_id_data(left, &mut output, s_left_pos, left_pos);
                let  (mut left_doc_id, mut s_left_pos, mut left_pos) = read_doc_id_data(left_pos,left,true);
            }
            else // if left_doc_id > right_doc_id
            {
                write_doc_id_data(right, &mut output, s_right_pos, right_pos);
                let  (mut right_doc_id, mut s_right_pos, mut right_pos) = read_doc_id_data(right_pos,right,true);
            }
         }


        output
    }

    fn write_doc_id_data(source: & Vec<u8>, dest: &mut Vec<u8>, start_pos: u32, end_pos: u32)
    {
        dest.extend(source[start_pos as usize .. end_pos as usize].iter().cloned());
    }


    //returns a tuple containing docId, old offset, new offset
    fn read_doc_id_data(offset: u32, block_data: &Vec<u8>, emit: bool) -> (u32,u32,u32)
    {
        let mut i = offset as usize;
          
        //Is it time to leave?
        if i >= block_data.len()
        {
            return (0,0,0);
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
                return (doc_id, offset, i as u32);
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

    pub fn traverse_hm(m: &HashMap<u128,indexer::WordBlock>, emit: bool)
    {
        for (k, v) in m.iter() {
            


            if emit
            {
                println!("word: {} ({})",word_hash::unhash_word(*k), v.word_count);
                //print_vec(&v.buffer);
            }

            let mut i = 0;
            loop
            {
                //Is it time to leave?
                if i >= v.buffer.len()
                {
                    break;
                }

                //First is always the doc_id
                let doc_id = unsafe { std::mem::transmute::<[u8; 4], u32>([v.buffer[i], v.buffer[i + 1], v.buffer[i + 2], v.buffer[i + 3]]) }.to_be();

                if emit
                {
                    println!("  doc_id: {}",doc_id);
                }
                
                i = i + 4;

                loop
                {
                    let raw_first_byte = v.buffer[i];
                    let address_first_byte = v.buffer[i] & 0b01111111;
                    let address_second_byte = v.buffer[i + 1];
                    let address = unsafe { std::mem::transmute::<[u8; 2], u16>([address_first_byte, v.buffer[i + 1]]) }.to_be();
                    
                    if emit
                    {
                        print!("   {}-{} ({}) ", format!("{:08b}", raw_first_byte), format!("{:08b}", v.buffer[i + 1]),address);
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
                        break;
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
                        let more_type = v.buffer[i] >> 6;
                        let aw = v.buffer[i] & 0b00111111;
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
                                println!("    raw:{}", format!("{:08b}", v.buffer[i]));
                            }
                            i = i + 1;
                        }
                        else if more_type == 0 //extended address
                        {
                            let b2 = address_second_byte;
                            let mut b1 = address_first_byte;
                            if v.buffer[i] & 0b1 == 0b1 //if the least bit in the overflow byte is set then set the high bit in the extended address
                            {
                                b1 = b1 | 0b10000000; 
                            }
                            let overflow_bits = (v.buffer[i] >> 1) & 0b00001111; //shift everyone down by 1
                            let address = unsafe { std::mem::transmute::<[u8; 4], u32>([0,overflow_bits, b1, b2]) }.to_be();
                            if emit
                            {
                                println!("    {}-{}-{} ext. ({})", format!("{:04b}", overflow_bits), format!("{:08b}", b1), format!("{:08b}", b2),address);
                            }
                            
                            let mut ext_more_bit = false;

                            if emit
                            {
                                println!("ext address byte: {}",format!("{:08b}", v.buffer[i]));
                            }

                            
                            //Check extended more bit
                            if v.buffer[i] & 0b00100000 == 0b00100000
                            {
                                ext_more_bit = true;
                            }
                            i = i + 1;

                            if ext_more_bit
                            {
                                let ext_more_type = v.buffer[i] >> 6;
                                let ext_aw = v.buffer[i] & 0b00111111;
                    
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
                                        println!("    rawe:{}", format!("{:08b}", v.buffer[i]));
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

        }

        println!("traverse successful.");

    }


    pub fn load_hm(wad_file: &str, block_file: &str, hm:&mut HashMap<u128,indexer::WordBlock>) -> io::Result<()>
    {
       
        //first fill the hashmap with perliminary info: key, capacity, address, position
        {
            let mut wadh = OpenOptions::new()
            .read(true)
            .open(wad_file)?;
    
            let mut wadh_bytes =  Vec::new();
            wadh.read_to_end(&mut wadh_bytes)?;
    
            println!("read {} wad bytes",wadh_bytes.len());

            let mut i = 0;
            let mut total_count = 0;
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

                hm.entry(key_bytes).or_insert_with(|| indexer::WordBlock {buffer:Vec::with_capacity(64),latest_doc_id:0,latest_index:0,word_count:0,capacity:capacity,address:address,position:position});
                total_count = total_count + 1;
            }

            println!("total word count read: {}", total_count);
        }
        

        {
     
            let mut bfh = OpenOptions::new()
            .read(true)
            .open(block_file)?;

            let mut bytes =  Vec::new();
            bfh.read_to_end(&mut bytes)?;

            println!("read {} word block bytes",bytes.len());

            
            let mut i = 0;
            let mut total_count = 0;
            while i<bytes.len() 
            {
                let word_key = BigEndian::read_uint128(&bytes[i..i+16], 16);
                i =  i + 16;



                if let Some(wb) = hm.get_mut(&word_key) 
                {
                    //println!("{}-{},{}",word_hash::unhash_word(word_key),wb.position,wb.capacity);

                    wb.buffer.extend(&bytes[i..i+1+wb.position as usize]);
                    i = i + wb.capacity as usize;
                }
                else
                {
                    panic!("word not found!");
                }

                
                total_count = total_count + 1;
            }

        }


        
        Ok(())
    }


    pub fn list_top_64(hm:& HashMap<u128,indexer::WordBlock>)
    {
        let mut vec:Vec<InstanceCount> = Vec::new();
        for (k, v) in hm.iter() {
            vec.push(InstanceCount {word:*k, count:v.word_count as u32});
        }
        //vec.sort();
        vec.sort_by(|a, b| b.cmp(a));
        let mut com_count = 0;
        for i in 0..64
        {
            com_count = com_count + vec[i].count;
            println!("{0}",word_hash::unhash_word(vec[i].word)); //,vec[i].count);
        }

        let mut other_count = 0;
        for i in 64..vec.len()
        {
            other_count = other_count + vec[i].count;
        }


        println!("top 64 count:{}",com_count);
        println!("rest count:  {}",other_count);

    }