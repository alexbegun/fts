    use std::collections::HashMap;
    use crate::indexer;
    use crate::word_hash;

    //Used for statistics
    #[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct InstanceCount {
        count:u32,
        word:u128
    }
    
    pub fn traverse_hm(m: & HashMap<u128,indexer::WordBlock>, emit: bool)
    {
        for (k, v) in m.iter() {
            
            if emit
            {
                println!("word: {} ({})",word_hash::unhash_word(*k), v.count);
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

    }


    pub fn list_top_64(hm:& HashMap<u128,indexer::WordBlock>)
    {
        let mut vec:Vec<InstanceCount> = Vec::new();
        for (k, v) in hm.iter() {
            vec.push(InstanceCount {word:*k, count:v.count as u32});
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