

    pub fn record(m: & HashMap<u128,WordBlock>, fill_capacity_percentage: u8, target_file: &str)
    {

        
        for (k, v) in m.iter() {
            //println!("word: {} ({})",unhash_word(*k), v.count);
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
                //println!("  doc_id: {}",doc_id);
                i = i + 4;

                loop
                {
                    let raw_first_byte = v.buffer[i];
                    let address_first_byte = v.buffer[i] & 0b01111111;
                    let address_second_byte = v.buffer[i + 1];
                    let address = unsafe { std::mem::transmute::<[u8; 2], u16>([address_first_byte, v.buffer[i + 1]]) }.to_be();
                    //print!("   {}-{} ({}) ", format!("{:08b}", raw_first_byte), format!("{:08b}", v.buffer[i + 1]),address);
                    
                
                    i = i + 2;

                    //Check if extended address

                    //This means end of document bytes are reached for this document
                    if address == 0x7fff && raw_first_byte & 0b10000000 == 0
                    {
                        //println!(" end of doc.");
                        break;
                    }
                    else 
                    {
                        //println!();
                    }

                    
                    let more_bit = raw_first_byte & 0x80 == 0x80;

                    if more_bit
                    {
                        let more_type = v.buffer[i] >> 6;
                        let aw = v.buffer[i] & 0b00111111;
                        if more_type == 1 //only law is present
                        {
                            //println!("    raw:{}", format!("{:08b}", aw));
                            i = i + 1;
                        }
                        else if more_type == 2 //only raw is present
                        {
                            //println!("    law:{}", format!("{:08b}", aw));
                            i = i + 1;
                        }
                        else if more_type == 3 //both law & raw present
                        {
                            //println!("    law:{}", format!("{:08b}", aw));
                            i = i + 1;
                            //println!("    raw:{}", format!("{:08b}", v.buffer[i]));
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
                            //println!("    {}-{}-{} ext. ({})", format!("{:04b}", overflow_bits), format!("{:08b}", b1), format!("{:08b}", b2),address);
                            let mut ext_more_bit = false;

                            //println!("ext address byte: {}",format!("{:08b}", v.buffer[i]));
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
                                    //println!("    rawe:{}", format!("{:08b}", ext_aw));
                                    i = i + 1;
                                }
                                else if ext_more_type == 2 //only raw is present
                                {
                                    //println!("    lawe:{}", format!("{:08b}", ext_aw));
                                    i = i + 1;
                                }
                                else if ext_more_type == 3 //both law & raw present
                                {
                                    //println!("    lawe:{}", format!("{:08b}", ext_aw));
                                    i = i + 1;
                                    //println!("    rawe:{}", format!("{:08b}", v.buffer[i]));
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