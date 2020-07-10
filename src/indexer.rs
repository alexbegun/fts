    use crate::word_hash;
    use crate::common_words;
    use crate::indexer_diagnostics;
    
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{self, prelude::*, BufReader};
    use std::fs::{self};
    use std::time::{Instant};
    use std::thread;


    
     //Main structure representing a Word Block
    pub struct WordBlock {
        pub buffer: Vec<u8>,
        latest_doc_id:u32,
        latest_index:u32,
        pub count:u64,
        count_64:u64,
        count_256:u64,
        count_long:u64,
    }

    // get all files from a directory
    fn get_files(directory: &str, v:&mut Vec<String>) -> io::Result<()> {
        let dirs =  fs::read_dir(directory).unwrap();
        for dir in dirs
        {
            let entry = dir.unwrap().path();
            if !entry.is_dir()
            {
                let file = entry.display().to_string();
                v.push(file);
            }
        }

        Ok(())
    }

    //derives a document id from file name
    fn get_doc_id(doc_file:&str) -> u32 
    {
        let path_parts_ar:Vec<&str> = doc_file.split("\\").collect();
        let file_parts_ar:Vec<&str> = path_parts_ar[path_parts_ar.len() - 1].split(".").collect();
        let name_parts_ar:Vec<&str> = file_parts_ar[0].split("-").collect();
        let doc_id: u32 = name_parts_ar[0].parse().unwrap();
        doc_id
    }


    //adds a word position to a particular WordBlock along with adjacent words
    fn add_word_to_hash_map(doc_id:u32,word_index:u32,law:u8,w:u128,raw:u8,hm:&mut HashMap<u128,WordBlock>)
    {
        let wb = hm.entry(w).or_insert_with(|| WordBlock {buffer:Vec::with_capacity(64),latest_doc_id:0,latest_index:0,count:0,count_64:0,count_256:0,count_long:0});

        //Write the doc_id if necessary
        if doc_id!=wb.latest_doc_id
        {
            //Write terminator bytes for previous doc
            if wb.buffer.len() > 0
            {
                wb.buffer.push(0x7f); 
                wb.buffer.push(0xff);
            }

            wb.latest_doc_id = doc_id;
            let b1 : u8 = ((doc_id >> 24) & 0xff) as u8;
            let b2 : u8 = ((doc_id >> 16) & 0xff) as u8;
            let b3 : u8 = ((doc_id >> 8) & 0xff) as u8;
            let b4 : u8 = (doc_id & 0xff) as u8;
            wb.buffer.extend([b1, b2, b3, b4].iter().copied());
            wb.latest_index = 0; //Don't forget to rest the latest index
        }

        //Calculate the offset
        let offset  = word_index - wb.latest_index;
        let is_offset_overflow = offset >= 32766;

        let mut more_ind = 0; //initial more indicator
    
        //set more indicator to true if there is law or raw or offset overflow
        if law != 255 || raw!=255 || is_offset_overflow
        {
            more_ind = 0b10000000;
        }

        //if there is no offset overflow
        if !is_offset_overflow
        {
            //push first two bytes with offset information and more indicator

            //println!("    b1:{}", format!("{:08b}", b1));
            //println!("    b2:{}", format!("{:08b}", b2));

            wb.buffer.push(((offset as u16 >> 8) as u8 | more_ind) as u8);
            wb.buffer.push((offset as u16 & 0xff) as u8);



            if law !=255 && raw==255 //If left is set
            {
                wb.buffer.push(0b10000000 | law);
            }
            else if raw != 255 && law==255 //If right is set
            {
                wb.buffer.push(0b01000000 | raw);
            }
            else if law!=255 && raw != 255 //if both
            {
                wb.buffer.push(0b11000000 | law);
                wb.buffer.push(raw);
            }

        }
        else //This indicates offset overflow.
        {
            //Write the firt 15 bits of the offset address along with more_ind
            wb.buffer.push(((offset as u16 >> 8) as u8 | more_ind) as u8);
            wb.buffer.push((offset as u16 & 0xff) as u8);

            //0b0001
            let mut ext_byte = (offset as u32 >> 15) as u8 & 0b00011111;

            //if law or raw is present explicitly set the ext_more bit.
            if law !=255 || raw != 255
            {
                ext_byte = ext_byte | 0b00100000;
            }
        
            //push the 5 remaining extended offset address bits  along with 3 leading bits 00 & 0 or 1 depending on the presence of law and/or raw
            wb.buffer.push(ext_byte);

            if law !=255 && raw==255 //If left is set
            {
                wb.buffer.push(0b10000000 | law);
            }
            else if raw != 255 && law==255 //If right is set
            {
                wb.buffer.push(0b01000000 | raw);
            }
            else if law!=255 && raw != 255 //if both
            {
                wb.buffer.push(0b11000000 | law);
                wb.buffer.push(raw);
            }

        }
    
        wb.latest_index = word_index; //Make sure to set the latest_index
        wb.count = wb.count + 1;
    
    }




    //Parses a particular file adding all word positions to WordBlocks.
    fn parse_file(doc_id: u32, file_name: &str, hm:&mut HashMap<u128,WordBlock>, com_words:& HashMap<u128, u8>) ->  io::Result<u32> {
        let file = File::open(file_name)?;
        let reader = BufReader::new(file);
        let mut word_index:u32 = 0;
        
        //let mut l:u128;
        let mut w:u128;
        let mut r:u128 = 0;

        let nomatch:u8 = 255;
    
        let mut rawh:u8 = nomatch;
        let mut cw:u8 = nomatch;
        let mut lawh:u8;

        let mut word = String::with_capacity(25);
            
        for line in reader.lines() {
            let st = line?;
        
        
            for c in st.chars() 
            { 
                if c.is_alphanumeric() || c=='\''
                {
                    word.push(c.to_ascii_lowercase());
                }
                else 
                {
                    if word.len() > 0
                    {
                        //l = w;
                        lawh = cw;
                        
                        w = r;
                        cw = rawh;
                        
                        r = word_hash::hash_word_to_u128(&word);
                        rawh = common_words::map_to(com_words,&r);

                        
                        //only add if not a common word.
                        if cw==nomatch && w!=0
                        {
                            add_word_to_hash_map(doc_id,word_index - 1, lawh, w, rawh, hm);
                        }

                        word.clear();
                        word_index = word_index + 1;
                    }
                }
            }
            if word.len() > 0
            {
                
                //l = w;
                lawh = cw;
                
                w = r;
                cw = rawh;
                
                r = word_hash::hash_word_to_u128(&word);
                rawh = common_words::map_to(com_words,&r);

                //only add if not a common word.
                if cw==255 && w!=0
                {
                    add_word_to_hash_map(doc_id,word_index - 1, lawh, w, rawh, hm);
                }

                //finally if at the end also add the last word if not common.println!
                //only add if not a common word.
                if rawh==255 && r!=0
                {
                    add_word_to_hash_map(doc_id,word_index, cw, r, nomatch, hm);
                }

                word.clear();

                word_index = word_index + 1;
            }
        }

        Ok(word_index)
    }

    fn index(source_path:&str, common_word_path:&str, worker_id:u8, worker_count:u8) -> HashMap<u128,WordBlock>
    {
        let mut hm:HashMap<u128,WordBlock> = HashMap::new();
        let mut com:HashMap<u128, u8> =  HashMap::new();
        common_words::load(common_word_path, & mut com).expect("Error Loading common words.");

        let mut doc_files = Vec::new();
        get_files(source_path, & mut doc_files).expect("Error Loading source file path.");

        doc_files.sort();
        doc_files.resize(20000,"".to_string());

        let mut count = 0;
    
        for doc_file in doc_files 
        {
            if doc_file == ""
            {
                break;
            }

            let doc_id = get_doc_id(&doc_file);
            if (doc_id % worker_count as u32) as u8 == worker_id || worker_id == 255
            {
                parse_file(doc_id, &doc_file, & mut hm, &com).expect("Unable to parse file.");
                count = count + 1;
            }
            //println!("{}", &doc_file);
        }

        add_terminators(& mut hm);

        println!("worker_id: {:?}  count: {:?}", worker_id, count);
        hm
    }

    fn add_terminators(m: &mut HashMap<u128,WordBlock>)
    {
        let key_v = m.keys().cloned().collect::<Vec<u128>>();

        for key in key_v {
            //Add terminating bytes to each word in Hashmap
            match m.get_mut(&key) 
            {
                Some(v) => v.buffer.extend([0x7f, 0xff].iter().copied()), 
                None => panic!("key not found.")
            }
        }
        
    }

    fn copy_map(master: &mut HashMap<u128,WordBlock>, worker: HashMap<u128,WordBlock>)
    {
        let s = Instant::now();
        for (k, v) in worker.iter() {
            let wb = master.entry(*k).or_insert_with(|| WordBlock {buffer:Vec::new(),latest_doc_id:0,latest_index:0, count:0,count_64:0,count_256:0,count_long:0});

            wb.count = wb.count + v.count;
            wb.buffer.extend(v.buffer.iter().cloned())
        }
        let e = s.elapsed();
        println!("copy time: {:?} ", e);

    }

    fn get_count(m: & HashMap<u128,WordBlock>) -> (u64,u64,u64,u64,u64)
    {
        let mut count = 0;
        let mut byte_count = 0;
        let mut count_64 = 0;
        let mut count_256 = 0;
        let mut count_long = 0;
        for (_, v) in m.iter() {
            count = count + v.count;
            byte_count = byte_count + v.buffer.len();
            count_64 = count_64 + v.count_64;
            count_256 = count_256 + v.count_256;
            count_long = count_long + v.count_long;
        }
        (count,byte_count as u64,count_64 as u64, count_256 as u64, count_long as u64)
    }


    pub fn index_files(source_path:&'static str, common_word_path:&'static str)
    {
        let worker_count = 1;

        if worker_count == 1
        {
            let s = Instant::now();
            let hm = index(source_path,common_word_path,255,1);
            let counts = get_count(&hm);

            let e = s.elapsed();
            println!("time: {:?} count:{:?}", e,counts);
            indexer_diagnostics::traverse_hm(&hm, false);

            //list_top_64(& hm);
    
        }
        else
        {
            let s = Instant::now();
        
            let mut workers = vec![];
            for i in 0..worker_count {
                // Spin up another thread
                workers.push(thread::spawn(move || {
                    println!("spawning worker {}", i);
                    let hm = index(source_path,common_word_path,i,worker_count);
                    hm
                }));
            }


            //Master Doc
            let mut mm:HashMap<u128,WordBlock> = HashMap::new();

            for worker in workers {
                let hm = worker.join().unwrap();
                copy_map(&mut mm,hm);
            }
            let count = get_count(&mm);
            let e = s.elapsed();
            println!("time: {:?} count:{:?}", e,count);
        }

    }