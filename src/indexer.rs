    use std::cell::Cell;
    
    use crate::word_hash;
    use crate::common_words;
    use crate::indexer_diagnostics;
    use crate::index_writer;
    
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{self, prelude::*, BufReader};
    use std::fs::{self};
    use std::time::{Instant};
    use std::thread;
    use std::path::{Path, PathBuf};

    use std::sync::Arc;

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    use std::cell::RefCell;
    

    //use bumpalo::{collections::Vec, vec, Bump};

    pub struct WordBlock2
    {
        pub buffer_id: i32,
        pub latest_doc_id:u32,
        pub latest_index:u32,
        pub word_count:u64,
        
        pub capacity:u32,
        pub address:u32,
        pub position:u32  
    }

     //Main structure representing a Word Block
    pub struct WordBlock {
        pub buffer: Vec<u8>,
        pub latest_doc_id:u32,
        pub latest_index:u32,
        pub word_count:u64,
        
        pub capacity:u32,
        pub address:u32,
        pub position:u32   
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


    // get all files from a directory
    fn get_all_files(directory: &str, v:&mut Vec<String>) -> io::Result<()> {
        let dirs =  fs::read_dir(directory).unwrap();
        for dir in dirs
        {
            let entry = dir.unwrap().path();
            if entry.is_dir()
            {
                let sub_dir = entry.display().to_string();
                get_files(&sub_dir, v)?;
            }
        }

        v.sort();

        Ok(())
    }




    // get all directories from a directory
    fn get_doc_collections(directory: &str, v:&mut Vec<u32>) -> io::Result<()> {
        let dirs =  fs::read_dir(directory).unwrap();
        for dir in dirs
        {
            let entry = dir.unwrap().path();
            if entry.is_dir()
            {
                let mut components = entry.components();
                let mut last = "";
                loop
                {
                    let c = components.next();
                    match c 
                    {
                        Some(x) => last = x.as_os_str().to_str().unwrap(),
                        None => break
                    }
                }

                let i: u32 = last.parse().unwrap();
                v.push(i);
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
        let doc_id: u32 = name_parts_ar[0].parse().unwrap_or(0);
        doc_id
    }


    //adds a word position to a particular WordBlock along with adjacent words
    fn add_word_to_hash_map(doc_id:u32,word_index:u32,law:u8,w:u128,raw:u8,hm:&mut HashMap<u128,WordBlock>, realoc_count:&mut u32)
    {
    

        let wb = hm.entry(w).or_insert_with(|| WordBlock {buffer:Vec::with_capacity(8 as usize),latest_doc_id:0,latest_index:0,word_count:0,capacity:0,address:0,position:0});

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
            
            wb.latest_index = 0; //Don't forget to reset the latest index
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
        wb.word_count = wb.word_count + 1;
    
    }


    fn write_to_buffer(doc_id:u32,word_index:u32,law:u8,w:u128,raw:u8,wb:&mut WordBlock2, buffer: &mut Vec<u8>)
    {
        //Write the doc_id if necessary
        if doc_id!=wb.latest_doc_id
        {
            //Write terminator bytes for previous doc
            if buffer.len() > 0
            {
                buffer.push(0x7f); 
                buffer.push(0xff);
            }

            wb.latest_doc_id = doc_id;
            let b1 : u8 = ((doc_id >> 24) & 0xff) as u8;
            let b2 : u8 = ((doc_id >> 16) & 0xff) as u8;
            let b3 : u8 = ((doc_id >> 8) & 0xff) as u8;
            let b4 : u8 = (doc_id & 0xff) as u8;
            buffer.extend([b1, b2, b3, b4].iter().copied());
            
            wb.latest_index = 0; //Don't forget to reset the latest index
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


            buffer.push(((offset as u16 >> 8) as u8 | more_ind) as u8);
            buffer.push((offset as u16 & 0xff) as u8);

            if law !=255 && raw==255 //If left is set
            {
                buffer.push(0b10000000 | law);
            }
            else if raw != 255 && law==255 //If right is set
            {
                buffer.push(0b01000000 | raw);
            }
            else if law!=255 && raw != 255 //if both
            {
                buffer.push(0b11000000 | law);
                buffer.push(raw);
            }

        }
        else //This indicates offset overflow.
        {
            //Write the firt 15 bits of the offset address along with more_ind
            buffer.push(((offset as u16 >> 8) as u8 | more_ind) as u8);
            buffer.push((offset as u16 & 0xff) as u8);

            //0b0001
            let mut ext_byte = (offset as u32 >> 15) as u8 & 0b00011111;

            //if law or raw is present explicitly set the ext_more bit.
            if law !=255 || raw != 255
            {
                ext_byte = ext_byte | 0b00100000;
            }
        
            //push the 5 remaining extended offset address bits  along with 3 leading bits 00 & 0 or 1 depending on the presence of law and/or raw
            buffer.push(ext_byte);

            if law !=255 && raw==255 //If left is set
            {
                buffer.push(0b10000000 | law);
            }
            else if raw != 255 && law==255 //If right is set
            {
                buffer.push(0b01000000 | raw);
            }
            else if law!=255 && raw != 255 //if both
            {
                buffer.push(0b11000000 | law);
                buffer.push(raw);
            }

        }
    }

    //adds a word position to a particular WordBlock along with adjacent words
    fn add_word_to_hash_map_2(doc_id:u32,word_index:u32,law:u8,w:u128,raw:u8,hm:&mut HashMap<u128,WordBlock2>,master_vec:&mut Vec<Vec<u8>>)
    {
    
        let wb = hm.entry(w).or_insert_with(|| WordBlock2 {buffer_id:-1,latest_doc_id:0,latest_index:0,word_count:0,capacity:0,address:0,position:0});

        if wb.buffer_id == -1
        {
            wb.buffer_id = master_vec.len() as i32; //assign buffer id
            let mut buffer = Vec::with_capacity(8); //allocate new buffer
            write_to_buffer(doc_id,word_index,law,w,raw,wb,&mut buffer);
            master_vec.push(buffer); 
        }
        else
        {
            let buffer = &mut master_vec[wb.buffer_id as usize];
            write_to_buffer(doc_id,word_index,law,w,raw,wb,buffer);
        }
    
    }



    //Parses a particular file adding all word positions to WordBlocks.
    fn parse_file(doc_id: u32, file_name: &str, content: &mut String, hm:&mut HashMap<u128,WordBlock>, com_words:& HashMap<u128, u8>, realoc_count:&mut u32) ->  io::Result<u32> 
    {
        {
            let mut file = File::open(file_name)?;
            file.read_to_string(content)?;
        }
        // Read all the file content into a variable (ignoring the result of the operation).
        let mut word_index:u32 = 0;

        //let mut l:u128;
        let mut w:u128;
        let mut r:u128 = 0;

        let nomatch:u8 = 255;
    
        let mut rawh:u8 = nomatch;
        let mut cw:u8 = nomatch;
        let mut lawh:u8;

        let mut word = String::with_capacity(25);
        
        for c in content.chars() 
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
                        add_word_to_hash_map(doc_id,word_index - 1, lawh, w, rawh, hm, realoc_count);
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
                add_word_to_hash_map(doc_id,word_index - 1, lawh, w, rawh, hm, realoc_count);
            }

            //finally if at the end also add the last word if not common.println!
            //only add if not a common word.
            if rawh==255 && r!=0
            {
                add_word_to_hash_map(doc_id,word_index, cw, r, nomatch, hm, realoc_count);
            }
            word_index = word_index + 1;
        }

        content.truncate(0);
        Ok(word_index)
    }

    fn parse_file2(doc_id: u32, file_name: &str, content: &mut String, hm:&mut HashMap<u128,WordBlock>, com_words:& HashMap<u128, u8>, realoc_count: &mut u32) ->  io::Result<u32> 
    {
        {
            //println!("{}", file_name);
            let mut file = File::open(file_name)?;
            // Read all the file content into a variable (ignoring the result of the operation).
            file.read_to_string(content)?;
        }
        let mut word_index:u32 = 0;

        //let mut l:u128;
        let mut w:u128;
        let mut r:u128 = 0;

        let nomatch:u8 = 255;
    
        let mut rawh:u8 = nomatch;
        let mut cw:u8 = nomatch;
        let mut lawh:u8;

        //let mut word = String::with_capacity(25);

        let mut idx_s = 0;
        let mut idx_e = 0;
        let mut started = false;
        let mut i = 0;
        
        for c in content.chars() 
        { 
            if c.is_alphanumeric() || c=='\''
            {
                if !started
                {
                    idx_s = i;
                    started = true;
                }
                //word.push(c.to_ascii_lowercase());
                idx_e = i;
            }
            else 
            {
                if (idx_e as i32 - idx_s as i32)  >= 0 
                {
                    //l = w;
                    lawh = cw;
                    
                    w = r;
                    cw = rawh;

                    let slice = &content[idx_s..idx_e+1];
                    //println!("{}",slice);
                    
                    r = word_hash::hash_word_to_u128(&slice);
                    rawh = common_words::map_to(com_words,&r);
            
                    //only add if not a common word.
                    if cw==nomatch && w!=0
                    {
                        add_word_to_hash_map(doc_id,word_index - 1, lawh, w, rawh, hm, realoc_count);
                    }
                    word_index = word_index + 1;
                    started = false; //reset started flag.

                    if idx_s>0
                    {
                        idx_e = idx_s-1;
                    }

                }
            }

            i = i + 1;

        }

        if (idx_e as i32 - idx_s as i32)  >= 0 && started
        {
            
            //l = w;
            lawh = cw;
            
            w = r;
            cw = rawh;
            
            let slice = &content[idx_s..idx_e+1];
            r = word_hash::hash_word_to_u128(&slice);
            rawh = common_words::map_to(com_words,&r);

            //only add if not a common word.
            if cw==255 && w!=0
            {
                add_word_to_hash_map(doc_id,word_index - 1, lawh, w, rawh, hm, realoc_count);
            }

            //finally if at the end also add the last word if not common.println!
            //only add if not a common word.
            if rawh==255 && r!=0
            {
                add_word_to_hash_map(doc_id,word_index, cw, r, nomatch, hm, realoc_count);
            }
            word_index = word_index + 1;
        }


        content.truncate(0);
     
        Ok(word_index)
    }



   


    fn get_hash_bucket(name: &str, worker_count: u8)->u32
    {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        let hash = hasher.finish();
        (hash % worker_count as u64) as u32
    }

 

 
    fn index(doc_files: &Vec<String>, hm: &mut HashMap<u128,WordBlock>, common_word_path:&str,collection_index:u32,collection_count: u32, worker_id:u8, worker_count:u8, limit:u32, realoc_count:&mut u32)
    {
        //let full_path = Path::new(source_path).join(doc_collection.to_string()).display().to_string();

        let mut com:HashMap<u128, u8> =  HashMap::new();
        common_words::load(common_word_path, & mut com).expect("Error Loading common words.");

        //let mut doc_files = Vec::new();
        //get_files_by_hash_bucket(&source_path,collection_index,collection_count, & mut doc_files).expect("Error Loading source file path.");


        let mut docs_to_process = Vec::new();
        get_files_by_hash_bucket(doc_files, collection_index,collection_count,worker_id,worker_count,limit, &mut docs_to_process).unwrap();
        //doc_files.sort();

        let mut count = 0;
        let content_ref:RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1000000));
    
        for doc_file in docs_to_process 
        {
            //println!("worker:{} doc:{}",worker_id,doc_file);
      
            if limit !=0 && count >= limit
            {
                break;
            }

            if doc_file == ""
            {
                break;
            }

            let doc_id = get_doc_id(&doc_file);
            //skip if unable 
            if doc_id == 0  
            {
                println!("Could not get doc id from: {}", doc_file);
                continue;
            }

            //parse_file(doc_id, &doc_file, &mut content, hm, &com).expect("Unable to parse file.");

          
            let mut file = File::open(doc_file).unwrap();
            let mut content = &mut *content_ref.borrow_mut();
            content.clear();
            file.read_to_end(&mut content).unwrap();

            // Read all the file content into a variable (ignoring the result of the operation).
            let mut word_index:u32 = 0;

            //let mut l:u128;
            let mut w:u128;
            let mut r:u128 = 0;

            let nomatch:u8 = 255;
        
            let mut rawh:u8 = nomatch;
            let mut cw:u8 = nomatch;
            let mut lawh:u8;
       
            let mut word: Vec<u8> = Vec::with_capacity(8);
           
                        
            for c in content
            { 
                if (*c as char).is_alphanumeric() || (*c as char)=='\''
                {
                    word.push(*c);
                }
                else 
                {
                    if word.len() > 0
                    {
                        //l = w;
                        lawh = cw;
                        
                        w = r;
                        cw = rawh;
                        
                        r = word_hash::hash_v_word_to_u128(&word);
                        rawh = common_words::map_to(&com,&r);

                        
                        //only add if not a common word.
                        if cw==nomatch && w!=0
                        {
                            add_word_to_hash_map(doc_id,word_index - 1, lawh, w, rawh, hm, realoc_count);
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
                
                r = word_hash::hash_v_word_to_u128(&word);
                rawh = common_words::map_to(&com,&r);

                //only add if not a common word.
                if cw==255 && w!=0
                {
                    add_word_to_hash_map(doc_id,word_index - 1, lawh, w, rawh, hm, realoc_count);
                }

                //finally if at the end also add the last word if not common.println!
                //only add if not a common word.
                if rawh==255 && r!=0
                {
                    add_word_to_hash_map(doc_id,word_index - 1, cw, r, nomatch, hm, realoc_count);
                    //add_word_to_big_v(doc_id,word_index, cw, r, nomatch, hm, big_v);
                }
                word_index = word_index + 1;
            }

            count+=1;
        }

        add_terminators(hm);

        println!("worker_id: {:?}  count: {:?}", worker_id, count);
    }



    fn index_2(doc_files: &Vec<String>, hm: &mut HashMap<u128,WordBlock2>, common_word_path:&str,collection_index:u32,collection_count: u32, worker_id:u8, worker_count:u8, limit:u32, master_vec:&mut Vec<Vec<u8>>)
    {
        //let full_path = Path::new(source_path).join(doc_collection.to_string()).display().to_string();

        let mut com:HashMap<u128, u8> =  HashMap::new();
        common_words::load(common_word_path, & mut com).expect("Error Loading common words.");

    
        let mut docs_to_process = Vec::new();
        get_files_by_hash_bucket(doc_files, collection_index,collection_count,worker_id,worker_count,limit, &mut docs_to_process).unwrap();
    
        let mut count = 0;
        let content_ref:RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1000000));
    
        for doc_file in docs_to_process 
        {
            //println!("worker:{} doc:{}",worker_id,doc_file);
      
            if limit !=0 && count >= limit
            {
                break;
            }

            if doc_file == ""
            {
                break;
            }

            let doc_id = get_doc_id(&doc_file);
            //skip if unable 
            if doc_id == 0  
            {
                println!("Could not get doc id from: {}", doc_file);
                continue;
            }

            //parse_file(doc_id, &doc_file, &mut content, hm, &com).expect("Unable to parse file.");

          
            let mut file = File::open(doc_file).unwrap();
            let mut content = &mut *content_ref.borrow_mut();
            content.clear();
            file.read_to_end(&mut content).unwrap();

            // Read all the file content into a variable (ignoring the result of the operation).
            let mut word_index:u32 = 0;

            //let mut l:u128;
            let mut w:u128;
            let mut r:u128 = 0;

            let nomatch:u8 = 255;
        
            let mut rawh:u8 = nomatch;
            let mut cw:u8 = nomatch;
            let mut lawh:u8;
       
            let mut word: Vec<u8> = Vec::with_capacity(20);
           
                        
            for c in content
            { 
                if (*c as char).is_alphanumeric() || (*c as char)=='\''
                {
                    word.push(*c);
                }
                else 
                {
                    if word.len() > 0
                    {
                        //l = w;
                        lawh = cw;
                        
                        w = r;
                        cw = rawh;
                        
                        r = word_hash::hash_v_word_to_u128(&word);
                        rawh = common_words::map_to(&com,&r);

                        
                        //only add if not a common word.
                        if cw==nomatch && w!=0
                        {
                            add_word_to_hash_map_2(doc_id,word_index - 1, lawh, w, rawh, hm, master_vec);
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
                
                r = word_hash::hash_v_word_to_u128(&word);
                rawh = common_words::map_to(&com,&r);

                //only add if not a common word.
                if cw==255 && w!=0
                {
                    add_word_to_hash_map_2(doc_id,word_index - 1, lawh, w, rawh, hm, master_vec);
                }

                //finally if at the end also add the last word if not common.println!
                //only add if not a common word.
                if rawh==255 && r!=0
                {
                    add_word_to_hash_map_2(doc_id,word_index - 1, cw, r, nomatch, hm, master_vec);
                    //add_word_to_big_v(doc_id,word_index, cw, r, nomatch, hm, big_v);
                }
                word_index = word_index + 1;
            }

            count+=1;
        }

        //TODO:Fix
        //add_terminators(hm);

        println!("worker_id: {:?}  count: {:?}", worker_id, count);
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
            let wb = master.entry(*k).or_insert_with(|| WordBlock {buffer:Vec::new(),latest_doc_id:0,latest_index:0, word_count:0,capacity:0,address:0,position:0});

            wb.word_count = wb.word_count + v.word_count;
            wb.buffer.extend(v.buffer.iter().cloned())
        }
        let e = s.elapsed();
        println!("copy time: {:?} ", e);

    }

    fn get_count(m: & HashMap<u128,WordBlock>) -> (u64,u64)
    {
        let mut word_count = 0;
        let mut byte_count = 0;
          for (_, v) in m.iter() {
            word_count = word_count + v.word_count;
            byte_count = byte_count + v.buffer.len();
       
        }
        (word_count,byte_count as u64)
    }


    pub fn index_all(source_path:String, common_word_path:String,index_path :String, collection_count:u32, worker_count:u8, limit:u32)->io::Result<()>
    {
        let s = Instant::now();

        let mut doc_files = Vec::new();
        let mut realoc_count:u32 = 0;
          
        get_all_files(&source_path,&mut doc_files)?;
        let e = s.elapsed();
        println!("get file paths: {:?}, {} files listed to load", e,doc_files.len());


        let mut count = 0;

        let doc_files_arc = Arc::new(doc_files); 
        let source_path_arc = Arc::new(source_path);
        let common_word_path_arc = Arc::new(common_word_path);
        let index_path_arc = Arc::new(index_path);


        for collection_index in 0..collection_count 
        {
            println!("indexing Collection: {}", collection_index);
            count += index_files(&doc_files_arc, &source_path_arc, &common_word_path_arc, &index_path_arc,collection_index, collection_count, worker_count, limit, &mut realoc_count);
        }

        let e = s.elapsed();
        println!("total time: {:?} total words indexed count:{}, realocation count:{}", e, count, realoc_count);

       
        Ok(())
    }

    /*
    pub fn index_all_perf_test(source_path:&'static str, common_word_path:&'static str, collection_count:u32, worker_count:u8, limit:u32)->io::Result<()>
    {
        let mut realoc_count: u32 = 0;
        let s = Instant::now();

        let mut doc_files = Vec::new();
      
          
        get_all_files(&source_path,&mut doc_files)?;
        let e = s.elapsed();
        println!("get file paths: {:?}, {} files listed to load", e,doc_files.len());


        let mut count = 0;

        let doc_files_arc = Arc::new(doc_files); 

        let mut doc_files = Vec::new();
        get_all_files(&source_path,&mut doc_files).unwrap();

        let data_path = "C:\\Dev\\rust\\fts\\data\\index";
        let wad_suffix = ["0".to_string(),".wad".to_string()].join(&String::from("_"));
        let seg_suffix = ["0".to_string(),".seg".to_string()].join(&String::from("_"));

        let wad_file = Path::new(data_path).join(wad_suffix).display().to_string();
        let segment_file = Path::new(data_path).join(seg_suffix).display().to_string();
        let mut hm:HashMap<u128,WordBlock> = HashMap::new();

        let s = Instant::now();
        index(&doc_files,&mut hm,common_word_path,0,collection_count,255,1,limit,&mut realoc_count);
        let counts = get_count(&hm);
  
        let e = s.elapsed();
        println!("time: {:?} count:{:?}", e,counts);
        //indexer_diagnostics::traverse_hm(&hm, true);

        println!("saving...");
       
        //index_writer::write_new(wad_file ,word_block, &hm,50);

        let saving_start = Instant::now();

        index_writer::write_segment(&wad_file,&segment_file,&hm).unwrap();
    
        //rocks_db::save(db, &hm, collection_index);

        let saving_end = saving_start.elapsed();
        println!("save time: {:?}", saving_end);



        let e = s.elapsed();
        println!("total time: {:?} total words indexed count:{}", e, count);

       
        Ok(())
    }
    */


    /*
    fn build_freq_map(doc_files: & Vec<String>, fm: &mut HashMap<u128,u32>, common_word_path:&'static str,collection_count: u32, doc_sample_size:u32, top_n: usize)
    {
        let mut realoc_count: u32 = 0;
        let mut hm:HashMap<u128,WordBlock> = HashMap::new();

        index(&doc_files,&mut hm,common_word_path,0,collection_count,255,1,doc_sample_size,&mut realoc_count,fm,5.1);
        indexer_diagnostics::build_freq_map(&hm, top_n, fm);
    }
    */



    pub fn index_files(doc_files_arc: &Arc<Vec<String>>, source_path_arc:&Arc<String>, common_word_path_arc:&Arc<String>,index_path_arc:&Arc<String>, collection_index:u32, collection_count: u32, worker_count:u8, limit:u32, realoc_count:&mut u32) -> u64
    {
        if worker_count == 1
        {
                //Build Frequency map here.
                /*
                let s = Instant::now();
                let mut fm:HashMap<u128,u32> = HashMap::new();
                let doc_sample_size = 100;
                build_freq_map(&doc_files,& mut fm, common_word_path,collection_count, doc_sample_size,10000);
                let e = s.elapsed();
                println!("frequency map time for sample size of {} : {:?} ", doc_sample_size, e);*/


                //let data_path = "C:\\Dev\\rust\\fts\\data\\index";
                let wad_suffix = [collection_index.to_string(),".wad".to_string()].join(&String::from("_"));
                let seg_suffix = [collection_index.to_string(),".seg".to_string()].join(&String::from("_"));

                let wad_file = join(&index_path_arc,&wad_suffix); //Path::new(index_path).join(wad_suffix).display().to_string();
                let segment_file = join(&index_path_arc,&seg_suffix); //Path::new(index_path).join(seg_suffix).display().to_string();


                let mut hm:HashMap<u128,WordBlock> = HashMap::with_capacity(100_000);
        
                let s = Instant::now();

                index(&doc_files_arc,&mut hm,&common_word_path_arc,collection_index,collection_count,255,1,limit,realoc_count);
    
                let counts = get_count(&hm);


                //indexer_diagnostics::list_top(&hm,16000);
        
                let e = s.elapsed();
                println!("time: {:?} ", e);
                //println!("time: {:?} count:{:?}", e,counts);
                //indexer_diagnostics::traverse_hm(&hm, true);

                println!("saving...");
            
    
                let saving_start = Instant::now();

                index_writer::write_segment(&wad_file,&segment_file,&hm).unwrap();
            
                let saving_end = saving_start.elapsed();
                println!("save time: {:?}", saving_end);
            
                
                println!("....");

                return counts.0;
    
        }
        {
            let s = Instant::now();
            let mut workers = vec![];
            for i in 0..worker_count 
            {
                let clone_arc = Arc::clone(&doc_files_arc);
                let common_word_path_arc_c = Arc::clone(&common_word_path_arc);
                let index_path_arc_c = Arc::clone(&index_path_arc);
                
                // Spin up another thread
                workers.push(thread::spawn(move || {
                    println!("spawning worker {}", i);
                    let mut hm:HashMap<u128,WordBlock> = HashMap::with_capacity(10_000);
                    let mut realoc_count:u32 = 0;

                    index(&clone_arc, &mut hm, &common_word_path_arc_c,collection_index,collection_count,i,worker_count,limit,&mut realoc_count);

                    let wad_suffix = [collection_index.to_string(),i.to_string(),".wad".to_string()].join(&String::from("_"));
                    let seg_suffix = [collection_index.to_string(),i.to_string(),".seg".to_string()].join(&String::from("_"));

                    
                    //let wad_file = Path::new(&index_path_arc_c).join(wad_suffix).display().to_string();
                    //let segment_file = Path::new(&index_path_arc_c).join(seg_suffix).display().to_string();

                    let wad_file = join(&index_path_arc_c, &wad_suffix);
                    let segment_file = join(&index_path_arc_c, &seg_suffix);

                    index_writer::write_segment(&wad_file,&segment_file,&hm).unwrap();
               
                    let counts = get_count(&hm);
            
                    counts.0
                }));
            }

            let mut count = 0;
            for worker in workers {
                count = count + worker.join().unwrap();
            }

            let e = s.elapsed();
            println!("time: {:?} ", e);
            return count;
        }
    }

    fn join(path:&str, suffix:&str) -> String
    {
        Path::new(path).join(suffix).display().to_string()
    }


    fn get_files_by_hash_bucket(doc_files: &Vec<String>,
        collection_index:u32,
        collection_count:u32, 
        worker_id:u8, 
        worker_count:u8,
        limit:u32,
        v:&mut Vec<String>) -> io::Result<()> 
    {
        let mut collection_size = doc_files.len() / collection_count as usize;

        if limit > 0
        {
            collection_size = limit as usize / collection_count as usize;
        }

        let from_index = collection_index as usize * collection_size;
        let mut to_index = (collection_index + 1) as usize * collection_size;

        //println!("collection_size:{} from_index:{} to_index: {}",collection_size,from_index,to_index);

        if limit>0 && to_index > limit as usize
        {
            to_index = limit as usize;
        }
        

        for i in from_index..to_index 
        {
            let hash_bucket = get_hash_bucket(&doc_files[i], worker_count);
            //only add file if hash_bucket matches
            if hash_bucket == worker_id as u32 || worker_id == 255
            {
                v.push(doc_files[i].clone());
            }
        }
        Ok(())
    }