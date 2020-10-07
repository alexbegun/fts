    use crate::word_hash;
    use crate::common_words::CwMap;
    //use crate::indexer_diagnostics;
    use crate::index_writer;
    use crate::input_file_set;
    use crate::input_file_set::InputFileSet;
    
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{self, prelude::*};
    use std::fs::{self};
    use std::time::{Instant};
    use std::thread;
    use std::path::{Path};

    use std::sync::Arc;


    
    use std::cell::RefCell;

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



    //adds a word position to a particular WordBlock along with adjacent words
    fn add_word_to_hash_map(doc_id:u32,word_index:u32,law:u8,w:u128,raw:u8,hm:&mut HashMap<u128,WordBlock>)
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



    fn process_word(common_words: &CwMap,  
                    hm: &mut HashMap<u128,WordBlock>, 
                    doc_id: u32,
                    w:&mut u128, 
                    r:&mut u128,
                    rawh:&mut u8,
                    cw:&mut u8,
                    lawh:&mut u8, 
                    word: &mut Vec<u8>,
                    word_index: &mut u32,
                    is_last: bool)
    {
        if word.len() == 1 && word[0] == 39
        {
            word.clear();
            return;
        }
        if word[0] == 39
        {
            word.remove(0);
        }
        if word[word.len()-1] == 39 
        {
            word.pop();
        }

        if word.len() == 0
        {
            return;
        }
       

        /*
        let wrd = std::str::from_utf8(&word).unwrap();
        if wrd.contains("'")
        {
            println!("{}",std::str::from_utf8(&word).unwrap());
        }
        */

        *lawh = *cw;
        *w = *r;
        *cw = *rawh;
        
        *rawh = common_words.map_to(&word);

        if *rawh == 255
        {
            *r = word_hash::hash_v_word_to_u128(&word);
        }
        else
        {
            *r = 0;
        }

        if *cw==255 && *w!=0 //only add if not a common word.
        {
            add_word_to_hash_map(doc_id, *word_index - 1, *lawh, *w, *rawh, hm);
        }

        if is_last
        {
                //finally if at the end also add the last word if not common.println!
                //only add if not a common word.
                if *rawh==255 && *r!=0
                {
                    add_word_to_hash_map(doc_id,*word_index - 1, *cw, *r, 255, hm);
                }
        }

        word.clear();
        *word_index += 1;
    }
 
    fn index(doc_files: &InputFileSet, common_words: &CwMap, hm: &mut HashMap<u128,WordBlock>,collection_index:u32,collection_count: u32, worker_id:u8, worker_count:u8, limit:u32)
    {
        let docs_to_process = doc_files.filter_by_hash_bucket(collection_index,collection_count,worker_id,worker_count,limit).unwrap();

        let mut doc_count = 0;
        let mut word_count = 0;
        let content_ref:RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1000000));
    
        for doc_file in docs_to_process 
        {
            //println!("worker:{} doc:{}",worker_id,doc_file);
            let doc_id = input_file_set::get_doc_id(&doc_file);
            if doc_id == 0 //skip if unable
            {
                println!("Could not get doc id from: {}", doc_file);
                continue;
            }

            let mut file = File::open(doc_file).unwrap();
            let mut content = &mut *content_ref.borrow_mut();
            content.clear();
            file.read_to_end(&mut content).unwrap();

            // Read all the file content into a variable (ignoring the result of the operation).
            let mut word_index:u32 = 0;
            let mut w:u128 = 0;
            let mut r:u128 = 0;
            let mut rawh:u8 = 255;
            let mut cw:u8 = 255;
            let mut lawh:u8 = 0;
            let mut word: Vec<u8> = Vec::with_capacity(20);
           
            for c in content
            { 
                match *c
                {
                    65..=90 => word.push(*c + 32), //If A - Z then turn to lower case
                    97..=122 | 39 | 48..=57 => word.push(*c), //If a-z, or ' - or 0-9
                    _ =>
                        {
                            if word.len() > 0
                            {
                                process_word(common_words,hm,doc_id,&mut w,&mut r,&mut rawh, &mut cw, &mut lawh, &mut word, & mut word_index, false);
                            }
                        }

                }
            }
            if word.len() > 0
            {
                process_word(common_words,hm,doc_id,&mut w,&mut r,&mut rawh, &mut cw, &mut lawh, &mut word, & mut word_index, true);
            }
            word_count+=word_index;
            doc_count+=1;
        }
        add_terminators(hm);
        println!("worker_id: {:?}  count: {:?} word count: {:?}", worker_id, doc_count, word_count);
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

        let mut doc_files = InputFileSet::new();
        doc_files.load(&source_path)?;
        
        let mut com_words = CwMap::new();
        com_words.load(&common_word_path)?;

        
        let e = s.elapsed();
        println!("get file paths: {:?}, {} files listed to load", e,doc_files.get_count());

        let mut count = 0;

        let doc_files_arc = Arc::new(doc_files); 
        let com_words_arc = Arc::new(com_words); 
        let source_path_arc = Arc::new(source_path);
        let index_path_arc = Arc::new(index_path);


        for collection_index in 0..collection_count 
        {
            println!("indexing Collection: {}", collection_index);
            count += index_files(&doc_files_arc,&com_words_arc, &source_path_arc, &index_path_arc,collection_index, collection_count, worker_count, limit);
        }

        let e = s.elapsed();
        println!("total time: {:?} total words indexed count:{}", e, count);

       
        Ok(())
    }

    
    pub fn index_files(doc_files_arc: &Arc<InputFileSet>, com_words_arc: &Arc<CwMap>, source_path_arc:&Arc<String>, index_path_arc:&Arc<String>, collection_index:u32, collection_count: u32, worker_count:u8, limit:u32) -> u64
    {
        if worker_count == 1
        {
                let wad_suffix = [collection_index.to_string(),".wad".to_string()].join(&String::from("_"));
                let seg_suffix = [collection_index.to_string(),".seg".to_string()].join(&String::from("_"));

                let wad_file = join(&index_path_arc,&wad_suffix); //Path::new(index_path).join(wad_suffix).display().to_string();
                let segment_file = join(&index_path_arc,&seg_suffix); //Path::new(index_path).join(seg_suffix).display().to_string();


                let mut hm:HashMap<u128,WordBlock> = HashMap::with_capacity(100_000);
        
                let s = Instant::now();

                index(&doc_files_arc,&com_words_arc, &mut hm,collection_index,collection_count,255,1,limit);
    
                let counts = get_count(&hm);
        
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
        else
        {
            let s = Instant::now();
            let mut workers = vec![];
            for i in 0..worker_count 
            {
                let doc_files_c = Arc::clone(&doc_files_arc);
                let com_word_c = Arc::clone(&com_words_arc);
                let index_path_arc_c = Arc::clone(&index_path_arc);
                
                // Spin up another thread
                workers.push(thread::spawn(move || {
                    println!("spawning worker {}", i);
                    let mut hm:HashMap<u128,WordBlock> = HashMap::with_capacity(20_000);
                
                    index(&doc_files_c,&com_word_c, &mut hm,collection_index,collection_count,i,worker_count,limit);

                    let wad_suffix = [collection_index.to_string(),i.to_string(),".wad".to_string()].join(&String::from("_"));
                    let seg_suffix = [collection_index.to_string(),i.to_string(),".seg".to_string()].join(&String::from("_"));

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
