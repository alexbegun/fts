    use crate::word_hash;
    use crate::common_words;
    use crate::indexer_diagnostics;
    use crate::rocks_db;
    use crate::index_writer;
    
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{self, prelude::*, BufReader};
    use std::fs::{self};
    use std::time::{Instant};
    use std::thread;
    use std::path::{Path, PathBuf};

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    use rocksdb::{Options, DB, MergeOperands};
    
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
    fn add_word_to_hash_map(doc_id:u32,word_index:u32,law:u8,w:u128,raw:u8,hm:&mut HashMap<u128,WordBlock>)
    {
        let wb = hm.entry(w).or_insert_with(|| WordBlock {buffer:Vec::with_capacity(64),latest_doc_id:0,latest_index:0,word_count:0,capacity:0,address:0,position:0});

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
        wb.word_count = wb.word_count + 1;
    
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


    fn get_hash_bucket(name: &str, collection_count: u32)->u32
    {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        let hash = hasher.finish();
        (hash % collection_count as u64) as u32
    }

    fn get_files_by_hash_bucket(directory: &str,collection_index:u32,collection_count: u32, v:&mut Vec<String>) -> io::Result<()> {
        let dirs =  fs::read_dir(directory).unwrap();
        for dir in dirs
        {
            let entry = dir.unwrap().path();
            if entry.is_dir()
            {
                let sub_dir = entry.display().to_string();
                let files_dir =  fs::read_dir(sub_dir).unwrap();

                for file in files_dir
                {
                    let file_entry = file.unwrap().path();
                    if !file_entry.is_dir()
                    {
                        let file = file_entry.display().to_string();
                        let hash_bucket = get_hash_bucket(&file, collection_count);
                        //only add file if hash_bucket matches
                        if hash_bucket == collection_index 
                        {
                            v.push(file);
                        }
                        
                    }
           
                }

                
            }
        }

        Ok(())
    }

    fn index(source_path:&str, common_word_path:&str,collection_index:u32,collection_count: u32, worker_id:u8, worker_count:u8) -> HashMap<u128,WordBlock>
    {
        //let full_path = Path::new(source_path).join(doc_collection.to_string()).display().to_string();

        let mut hm:HashMap<u128,WordBlock> = HashMap::new();
        let mut com:HashMap<u128, u8> =  HashMap::new();
        common_words::load(common_word_path, & mut com).expect("Error Loading common words.");

        let mut doc_files = Vec::new();
        get_files_by_hash_bucket(&source_path,collection_index,collection_count, & mut doc_files).expect("Error Loading source file path.");

        doc_files.sort();

        let mut count = 0;
    
        for doc_file in doc_files 
        {
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


    pub fn index_all(source_path:&'static str, common_word_path:&'static str, collection_count:u32, worker_count:u8)->io::Result<()>
    {
        let s = Instant::now();
        

        let mut directories = Vec::new();
        let mut count = 0;
        get_doc_collections(source_path, & mut directories).expect("Error Loading source file path.");
        directories.sort();


        for collection_index in 0..collection_count 
        {
            println!("indexing Collection: {}", collection_index);
            count += index_files(source_path, common_word_path, collection_index, collection_count, worker_count);
            
        }

        let e = s.elapsed();
        println!("total time: {:?} total word count:{}", e, count);
          
        Ok(())
    }

    pub fn index_files(source_path:&'static str, common_word_path:&'static str, collection_index:u32, collection_count: u32, worker_count:u8) -> u64
    {
        let path = "C:\\Dev\\rust\\fts\\data\\rocks.db";
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut db = DB::open(&opts, path).unwrap();

        if worker_count == 1
        {
            let s = Instant::now();
            let hm = index(source_path,common_word_path,collection_index,collection_count,255,1);
            let counts = get_count(&hm);

            let e = s.elapsed();
            println!("time: {:?} count:{:?}", e,counts);
            //indexer_diagnostics::traverse_hm(&hm, true);

            println!("saving...");
           
            //index_writer::write_new(wad_file ,word_block, &hm,50);

            let saving_start = Instant::now();


        
            rocks_db::save(&mut db, &hm, collection_index);

            let saving_end = saving_start.elapsed();
            println!("save time: {:?}", saving_end);
          
            //index_writer::write_existing(wad_file ,word_block, &hm,25);
            
            println!("....");

            return counts.0;


            //let mut hm2:HashMap<u128,WordBlock> = HashMap::new();
            //indexer_diagnostics::load_hm(wad_file, word_block, &mut hm2);

            //indexer_diagnostics::traverse_hm(&hm2, false);

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
                    let hm = index(source_path,common_word_path,collection_index,collection_count,i,worker_count);
                    hm
                }));
            }


            //Master Doc
            let mut mm:HashMap<u128,WordBlock> = HashMap::new();

            for worker in workers {
                let hm = worker.join().unwrap();
                copy_map(&mut mm,hm);
            }

            //index_writer::write_new(wad_file ,word_block, &mm,33);
            let saving_start = Instant::now();
            rocks_db::save(&mut db, &mm,collection_index);
            let saving_end = saving_start.elapsed();
            println!("save time: {:?}", saving_end);
         


            let count = get_count(&mm);
            let e = s.elapsed();
            println!("time: {:?} count:{:?}", e,count);
            return count.0;
        }

    }