use std::io::{self};
use std::fs::{self};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct InputFileSet
{
   files: Vec<String>
}

impl InputFileSet
{
    pub fn new() -> InputFileSet
    {
        InputFileSet {files:Vec::new()}
    }

    // get all files from a directory and store them in a string vector
    pub fn load(&mut self, directory:&str) -> io::Result<()> {
        let dirs =  fs::read_dir(directory).unwrap();
        for dir in dirs
        {
            let entry = dir.unwrap().path();
            if entry.is_dir()
            {
                let sub_dir = entry.display().to_string();
                self.load_files(&sub_dir)?;
            }
        }

        self.files.sort();

        Ok(())
    }

    // get all files from a directory
    fn load_files(&mut self, directory: &str) -> io::Result<()> {
        let dirs =  fs::read_dir(directory).unwrap();
        for dir in dirs
        {
            let entry = dir.unwrap().path();
            if !entry.is_dir()
            {
                let file = entry.display().to_string();
                self.files.push(file);
            }
        }

        Ok(())
    }





    pub fn filter_by_hash_bucket(&self,
        collection_index:u32,
        collection_count:u32, 
        worker_id:u8, 
        worker_count:u8,
        limit:u32) -> io::Result<Vec<String>> 
    {
        let mut filtered_vec = Vec::new();
        let mut collection_size = self.files.len() / collection_count as usize;

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

        //only for last
        if limit == 0 && collection_index == collection_count -1
        {
            to_index = self.get_count();
        }
        
        //let mut count = 0;
        for i in from_index..to_index
        {
            let hash_bucket = get_hash_bucket(&self.files[i], worker_count);
            //only add file if hash_bucket matches
            if hash_bucket == worker_id as u32 || worker_id == 255
            {
                filtered_vec.push(self.files[i].clone());
            }
        }

        //println!("{}-{} total: {}", from_index,to_index,count);

        Ok(filtered_vec)
    }

    pub fn get_count(&self) -> usize
    {
        self.files.len()
    }


}

fn get_hash_bucket(name: &str, worker_count: u8)->u32
{
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % worker_count as u64) as u32
}

//derives a document id from file name
pub fn get_doc_id(doc_file:&str) -> u32 
{
    //KLUDGE: NEED TO FIX THIS BETTER SO WORKS ON LINUX
    let mut path_parts_ar:Vec<&str> = doc_file.split("/").collect();
    if path_parts_ar.len() == 1
    {
        path_parts_ar = doc_file.split("\\").collect();
    }

    let file_parts_ar:Vec<&str> = path_parts_ar[path_parts_ar.len() - 1].split(".").collect();
    let name_parts_ar:Vec<&str> = file_parts_ar[0].split("-").collect();
    let doc_id: u32 = name_parts_ar[0].parse().unwrap_or(0);
    doc_id
}



