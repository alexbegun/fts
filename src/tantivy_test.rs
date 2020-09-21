#[macro_use]

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::ReloadPolicy;
use tempdir::TempDir;
use std::time::{Instant};

use std::io::{self, prelude::*, BufReader};
use std::fs::{self};



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

    Ok(())
}

pub fn index(index_path: &str, data_path: &str)-> tantivy::Result<()>
{
    //let index_path = TempDir::new(index_path)?;
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_dir(&index_path, schema.clone())?;

    let body = schema.get_field("body").unwrap();

    let mut v:Vec<String> = Vec::new();

    let s = Instant::now();

    get_all_files(data_path,&mut v);

    let mut index_writer = index.writer(5_000_000_000)?;

    let mut count = 0;

    for doc_file in v
    {
        count = count+1;
        let mut doc = Document::default();
        let contents = fs::read_to_string(doc_file)?;

        doc.add_text(
            body,
            &contents,
        );

        index_writer.add_document(doc);

        if count % 10000 == 0
        {
            index_writer.commit().unwrap();
            println!("commiting... {}",count);
            let e = s.elapsed();
            println!("elapsed time: {:?}", e);        
        }

    }

    println!("done commit.");


    let e = s.elapsed();
    println!("total time: {:?}", e);

    Ok(())
}