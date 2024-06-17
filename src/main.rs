use std::fs::File;
use std::path::PathBuf;
use std::io::{BufReader, BufRead, Cursor, Write, Read};
use std::time::Instant;
use anyhow::{anyhow, Result, Error};
use clap::{Parser};
use serde_json;
use serde_json::Value;
use flate2::read::MultiGzDecoder;   
use flate2::write::GzEncoder;
use flate2::Compression;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::s3::{is_s3, expand_s3_dir, get_reader_from_s3, write_cursor_to_s3};
use indicatif::{ProgressBar,ProgressStyle};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::available_parallelism;
use threadpool::ThreadPool;
use glob::glob; 
use std::hash::{Hash, Hasher, DefaultHasher};
use dashmap::DashMap;


pub mod s3;

/*============================================
=            Args                            =
============================================*/

#[derive(Parser, Debug)]
struct Args {
    /// (List of) directories/files (on s3 or local) that are jsonl.gz or jsonl.zstd files
    #[arg(required=true, long, num_args=1..)]
    input: Vec<PathBuf>,

    /// Output location (may be an s3 uri)
    #[arg(required=true, long)]
    output: PathBuf,

    /// How many copies are allowed of a single document
    #[arg(long, default_value_t=1)]
    allowed_copies: usize,

    /// How many threads to use (default is max available)
    #[arg(long, default_value_t=0)]
    threads: usize,

    /// Seed to initialize hasher 
    #[arg(long, default_value_t=1234)]
    hash_seed: usize,

    /// How many times to retry s3 operations
    #[arg(long, default_value_t=3)]
    s3_retries: usize,
}


/*================================================
=            Utilities/Helpers                   =
================================================*/

async fn expand_dirs(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {    
    // Can handle both local and s3 files/directories
    // Note that this function is async because we need to wait for all the s3 files to get expanded
    // Also note that the output vector is SORTED (in case S3 has inconsistent list_objects_v2 ordering)
    let mut files: Vec<PathBuf> = Vec::new();
    for path in paths {
        if is_s3(path) {
            let s3_result = expand_s3_dir(path).await?;
            for file in s3_result {
                files.push(file.clone());
            }
        } else if path.is_dir() {
            let path_str = path
                .to_str()
                .ok_or_else(|| anyhow!("invalid path '{}'", path.to_string_lossy()))?;
            for entry in glob(&format!("{}/**/*.json*.gz", path_str))? {
                files.push(entry?.to_path_buf());
            }
        } else {
            files.push(path.clone());
        }
    }   
    files.sort();
    Ok(files)
}


fn get_output_filename(inputs: &[PathBuf], input_filename: &PathBuf, output_directory: &PathBuf) -> PathBuf {
    // More fancy output-file naming that no longer assumes unique inputs
    let matching_prefix = inputs
        .iter()
        .find(|pfx| input_filename.starts_with(pfx))
        .expect("No matching prefix found?!?");

    let relative_path = input_filename.strip_prefix(matching_prefix).unwrap();
    output_directory.clone().join(relative_path)

}

fn read_file_into_memory(input_file: &PathBuf) ->Result<Cursor<Vec<u8>>, Error>{
    // Takes a local file (must be local!) and reads it into a Cursor of bytes
    let mut file = File::open(input_file).expect("Failed to open file");

    let mut contents = Vec::new();
    let ext = input_file.extension().unwrap().to_string_lossy().to_lowercase();
    if ext == "gz" {
        // Gzip case        
        let mut decoder = MultiGzDecoder::new(file);
        decoder.read_to_end(&mut contents).expect("Failed to read loca gzip file");
    } else if ext == "zstd" || ext == "zst" {
        // Zstd case
        let mut decoder = ZstdDecoder::new(file).unwrap();
        decoder.read_to_end(&mut contents).expect("Failed to read local zstd file");
    } else {
        file.read_to_end(&mut contents).expect("Failed to read local file");

        // No compression case 
    }
    Ok(Cursor::new(contents))
}


fn hash_str(text: &str, seed: usize) -> u64 {
    // Hashes a vector of type T into a u64 hash value
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    text.hash(&mut hasher);
    hasher.finish()
}


/*=========================================================
=                Process file fxn + helpers               =
=========================================================*/


async fn process_file(input: &PathBuf, output: &PathBuf, dup_map: &Arc<DashMap<u64, usize>>, 
                      allowed_copies: usize, total_docs: Arc<AtomicUsize>, removed_docs: Arc<AtomicUsize>, 
                      hash_seed: usize, s3_retries: usize, pbar: Arc<Mutex<ProgressBar>>) -> Result<(), Error> {

    // Step a: Load file into memory/lines
    let reader = if is_s3(input) {
        get_reader_from_s3(input, Some(s3_retries)).await.unwrap()
    } else {
        BufReader::new(read_file_into_memory(&input).unwrap())
    };


    // Step b: Iterate over lines and check if seen so many duplicates
    let mut cur_lines = 0;
    let mut cur_removed = 0;
    let mut output_lines : Vec<String> = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let json: Value = serde_json::from_str(&line)?;
        let text = json["text"].as_str().unwrap();  
        let hash_val = hash_str(&text, hash_seed);
        let mut should_include = false;        
        dup_map.entry(hash_val).or_insert(0);
        dup_map.alter(&hash_val, |_, mut count| {
            count += 1;
            should_include = count <= allowed_copies;
            count
            });
        if should_include {
            output_lines.push(line);
        }
        cur_lines += 1;
        cur_removed += 1 - should_include as usize;

    }
    let output_lines = output_lines.join("\n");
    total_docs.fetch_add(cur_lines, Ordering::SeqCst);
    removed_docs.fetch_add(cur_removed, Ordering::SeqCst);

    // Step c: Take lines and write to output file, modify loggers
    if output_lines.len() == 0 {
        pbar.lock().unwrap().inc(1);    
        return Ok(());
    }
    let output_bytes = output_lines.as_bytes();
    let output_bytes = match output.extension().unwrap().to_str() {
        Some("gz") => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&output_bytes).unwrap();            
            encoder.finish().unwrap()
        },
        Some("zstd") => {
            let mut encoder = ZstdEncoder::new(Vec::new(), 0).unwrap();
            encoder.write_all(&output_bytes).unwrap();
            encoder.finish().unwrap()
        },
        _ => {output_bytes.to_vec()}
    };

    if is_s3(output) {
        let cursor = Cursor::new(output_bytes.to_vec());
        write_cursor_to_s3(&output, cursor).await.expect(format!("Unable to write to output file {:?}", output).as_str());
    } else {
        let mut file = File::create(output).expect(format!("Unable to create output file {:?}", output).as_str());
        file.write_all(&output_bytes).expect(format!("Unable to write to {:?}", output).as_str());
    }

    pbar.lock().unwrap().inc(1);    
    Ok(())
}


/*=================================================
=                Main logic flow                  =
=================================================*/


#[tokio::main]
async fn main()-> Result<()> {

    // Step 1: initialize things and collect files 
    let start_time = Instant::now();

    let args = Args::parse();
    let threads = if args.threads == 0 {
        available_parallelism().unwrap().get()
    } else {
        args.threads
    };
    let input_files = expand_dirs(&args.input).await?;
    let pbar = ProgressBar::new(input_files.len() as u64)
        .with_style(
            ProgressStyle::with_template(
                "Files {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]",
            ).unwrap()
        );
    let pbar = Arc::new(Mutex::new(pbar));
    let dup_map = Arc::new(DashMap::new());


    // Step 2: Iterate over all files and process
    let threadpool = ThreadPool::new(threads);
    let total_docs = Arc::new(AtomicUsize::new(0));
    let removed_docs = Arc::new(AtomicUsize::new(0));
    for input in input_files {    
        let output = get_output_filename(&args.input, &input, &args.output);
        let pbar = pbar.clone();
        let allowed_copies = args.allowed_copies.clone();
        let total_docs = Arc::clone(&total_docs);
        let removed_docs = Arc::clone(&removed_docs);
        let dup_map = Arc::clone(&dup_map);
        threadpool.execute(move || {        
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();   
            let result = rt.block_on(
                process_file(
                    &input,
                    &output,
                    &dup_map,
                    allowed_copies,
                    total_docs, 
                    removed_docs,
                    args.hash_seed,
                    args.s3_retries,
                    pbar
                    )                
                );            
            match result {            
                Err(err) => {
                    eprintln!("Error processing {:?}; {:?}", input, err);
                },
                _ => {},
            };
        });
    }
    threadpool.join();
    

    // Step 3: finalize
    println!("Finishing exact dedup run!");
    println!("-------------------------------");
    println!("Ran in {:?} (s)", start_time.elapsed().as_secs());
    println!("Saw {:?} documents | Removed {:?} of them", 
             total_docs.fetch_add(0, Ordering::SeqCst),
             removed_docs.fetch_add(0, Ordering::SeqCst));


    Ok(())
}



