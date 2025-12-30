use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;

use crate::config::{Config, R2Overrides};
use crate::hasher::{self, Hasher};
use crate::output;
use crate::source;
use crate::status;
use crate::storage::{HashRecord, ParquetStorage, R2Config, R2Storage, Storage};

const BATCH_SIZE: usize = 100_000;

#[derive(Args)]
pub struct BuildArgs {
    /// Input file (for backward compatibility)
    pub input: Option<PathBuf>,

    /// Source specification (seclists:path, aspell:lang, file:path, or URL)
    #[arg(long)]
    pub from: Option<String>,

    /// Hash algorithms to use
    #[arg(short, long, default_value = "sha256", value_parser = hasher::algo_value_parser())]
    pub algo: Vec<String>,

    /// Output file
    #[arg(short, long, default_value = "hashes.parquet")]
    pub output: PathBuf,

    /// Source name for metadata (defaults to source name)
    #[arg(short, long)]
    pub name: Option<String>,

    /// Append to existing database (merge sources)
    #[arg(long)]
    pub append: bool,

    /// Force rebuild even if source was already processed
    #[arg(long)]
    pub force: bool,

    /// Upload to R2/S3 storage instead of local file
    #[arg(long)]
    pub r2: bool,

    /// R2/S3 endpoint URL (or SHAHA_R2_ENDPOINT env var)
    #[arg(long, env = "SHAHA_R2_ENDPOINT")]
    pub endpoint: Option<String>,

    /// R2/S3 bucket name (or SHAHA_R2_BUCKET env var)
    #[arg(long, env = "SHAHA_R2_BUCKET")]
    pub bucket: Option<String>,

    /// R2/S3 access key ID (or SHAHA_R2_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID env var)
    #[arg(long, env = "SHAHA_R2_ACCESS_KEY_ID")]
    pub access_key_id: Option<String>,

    /// R2/S3 secret access key (or SHAHA_R2_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY env var)
    #[arg(long, env = "SHAHA_R2_SECRET_ACCESS_KEY")]
    pub secret_access_key: Option<String>,

    /// Path within bucket (defaults to output filename)
    #[arg(long, env = "SHAHA_R2_PATH")]
    pub r2_path: Option<String>,

    /// R2/S3 region (default: "auto" for R2)
    #[arg(long, env = "SHAHA_R2_REGION", default_value = "auto")]
    pub region: String,
}

type RecordKey = (Vec<u8>, String);

pub fn run(args: BuildArgs) -> Result<()> {
    let hashers: Vec<Box<dyn Hasher>> = args
        .algo
        .iter()
        .map(|name| hasher::get_hasher(name).expect("algorithm validated by clap"))
        .collect();

    if hashers.is_empty() {
        bail!("No valid algorithms specified");
    }

    let source_spec = match (&args.input, &args.from) {
        (None, None) => bail!(
            "Either INPUT or --from required.\n\
            Examples:\n  \
            shaha build words.txt\n  \
            shaha build --from seclists:Passwords/rockyou.txt\n  \
            shaha build --from aspell:en"
        ),
        (Some(_), Some(_)) => bail!("Cannot use both INPUT and --from"),
        (None, Some(spec)) => spec.clone(),
        (Some(input), None) => input.to_string_lossy().to_string(),
    };

    let data_source = source::parse(&source_spec)?;
    let source_name = args.name.clone().unwrap_or_else(|| data_source.name().to_string());
    let source_hash = data_source.content_hash()?;

    if !args.force && !args.r2 && args.output.exists() {
        if let Some(ref hash) = source_hash {
            let existing_storage = ParquetStorage::new(&args.output);
            let existing_hashes = existing_storage.get_source_hashes()?;
            if existing_hashes.contains(hash) {
                status!(
                    "Source already processed (content hash {}). Use --force to rebuild.",
                    &hash[..12]
                );
                return Ok(());
            }
        }
    }

    status!("Reading words from {}...", data_source.name());

    let words_iter = data_source.words()?;

    let mut total_words = 0usize;
    let mut unique_words = 0usize;
    let mut batch: Vec<String> = Vec::with_capacity(BATCH_SIZE);
    let mut seen: HashSet<String> = HashSet::new();
    let mut new_records_map: HashMap<RecordKey, HashRecord> = HashMap::new();

    let pb = if output::is_quiet() {
        ProgressBar::hidden()
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb
    };

    for word in words_iter {
        total_words += 1;

        if seen.insert(word.clone()) {
            batch.push(word);

            if batch.len() >= BATCH_SIZE {
                process_new_words(&batch, &hashers, &source_name, &mut new_records_map);
                unique_words += batch.len();

                pb.set_message(format!(
                    "{} words ({} unique), {} hashes",
                    total_words, unique_words, new_records_map.len()
                ));

                batch.clear();
            }
        }
    }

    if !batch.is_empty() {
        process_new_words(&batch, &hashers, &source_name, &mut new_records_map);
        unique_words += batch.len();
    }

    pb.finish_and_clear();

    let mut existing_count = 0usize;
    let mut merged_count = 0usize;
    let mut final_records: Vec<HashRecord> = Vec::new();

    if args.append && !args.r2 && args.output.exists() {
        status!("Streaming existing database for merge...");
        let existing_storage = ParquetStorage::new(&args.output);
        
        existing_storage.for_each_record(|mut record| {
            existing_count += 1;
            let key = (record.hash.clone(), record.algorithm.clone());
            
            if let Some(new_record) = new_records_map.remove(&key) {
                for source in new_record.sources {
                    if !record.sources.contains(&source) {
                        record.sources.push(source);
                        merged_count += 1;
                    }
                }
            }
            final_records.push(record);
            Ok(())
        })?;
        
        status!("Processed {} existing records, {} sources merged", existing_count, merged_count);
    }

    let new_records = new_records_map.len();
    final_records.extend(new_records_map.into_values());

    status!("Sorting and writing {} total records...", final_records.len());

    final_records.sort_by(|a, b| a.hash.cmp(&b.hash));

    let output_location: String;
    
    if args.r2 {
        let r2_config = build_r2_config(&args)?;
        output_location = r2_config.s3_url();
        
        status!("Uploading to {}...", output_location);
        let mut storage = R2Storage::new(r2_config)?;
        for chunk in final_records.chunks(BATCH_SIZE) {
            storage.write_batch(chunk.to_vec())?;
        }
        storage.finish()?;
    } else {
        output_location = args.output.display().to_string();
        let mut storage = ParquetStorage::with_expected_capacity(&args.output, final_records.len());
        if let Some(ref hash) = source_hash {
            storage.add_source_hash(hash);
        }
        for chunk in final_records.chunks(BATCH_SIZE) {
            storage.write_batch(chunk.to_vec())?;
        }
        storage.finish()?;
    }

    let duplicates = total_words - unique_words;
    status!(
        "Processed {} words ({} unique, {} duplicates skipped)",
        total_words, unique_words, duplicates
    );
    if args.append && existing_count > 0 {
        status!(
            "Records: {} existing + {} new ({} sources merged) = {} total",
            existing_count, new_records, merged_count, 
            final_records.len()
        );
    } else {
        status!("Generated {} hash records", final_records.len());
    }
    status!("Wrote to {}", output_location);

    Ok(())
}

fn build_r2_config(args: &BuildArgs) -> Result<R2Config> {
    let default_path = args.output.file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "hashes.parquet".to_string());

    let overrides = R2Overrides {
        endpoint: args.endpoint.as_deref(),
        bucket: args.bucket.as_deref(),
        access_key_id: args.access_key_id.as_deref(),
        secret_access_key: args.secret_access_key.as_deref(),
        path: args.r2_path.as_deref(),
        region: &args.region,
        default_path: &default_path,
    };

    Config::load().unwrap_or_default().build_r2_config(overrides)
}

fn process_new_words(
    words: &[String],
    hashers: &[Box<dyn Hasher>],
    source_name: &str,
    records_map: &mut HashMap<RecordKey, HashRecord>,
) {
    let new_records: Vec<HashRecord> = words
        .par_iter()
        .flat_map(|word| {
            hashers
                .iter()
                .map(|hasher| HashRecord {
                    hash: hasher.hash(word.as_bytes()),
                    preimage: word.clone(),
                    algorithm: hasher.name().to_string(),
                    sources: vec![source_name.to_string()],
                })
                .collect::<Vec<_>>()
        })
        .collect();

    for record in new_records {
        let key = (record.hash.clone(), record.algorithm.clone());
        records_map.entry(key).or_insert(record);
    }
}
