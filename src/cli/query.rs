use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::{Args, ValueEnum};
use comfy_table::{presets::UTF8_FULL, Table};

use crate::hasher;
use crate::storage::{HashRecord, ParquetStorage, R2Storage, Storage};

#[derive(Args)]
pub struct QueryArgs {
    /// Hash to search for (hex string, can be prefix)
    pub hash: String,

    /// Database file
    #[arg(short, long, default_value = "hashes.parquet")]
    pub database: PathBuf,

    /// Filter by algorithm
    #[arg(short, long, value_parser = hasher::algo_value_parser())]
    pub algo: Option<String>,

    /// Output format
    #[arg(short, long, default_value = "plain")]
    pub format: OutputFormat,

    #[command(flatten)]
    pub r2: super::R2Args,

    /// Maximum number of results to return
    #[arg(short, long)]
    pub limit: Option<usize>,
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Plain,
    Json,
    Table,
}

pub fn run(args: QueryArgs) -> Result<()> {
    let hash_bytes = hex::decode(&args.hash)
        .map_err(|_| anyhow::anyhow!("Invalid hex string: {}", args.hash))?;

    let results = if args.r2.enabled {
        let r2_config = args.r2.build_config(&args.database)?;
        let storage = R2Storage::new(r2_config)?;
        storage.query(&hash_bytes, args.algo.as_deref(), args.limit)?
    } else {
        let storage = ParquetStorage::new(&args.database);
        storage.query(&hash_bytes, args.algo.as_deref(), args.limit)?
    };

    if results.is_empty() {
        bail!("No matches found");
    }

    match args.format {
        OutputFormat::Plain => print_plain(&results),
        OutputFormat::Json => print_json(&results)?,
        OutputFormat::Table => print_table(&results),
    }

    let count = results.len();
    let prefix = match args.format {
        OutputFormat::Json => "",
        _ => "\n",
    };
    crate::status!(
        "{}Found {} {}",
        prefix,
        count,
        if count == 1 { "result" } else { "results" }
    );

    Ok(())
}


fn format_sources(sources: &[String]) -> String {
    if sources.is_empty() {
        "-".to_string()
    } else {
        sources.join(", ")
    }
}

fn print_plain(results: &[HashRecord]) {
    for r in results {
        println!(
            "{} ({}, {})",
            r.preimage, r.algorithm, format_sources(&r.sources)
        );
    }
}

fn print_json(results: &[HashRecord]) -> Result<()> {
    #[derive(serde::Serialize)]
    struct JsonRecord {
        hash: String,
        preimage: String,
        algorithm: String,
        sources: Vec<String>,
    }

    let json_results: Vec<JsonRecord> = results
        .iter()
        .map(|r| JsonRecord {
            hash: hex::encode(&r.hash),
            preimage: r.preimage.clone(),
            algorithm: r.algorithm.clone(),
            sources: r.sources.clone(),
        })
        .collect();

    let json = serde_json::to_string_pretty(&json_results)?;
    println!("{}", json);
    Ok(())
}

fn print_table(results: &[HashRecord]) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec!["Preimage", "Algorithm", "Sources"]);

    for r in results {
        table.add_row(vec![
            r.preimage.clone(),
            r.algorithm.clone(),
            format_sources(&r.sources),
        ]);
    }

    println!("{table}");
}
