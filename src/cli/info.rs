use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, ValueEnum};

use crate::config::{Config, R2Overrides};
use crate::storage::{ParquetStorage, R2Config, R2Storage, Storage};

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Plain,
    Json,
}

#[derive(Args)]
pub struct InfoArgs {
    #[arg(default_value = "hashes.parquet")]
    pub database: PathBuf,

    #[arg(short, long, default_value = "plain")]
    pub format: OutputFormat,

    #[arg(long)]
    pub r2: bool,

    #[arg(long, env = "SHAHA_R2_ENDPOINT")]
    pub endpoint: Option<String>,

    #[arg(long, env = "SHAHA_R2_BUCKET")]
    pub bucket: Option<String>,

    #[arg(long, env = "SHAHA_R2_ACCESS_KEY_ID")]
    pub access_key_id: Option<String>,

    #[arg(long, env = "SHAHA_R2_SECRET_ACCESS_KEY")]
    pub secret_access_key: Option<String>,

    #[arg(long, env = "SHAHA_R2_PATH")]
    pub r2_path: Option<String>,

    #[arg(long, env = "SHAHA_R2_REGION", default_value = "auto")]
    pub region: String,
}

pub fn run(args: InfoArgs) -> Result<()> {
    let (stats, location) = if args.r2 {
        let r2_config = build_r2_config(&args)?;
        let url = r2_config.s3_url();
        let storage = R2Storage::new(r2_config)?;
        (storage.stats()?, url)
    } else {
        let storage = ParquetStorage::new(&args.database);
        (storage.stats()?, args.database.display().to_string())
    };

    match args.format {
        OutputFormat::Plain => print_plain(&location, &stats),
        OutputFormat::Json => print_json(&location, &stats)?,
    }

    Ok(())
}

fn print_plain(location: &str, stats: &crate::storage::Stats) {
    println!("Database:   {}", location);
    println!("Records:    {}", stats.total_records);
    if stats.file_size_bytes > 0 {
        println!("Size:       {}", format_bytes(stats.file_size_bytes));
    }
    println!(
        "Algorithms: {}",
        if stats.algorithms.is_empty() {
            "-".to_string()
        } else {
            stats.algorithms.join(", ")
        }
    );
    println!(
        "Sources:    {}",
        if stats.sources.is_empty() {
            "-".to_string()
        } else {
            stats.sources.join(", ")
        }
    );
}

fn print_json(location: &str, stats: &crate::storage::Stats) -> Result<()> {
    #[derive(serde::Serialize)]
    struct JsonInfo {
        database: String,
        total_records: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        file_size_bytes: Option<u64>,
        algorithms: Vec<String>,
        sources: Vec<String>,
    }

    let info = JsonInfo {
        database: location.to_string(),
        total_records: stats.total_records,
        file_size_bytes: if stats.file_size_bytes > 0 {
            Some(stats.file_size_bytes)
        } else {
            None
        },
        algorithms: stats.algorithms.clone(),
        sources: stats.sources.clone(),
    };

    println!("{}", serde_json::to_string_pretty(&info)?);
    Ok(())
}

fn build_r2_config(args: &InfoArgs) -> Result<R2Config> {
    let default_path = args.database.file_name()
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

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
