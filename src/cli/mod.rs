pub mod build;
pub mod hash;
pub mod info;
pub mod query;
pub mod source;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "shaha")]
#[command(about = "Hash database builder and reverse lookup tool (SHA + aha!)")]
#[command(version)]
pub struct Cli {
    /// Suppress progress output (errors still shown)
    #[arg(short, long, global = true)]
    pub quiet: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Build hash database from input file
    Build(build::BuildArgs),
    /// Compute hash of input text
    Hash(hash::HashArgs),
    /// Query hash database for preimage
    Query(query::QueryArgs),
    /// Show database statistics
    Info(info::InfoArgs),
    /// Manage source providers (seclists, aspell)
    Source(source::SourceArgs),
}

use std::path::Path;

use anyhow::Result;
use crate::config::{Config, R2Overrides};
use crate::storage::R2Config;

#[derive(Args)]
pub struct R2Args {
    /// Upload to R2/S3 storage instead of local file
    #[arg(long = "r2")]
    pub enabled: bool,

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

impl R2Args {
    pub fn build_config(&self, fallback_path: &Path) -> Result<R2Config> {
        let default_path = fallback_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "hashes.parquet".to_string());

        let overrides = R2Overrides {
            endpoint: self.endpoint.as_deref(),
            bucket: self.bucket.as_deref(),
            access_key_id: self.access_key_id.as_deref(),
            secret_access_key: self.secret_access_key.as_deref(),
            path: self.r2_path.as_deref(),
            region: &self.region,
            default_path: &default_path,
        };

        Config::load()?.build_r2_config(overrides)
    }
}
