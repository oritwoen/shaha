pub mod build;
pub mod info;
pub mod query;
pub mod source;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "shaha")]
#[command(about = "Hash database builder and reverse lookup tool (SHA + aha!)")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Build hash database from input file
    Build(build::BuildArgs),
    /// Query hash database for preimage
    Query(query::QueryArgs),
    /// Show database statistics
    Info(info::InfoArgs),
    /// Manage source providers (seclists, aspell)
    Source(source::SourceArgs),
}
