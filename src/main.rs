use anyhow::Result;
use clap::Parser;

use shaha::cli::{Cli, Commands};

fn main() -> Result<()> {
    let cli = Cli::parse();
    shaha::output::set_quiet(cli.quiet);

    match cli.command {
        Commands::Build(args) => shaha::cli::build::run(args),
        Commands::Query(args) => shaha::cli::query::run(args),
        Commands::Info(args) => shaha::cli::info::run(args),
        Commands::Source(args) => shaha::cli::source::run(args),
    }
}
