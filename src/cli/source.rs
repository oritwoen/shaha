use anyhow::{bail, Result};
use clap::{Args, Subcommand};

use crate::source::{aspell, seclists};
use crate::status;

#[derive(Args)]
pub struct SourceArgs {
    #[command(subcommand)]
    pub command: SourceCommands,
}

#[derive(Subcommand)]
pub enum SourceCommands {
    /// Download/update a source provider
    Pull {
        /// Provider name (seclists)
        provider: String,
    },
    /// List available files from a provider
    List {
        /// Provider name (seclists, aspell)
        provider: String,
        /// Optional subpath to filter
        path: Option<String>,
    },
    /// Show cache path for a provider
    Path {
        /// Provider name (seclists)
        provider: String,
    },
}

pub fn run(args: SourceArgs) -> Result<()> {
    match args.command {
        SourceCommands::Pull { provider } => pull(&provider),
        SourceCommands::List { provider, path } => list(&provider, path.as_deref()),
        SourceCommands::Path { provider } => path(&provider),
    }
}

fn pull(provider: &str) -> Result<()> {
    match provider {
        "seclists" => seclists::pull(),
        "aspell" => {
            if aspell::is_available() {
                status!("aspell is installed and ready.");
                Ok(())
            } else {
                bail!("aspell is not installed. Install it with your package manager:\n  apt install aspell aspell-en aspell-pl\n  brew install aspell")
            }
        }
        _ => bail!(
            "Unknown provider: '{}'. Available: seclists, aspell",
            provider
        ),
    }
}

fn list(provider: &str, subpath: Option<&str>) -> Result<()> {
    match provider {
        "seclists" => {
            let files = seclists::list(subpath)?;
            for file in files {
                println!("{}", file);
            }
            Ok(())
        }
        "aspell" => {
            let langs = aspell::list_languages()?;
            for lang in langs {
                println!("{}", lang);
            }
            Ok(())
        }
        _ => bail!(
            "Unknown provider: '{}'. Available: seclists, aspell",
            provider
        ),
    }
}

fn path(provider: &str) -> Result<()> {
    match provider {
        "seclists" => {
            println!("{}", seclists::path().display());
            Ok(())
        }
        "aspell" => {
            status!("aspell uses system dictionaries, no local cache path.");
            Ok(())
        }
        _ => bail!(
            "Unknown provider: '{}'. Available: seclists, aspell",
            provider
        ),
    }
}
