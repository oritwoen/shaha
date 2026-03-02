use std::io::{self, BufRead, IsTerminal};

use anyhow::{bail, Result};
use clap::{Args, ValueEnum};

use crate::hasher;

#[derive(Args)]
pub struct HashArgs {
    /// Text to hash (reads from stdin if omitted)
    pub input: Option<String>,

    /// Hash algorithms to use
    #[arg(short, long, default_value = "sha256", value_parser = hasher::algo_value_parser())]
    pub algo: Vec<String>,

    /// Output format
    #[arg(short, long, default_value = "plain")]
    pub format: OutputFormat,
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Plain,
    Json,
}

pub fn run(args: HashArgs) -> Result<()> {
    let hashers: Vec<Box<dyn hasher::Hasher>> = args
        .algo
        .iter()
        .map(|name| hasher::get_hasher(name).expect("algorithm validated by clap"))
        .collect();

    let inputs = collect_inputs(&args)?;

    if inputs.is_empty() {
        bail!("No input provided. Pass text as argument or pipe via stdin.");
    }

    match args.format {
        OutputFormat::Plain => print_plain(&inputs, &hashers),
        OutputFormat::Json => print_json(&inputs, &hashers)?,
    }

    Ok(())
}

fn collect_inputs(args: &HashArgs) -> Result<Vec<String>> {
    if let Some(ref text) = args.input {
        return Ok(vec![text.clone()]);
    }

    if io::stdin().is_terminal() {
        return Ok(vec![]);
    }

    let reader = io::stdin().lock();
    let lines: Vec<String> = reader
        .lines()
        .map_while(Result::ok)
        .filter(|line| !line.is_empty())
        .collect();

    Ok(lines)
}

fn print_plain(inputs: &[String], hashers: &[Box<dyn hasher::Hasher>]) {
    let single_algo = hashers.len() == 1;
    let single_input = inputs.len() == 1;

    for input in inputs {
        for hasher in hashers {
            let hash = hex::encode(hasher.hash(input.as_bytes()));

            if single_input && single_algo {
                println!("{hash}");
            } else if single_algo {
                println!("{hash}  {input}");
            } else if single_input {
                println!("{hash}  {}", hasher.name());
            } else {
                println!("{hash}  {}  {input}", hasher.name());
            }
        }
    }
}

fn print_json(inputs: &[String], hashers: &[Box<dyn hasher::Hasher>]) -> Result<()> {
    #[derive(serde::Serialize)]
    struct JsonHash {
        input: String,
        algorithm: String,
        hash: String,
    }

    let results: Vec<JsonHash> = inputs
        .iter()
        .flat_map(|input| {
            hashers.iter().map(move |hasher| JsonHash {
                input: input.clone(),
                algorithm: hasher.name().to_string(),
                hash: hex::encode(hasher.hash(input.as_bytes())),
            })
        })
        .collect();

    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}
