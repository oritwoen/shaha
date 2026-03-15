use std::io::{self, BufRead, BufReader};

use anyhow::Result;

use super::Source;

pub struct StdinSource;

impl StdinSource {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StdinSource {
    fn default() -> Self {
        Self::new()
    }
}

impl Source for StdinSource {
    fn name(&self) -> &str {
        "stdin"
    }

    fn words(&self) -> Result<Box<dyn Iterator<Item = String>>> {
        let reader = BufReader::new(io::stdin());
        let lines: Vec<String> = reader
            .lines()
            .collect::<io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|line| !line.is_empty())
            .collect();
        Ok(Box::new(lines.into_iter()))
    }

    fn content_hash(&self) -> Result<Option<String>> {
        Ok(None)
    }
}
