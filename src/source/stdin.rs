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
        Ok(Box::new(
            reader
                .lines()
                .map_while(Result::ok)
                .filter(|line| !line.is_empty()),
        ))
    }

    fn content_hash(&self) -> Result<Option<String>> {
        Ok(None)
    }
}
