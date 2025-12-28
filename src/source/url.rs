use std::io::{BufRead, BufReader, Cursor};
use std::sync::OnceLock;

use anyhow::{Context, Result};

use super::Source;

pub struct UrlSource {
    name: String,
    cached_content: OnceLock<String>,
}

impl UrlSource {
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let name = url
            .rsplit('/')
            .next()
            .and_then(|s| s.split('.').next())
            .unwrap_or("url")
            .to_string();

        let response = reqwest::blocking::get(&url)
            .with_context(|| format!("Failed to fetch URL: {}", url))?;
        let content = response
            .text()
            .with_context(|| format!("Failed to read response from: {}", url))?;

        let source = Self {
            name,
            cached_content: OnceLock::new(),
        };
        let _ = source.cached_content.set(content);

        Ok(source)
    }

    fn get_content(&self) -> &str {
        self.cached_content.get().expect("content initialized in new()")
    }
}

impl Source for UrlSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn words(&self) -> Result<Box<dyn Iterator<Item = String>>> {
        let content = self.get_content();
        let reader = BufReader::new(Cursor::new(content));
        let lines: Vec<String> = reader
            .lines()
            .map_while(Result::ok)
            .filter(|line| !line.is_empty())
            .collect();

        Ok(Box::new(lines.into_iter()))
    }

    fn content_hash(&self) -> Result<Option<String>> {
        let content = self.get_content();
        let hash = blake3::hash(content.as_bytes());
        Ok(Some(hash.to_hex().to_string()))
    }
}
