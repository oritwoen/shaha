use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};

use super::Source;

pub struct AspellSource {
    lang: String,
    cached_dump: OnceLock<Vec<u8>>,
}

impl AspellSource {
    pub fn new(lang: &str) -> Result<Self> {
        let available = list_languages()?;
        if !available.contains(&lang.to_string()) {
            bail!(
                "Aspell dictionary '{}' not found. Available: {:?}",
                lang,
                available
            );
        }

        let output = Command::new("aspell")
            .args(["-d", lang, "dump", "master"])
            .output()
            .context("Failed to run aspell. Is it installed?")?;

        if !output.status.success() {
            bail!(
                "aspell failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let source = Self {
            lang: lang.to_string(),
            cached_dump: OnceLock::new(),
        };
        let _ = source.cached_dump.set(output.stdout);
        
        Ok(source)
    }

    fn get_dump(&self) -> &[u8] {
        self.cached_dump.get().expect("dump initialized in new()")
    }
}

impl Source for AspellSource {
    fn name(&self) -> &str {
        &self.lang
    }

    fn words(&self) -> Result<Box<dyn Iterator<Item = String>>> {
        let words: Vec<String> = self.get_dump()
            .lines()
            .map_while(Result::ok)
            .filter(|line| !line.is_empty())
            .collect();

        Ok(Box::new(words.into_iter()))
    }

    fn content_hash(&self) -> Result<Option<String>> {
        let hash = blake3::hash(self.get_dump());
        Ok(Some(hash.to_hex().to_string()))
    }
}

pub fn is_available() -> bool {
    Command::new("aspell")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub fn list_languages() -> Result<Vec<String>> {
    if !is_available() {
        bail!("aspell is not installed. Install it with your package manager.");
    }

    let output = Command::new("aspell")
        .arg("dicts")
        .output()
        .context("Failed to run aspell dicts")?;

    if !output.status.success() {
        bail!(
            "aspell dicts failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let langs: Vec<String> = BufReader::new(&output.stdout[..])
        .lines()
        .map_while(Result::ok)
        .filter(|line| !line.is_empty())
        .collect();

    Ok(langs)
}
