use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::Source;

pub struct FileSource {
    path: PathBuf,
    name: String,
}

impl FileSource {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        Self { path, name }
    }
}

impl Source for FileSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn words(&self) -> Result<Box<dyn Iterator<Item = String>>> {
        let file = File::open(&self.path)
            .with_context(|| format!("Failed to open file: {:?}", self.path))?;
        let reader = BufReader::new(file);
        Ok(Box::new(
            reader
                .lines()
                .map_while(Result::ok)
                .filter(|line| !line.is_empty()),
        ))
    }

    fn content_hash(&self) -> Result<Option<String>> {
        let mut file = File::open(&self.path)
            .with_context(|| format!("Failed to open file: {:?}", self.path))?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0u8; 65536];
        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
        Ok(Some(hasher.finalize().to_hex().to_string()))
    }
}
