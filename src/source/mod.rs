mod file;
mod stdin;
mod url;
pub mod aspell;
pub mod seclists;

pub use aspell::AspellSource;
pub use file::FileSource;
pub use seclists::SecListsSource;
pub use stdin::StdinSource;
pub use url::UrlSource;

use anyhow::{bail, Result};

pub trait Source {
    fn name(&self) -> &str;
    fn words(&self) -> Result<Box<dyn Iterator<Item = String>>>;
    fn content_hash(&self) -> Result<Option<String>>;
}

pub fn parse(spec: &str) -> Result<Box<dyn Source>> {
    if spec == "-" {
        return Ok(Box::new(StdinSource::new()));
    }

    if spec.starts_with("http://") || spec.starts_with("https://") {
        return Ok(Box::new(UrlSource::new(spec)?));
    }

    if let Some((provider, path)) = spec.split_once(':') {
        match provider {
            "seclists" => Ok(Box::new(SecListsSource::new(path)?)),
            "aspell" => Ok(Box::new(AspellSource::new(path)?)),
            "file" => Ok(Box::new(FileSource::new(path))),
            _ => bail!(
                "Unknown source provider: '{}'. Available: seclists, aspell, file",
                provider
            ),
        }
    } else {
        Ok(Box::new(FileSource::new(spec)))
    }
}
