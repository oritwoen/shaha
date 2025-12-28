use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

use super::Source;

const SECLISTS_REPO: &str = "https://github.com/danielmiessler/SecLists.git";

pub struct SecListsSource {
    path: String,
    full_path: PathBuf,
}

impl SecListsSource {
    pub fn new(path: &str) -> Result<Self> {
        let base = seclists_dir();
        if !base.exists() {
            bail!(
                "SecLists not found. Run `shaha source pull seclists` first."
            );
        }

        let full_path = base.join(path);
        if !full_path.exists() {
            bail!(
                "File not found: {}. Use `shaha source list seclists` to see available files.",
                path
            );
        }

        Ok(Self {
            path: path.to_string(),
            full_path,
        })
    }
}

impl Source for SecListsSource {
    fn name(&self) -> &str {
        &self.path
    }

    fn words(&self) -> Result<Box<dyn Iterator<Item = String>>> {
        let file = File::open(&self.full_path)
            .with_context(|| format!("Failed to open: {:?}", self.full_path))?;
        let reader = BufReader::new(file);
        Ok(Box::new(
            reader
                .lines()
                .map_while(Result::ok)
                .filter(|line| !line.is_empty()),
        ))
    }

    fn content_hash(&self) -> Result<Option<String>> {
        let mut file = File::open(&self.full_path)
            .with_context(|| format!("Failed to open: {:?}", self.full_path))?;
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

fn seclists_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("shaha")
        .join("seclists")
}

pub fn is_pulled() -> bool {
    seclists_dir().join(".git").exists()
}

pub fn pull() -> Result<()> {
    let dir = seclists_dir();

    if dir.join(".git").exists() {
        eprintln!("Updating SecLists...");
        let status = Command::new("git")
            .args(["pull", "--ff-only"])
            .current_dir(&dir)
            .status()
            .context("Failed to run git pull")?;

        if !status.success() {
            bail!("git pull failed");
        }
        eprintln!("SecLists updated.");
    } else {
        if let Some(parent) = dir.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        eprintln!("Cloning SecLists (this may take a while)...");
        let status = Command::new("git")
            .args(["clone", "--depth", "1", SECLISTS_REPO, dir.to_str().unwrap()])
            .status()
            .context("Failed to run git clone")?;

        if !status.success() {
            bail!("git clone failed");
        }
        eprintln!("SecLists cloned to {:?}", dir);
    }

    Ok(())
}

pub fn list(subpath: Option<&str>) -> Result<Vec<String>> {
    let base = seclists_dir();
    if !base.exists() {
        bail!("SecLists not found. Run `shaha source pull seclists` first.");
    }

    let search_dir = match subpath {
        Some(p) => base.join(p),
        None => base,
    };

    if !search_dir.exists() {
        bail!("Path not found: {:?}", search_dir);
    }

    let mut files = Vec::new();
    collect_txt_files(&search_dir, &seclists_dir(), &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_txt_files(dir: &Path, base: &Path, files: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            let name = path.file_name().unwrap().to_string_lossy();
            if name.starts_with('.') {
                continue;
            }
            collect_txt_files(&path, base, files)?;
        } else if let Some(ext) = path.extension() {
            if ext == "txt" {
                let relative = path.strip_prefix(base)
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                files.push(relative);
            }
        }
    }
    Ok(())
}

pub fn path() -> PathBuf {
    seclists_dir()
}
