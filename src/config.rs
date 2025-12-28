use std::path::PathBuf;

use anyhow::Result;
use serde::Deserialize;

use crate::storage::R2Config;

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub defaults: DefaultsSection,
}

#[derive(Debug, Default, Deserialize)]
pub struct StorageSection {
    #[serde(default)]
    pub r2: R2Section,
}

#[derive(Debug, Default, Deserialize)]
pub struct R2Section {
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DefaultsSection {
    pub algorithms: Option<Vec<String>>,
    pub output: Option<String>,
}

#[derive(Default)]
pub struct R2Overrides<'a> {
    pub endpoint: Option<&'a str>,
    pub bucket: Option<&'a str>,
    pub access_key_id: Option<&'a str>,
    pub secret_access_key: Option<&'a str>,
    pub path: Option<&'a str>,
    pub region: &'a str,
    pub default_path: &'a str,
}

impl<'a> R2Overrides<'a> {
    pub fn new(region: &'a str, default_path: &'a str) -> Self {
        Self {
            region,
            default_path,
            ..Default::default()
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let paths = config_paths();
        
        for path in paths {
            if path.exists() {
                let content = std::fs::read_to_string(&path)?;
                let config: Config = toml::from_str(&content)?;
                return Ok(config);
            }
        }
        
        Ok(Config::default())
    }

    pub fn to_r2_config(&self) -> Option<R2Config> {
        let r2 = &self.storage.r2;
        
        let endpoint = r2.endpoint.clone()
            .or_else(|| std::env::var("SHAHA_R2_ENDPOINT").ok())?;
        let bucket = r2.bucket.clone()
            .or_else(|| std::env::var("SHAHA_R2_BUCKET").ok())?;
        let access_key_id = r2.access_key_id.clone()
            .or_else(|| std::env::var("SHAHA_R2_ACCESS_KEY_ID").ok())
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())?;
        let secret_access_key = r2.secret_access_key.clone()
            .or_else(|| std::env::var("SHAHA_R2_SECRET_ACCESS_KEY").ok())
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())?;
        let path = r2.path.clone().unwrap_or_else(|| "hashes.parquet".to_string());
        
        let mut config = R2Config::new(endpoint, access_key_id, secret_access_key, bucket, path);
        if let Some(ref region) = r2.region {
            config.region = region.clone();
        }
        
        Some(config)
    }

    pub fn build_r2_config(&self, overrides: R2Overrides) -> Result<R2Config> {
        let r2 = &self.storage.r2;

        let endpoint = overrides.endpoint.map(String::from)
            .or_else(|| std::env::var("SHAHA_R2_ENDPOINT").ok())
            .or_else(|| r2.endpoint.clone())
            .ok_or_else(|| anyhow::anyhow!(
                "R2 endpoint required: use --endpoint, SHAHA_R2_ENDPOINT env var, or config file"
            ))?;

        let bucket = overrides.bucket.map(String::from)
            .or_else(|| std::env::var("SHAHA_R2_BUCKET").ok())
            .or_else(|| r2.bucket.clone())
            .ok_or_else(|| anyhow::anyhow!(
                "R2 bucket required: use --bucket, SHAHA_R2_BUCKET env var, or config file"
            ))?;

        let access_key_id = overrides.access_key_id.map(String::from)
            .or_else(|| std::env::var("SHAHA_R2_ACCESS_KEY_ID").ok())
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
            .or_else(|| r2.access_key_id.clone())
            .ok_or_else(|| anyhow::anyhow!(
                "R2 access key required: use --access-key-id, env var, or config file"
            ))?;

        let secret_access_key = overrides.secret_access_key.map(String::from)
            .or_else(|| std::env::var("SHAHA_R2_SECRET_ACCESS_KEY").ok())
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
            .or_else(|| r2.secret_access_key.clone())
            .ok_or_else(|| anyhow::anyhow!(
                "R2 secret key required: use --secret-access-key, env var, or config file"
            ))?;

        let path = overrides.path.map(String::from)
            .or_else(|| r2.path.clone())
            .unwrap_or_else(|| overrides.default_path.to_string());

        let region = if overrides.region != "auto" {
            overrides.region.to_string()
        } else {
            r2.region.clone().unwrap_or_else(|| "auto".to_string())
        };

        let mut config = R2Config::new(endpoint, access_key_id, secret_access_key, bucket, path);
        config.region = region;

        Ok(config)
    }
}

fn config_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    
    if let Ok(cwd) = std::env::current_dir() {
        paths.push(cwd.join(".shaha.toml"));
    }
    
    if let Some(config_dir) = dirs::config_dir() {
        paths.push(config_dir.join("shaha").join("config.toml"));
    }
    
    paths
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_config() {
        let config: Config = toml::from_str("").unwrap();
        assert!(config.storage.r2.endpoint.is_none());
        assert!(config.defaults.algorithms.is_none());
    }

    #[test]
    fn test_parse_r2_config() {
        let toml = r#"
[storage.r2]
endpoint = "https://example.r2.cloudflarestorage.com"
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
region = "auto"
path = "hashes.parquet"

[defaults]
algorithms = ["sha256", "md5"]
output = "custom.parquet"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        
        assert_eq!(config.storage.r2.endpoint, Some("https://example.r2.cloudflarestorage.com".to_string()));
        assert_eq!(config.storage.r2.bucket, Some("my-bucket".to_string()));
        assert_eq!(config.defaults.algorithms, Some(vec!["sha256".to_string(), "md5".to_string()]));
    }

    #[test]
    fn test_to_r2_config_complete() {
        let toml = r#"
[storage.r2]
endpoint = "https://example.r2.cloudflarestorage.com"
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        let r2_config = config.to_r2_config().unwrap();
        
        assert_eq!(r2_config.s3_url(), "s3://my-bucket/hashes.parquet");
    }

    #[test]
    fn test_to_r2_config_incomplete() {
        let toml = r#"
[storage.r2]
endpoint = "https://example.r2.cloudflarestorage.com"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.to_r2_config().is_none());
    }
}
