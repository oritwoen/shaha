mod parquet;
mod r2;

pub use self::parquet::ParquetStorage;
pub use self::r2::{R2Config, R2Storage};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashRecord {
    pub hash: Vec<u8>,
    pub preimage: String,
    pub algorithm: String,
    pub sources: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Stats {
    pub total_records: usize,
    pub algorithms: Vec<String>,
    pub sources: Vec<String>,
    pub file_size_bytes: u64,
}

pub trait Storage {
    fn write_batch(&mut self, records: Vec<HashRecord>) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn query(&self, hash_prefix: &[u8], algo: Option<&str>, limit: Option<usize>) -> Result<Vec<HashRecord>>;
    fn stats(&self) -> Result<Stats>;
}
