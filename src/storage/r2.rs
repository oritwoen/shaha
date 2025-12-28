use anyhow::{Context, Result};
use duckdb::{params, Connection};

use super::{HashRecord, Stats, Storage};

/// Configuration for R2/S3 storage
#[derive(Debug, Clone)]
pub struct R2Config {
    /// S3/R2 endpoint URL (e.g., "https://account-id.r2.cloudflarestorage.com")
    pub endpoint: String,
    /// Access key ID
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Bucket name
    pub bucket: String,
    /// Path within bucket (e.g., "hashes/db.parquet")
    pub path: String,
    /// Region (default: "auto" for R2)
    pub region: String,
}

impl R2Config {
    pub fn new(
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        bucket: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            bucket: bucket.into(),
            path: path.into(),
            region: "auto".to_string(),
        }
    }

    /// Build S3 URL for the parquet file
    pub fn s3_url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.path)
    }

    /// Load config from environment variables
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            endpoint: std::env::var("SHAHA_R2_ENDPOINT")
                .context("SHAHA_R2_ENDPOINT not set")?,
            access_key_id: std::env::var("SHAHA_R2_ACCESS_KEY_ID")
                .or_else(|_| std::env::var("AWS_ACCESS_KEY_ID"))
                .context("SHAHA_R2_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID not set")?,
            secret_access_key: std::env::var("SHAHA_R2_SECRET_ACCESS_KEY")
                .or_else(|_| std::env::var("AWS_SECRET_ACCESS_KEY"))
                .context("SHAHA_R2_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY not set")?,
            bucket: std::env::var("SHAHA_R2_BUCKET")
                .context("SHAHA_R2_BUCKET not set")?,
            path: std::env::var("SHAHA_R2_PATH")
                .unwrap_or_else(|_| "hashes.parquet".to_string()),
            region: std::env::var("SHAHA_R2_REGION")
                .unwrap_or_else(|_| "auto".to_string()),
        })
    }
}

pub struct R2Storage {
    conn: Connection,
    config: R2Config,
    pending_records: Vec<HashRecord>,
}

impl R2Storage {
    pub fn new(config: R2Config) -> Result<Self> {
        let conn = Connection::open_in_memory()
            .context("Failed to open DuckDB in-memory database")?;

        // Install and load httpfs extension for S3 support
        conn.execute_batch(
            "INSTALL httpfs;
             LOAD httpfs;"
        ).context("Failed to install/load httpfs extension")?;

        // Configure S3/R2 credentials
        conn.execute_batch(&format!(
            "SET s3_endpoint = '{}';
             SET s3_access_key_id = '{}';
             SET s3_secret_access_key = '{}';
             SET s3_region = '{}';
             SET s3_url_style = 'path';",
            config.endpoint.trim_start_matches("https://").trim_start_matches("http://"),
            config.access_key_id,
            config.secret_access_key,
            config.region,
        )).context("Failed to configure S3 credentials")?;

        conn.execute_batch(
            "CREATE TABLE pending_records (
                hash BLOB NOT NULL,
                preimage VARCHAR NOT NULL,
                algorithm VARCHAR NOT NULL,
                sources VARCHAR[] NOT NULL
            );"
        ).context("Failed to create pending_records table")?;

        Ok(Self {
            conn,
            config,
            pending_records: Vec::new(),
        })
    }

    fn insert_pending_to_table(&mut self) -> Result<()> {
        if self.pending_records.is_empty() {
            return Ok(());
        }

        for record in self.pending_records.drain(..) {
            let sources_literal = Self::sources_to_array_literal(&record.sources);
            let query = format!(
                "INSERT INTO pending_records (hash, preimage, algorithm, sources) VALUES (?, ?, ?, {})",
                sources_literal
            );
            self.conn.execute(&query, params![
                record.hash.as_slice(),
                record.preimage.as_str(),
                record.algorithm.as_str(),
            ])?;
        }

        Ok(())
    }

    fn sources_to_array_literal(sources: &[String]) -> String {
        if sources.is_empty() {
            return "[]::VARCHAR[]".to_string();
        }
        let escaped: Vec<String> = sources
            .iter()
            .map(|s| format!("'{}'", s.replace('\'', "''")))
            .collect();
        format!("[{}]", escaped.join(", "))
    }

    fn row_to_record(row: &duckdb::Row<'_>) -> std::result::Result<HashRecord, duckdb::Error> {
        let hash: Vec<u8> = row.get(0)?;
        let preimage: String = row.get(1)?;
        let algorithm: String = row.get(2)?;
        let sources_json: String = row.get(3)?;
        let sources: Vec<String> = serde_json::from_str(&sources_json).unwrap_or_default();
        Ok(HashRecord {
            hash,
            preimage,
            algorithm,
            sources,
        })
    }
}

impl Storage for R2Storage {
    fn write_batch(&mut self, records: Vec<HashRecord>) -> Result<()> {
        self.pending_records.extend(records);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.pending_records.is_empty() {
            return Ok(());
        }

        self.insert_pending_to_table()?;

        let s3_url = self.config.s3_url();

        // Write to S3/R2 as parquet with ZSTD compression
        self.conn.execute_batch(&format!(
            "COPY pending_records TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD);",
            s3_url
        )).with_context(|| format!("Failed to write parquet to {}", s3_url))?;

        // Clear the temp table
        self.conn.execute_batch("DELETE FROM pending_records;")?;

        Ok(())
    }

    fn query(&self, hash_prefix: &[u8], algo: Option<&str>, limit: Option<usize>) -> Result<Vec<HashRecord>> {
        let s3_url = self.config.s3_url();

        let mut conditions = Vec::new();
        let mut param_values: Vec<String> = Vec::new();
        
        if !hash_prefix.is_empty() {
            let hex_prefix = hex::encode(hash_prefix);
            conditions.push("starts_with(encode(hash)::VARCHAR, ?)".to_string());
            param_values.push(hex_prefix);
        }

        if let Some(algorithm) = algo {
            conditions.push("algorithm = ?".to_string());
            param_values.push(algorithm.to_string());
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let limit_clause = limit
            .map(|l| format!(" LIMIT {}", l))
            .unwrap_or_default();

        let query = format!(
            "SELECT hash, preimage, algorithm, to_json(sources)::VARCHAR FROM read_parquet('{}'){}{};",
            s3_url, where_clause, limit_clause
        );

        let mut stmt = self.conn.prepare(&query)
            .with_context(|| format!("Failed to query parquet at {}", s3_url))?;

        let records: Result<Vec<HashRecord>> = match param_values.len() {
            0 => stmt.query_map([], Self::row_to_record)?,
            1 => stmt.query_map([&param_values[0]], Self::row_to_record)?,
            2 => stmt.query_map([&param_values[0], &param_values[1]], Self::row_to_record)?,
            _ => unreachable!(),
        }
        .map(|r| r.map_err(|e| anyhow::anyhow!("{}", e)))
        .collect();

        records
    }

    fn stats(&self) -> Result<Stats> {
        let s3_url = self.config.s3_url();

        let stats_query = format!(
            "WITH data AS (SELECT algorithm, sources FROM read_parquet('{}'))
             SELECT 
                 (SELECT COUNT(*) FROM data) as total,
                 (SELECT string_agg(DISTINCT algorithm, ',') FROM data) as algorithms,
                 (SELECT string_agg(DISTINCT s, ',') FROM data, unnest(sources) as t(s)) as sources",
            s3_url
        );

        let result = self.conn.query_row(&stats_query, [], |row| {
            let total: usize = row.get(0)?;
            let algos: Option<String> = row.get(1)?;
            let srcs: Option<String> = row.get(2)?;
            Ok((total, algos, srcs))
        });

        match result {
            Ok((total_records, algos_str, sources_str)) => {
                let algorithms = algos_str
                    .map(|s| s.split(',').map(String::from).collect())
                    .unwrap_or_default();
                let sources = sources_str
                    .map(|s| s.split(',').map(String::from).collect())
                    .unwrap_or_default();
                
                Ok(Stats {
                    total_records,
                    algorithms,
                    sources,
                    file_size_bytes: 0,
                })
            }
            Err(_) => Ok(Stats::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_r2_config_s3_url() {
        let config = R2Config::new(
            "https://account.r2.cloudflarestorage.com",
            "key",
            "secret",
            "my-bucket",
            "path/to/hashes.parquet",
        );
        assert_eq!(config.s3_url(), "s3://my-bucket/path/to/hashes.parquet");
    }

    #[test]
    fn test_r2_config_from_env_missing() {
        unsafe { std::env::remove_var("SHAHA_R2_ENDPOINT") };
        let result = R2Config::from_env();
        assert!(result.is_err());
    }

    #[test]
    fn test_sources_to_array_literal() {
        let sources = vec!["rockyou".to_string(), "common".to_string()];
        assert_eq!(R2Storage::sources_to_array_literal(&sources), "['rockyou', 'common']");
        
        let empty: Vec<String> = vec![];
        assert_eq!(R2Storage::sources_to_array_literal(&empty), "[]::VARCHAR[]");

        let with_quote = vec!["it's".to_string()];
        assert_eq!(R2Storage::sources_to_array_literal(&with_quote), "['it''s']");
    }
}
