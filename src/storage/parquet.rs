use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, ArrayRef, BinaryArray, ListArray, RecordBatch, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bloomfilter::Bloom;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;

use super::{HashRecord, Stats, Storage};

const META_TOTAL_RECORDS: &str = "shaha:total_records";
const META_ALGORITHMS: &str = "shaha:algorithms";
const META_SOURCES: &str = "shaha:sources";
const META_SOURCE_HASHES: &str = "shaha:source_hashes";
const META_BLOOM_BITMAP: &str = "shaha:bloom_bitmap";
const META_BLOOM_KEYS: &str = "shaha:bloom_keys";
const META_BLOOM_ITEMS: &str = "shaha:bloom_items";

const DEFAULT_BLOOM_CAPACITY: usize = 1_000_000;
const BLOOM_FP_RATE: f64 = 0.01;

pub struct ParquetStorage {
    path: PathBuf,
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    write_stats: WriteStats,
}

struct WriteStats {
    total_records: usize,
    algorithms: HashSet<String>,
    sources: HashSet<String>,
    source_hashes: HashSet<String>,
    bloom: Bloom<Vec<u8>>,
}

impl WriteStats {
    fn with_capacity(expected_records: usize) -> Self {
        let bloom_capacity = expected_records.max(DEFAULT_BLOOM_CAPACITY);
        Self {
            total_records: 0,
            algorithms: HashSet::new(),
            sources: HashSet::new(),
            source_hashes: HashSet::new(),
            bloom: Bloom::new_for_fp_rate(bloom_capacity, BLOOM_FP_RATE),
        }
    }
}

impl Default for WriteStats {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_BLOOM_CAPACITY)
    }
}

impl ParquetStorage {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self::with_expected_capacity(path, DEFAULT_BLOOM_CAPACITY)
    }

    pub fn with_expected_capacity(path: impl AsRef<Path>, expected_records: usize) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            writer: None,
            schema: Arc::new(Schema::new(vec![
                Field::new("hash", DataType::Binary, false),
                Field::new("preimage", DataType::Utf8, false),
                Field::new("algorithm", DataType::Utf8, false),
                Field::new(
                    "sources",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                    false,
                ),
            ])),
            write_stats: WriteStats::with_capacity(expected_records),
        }
    }

    fn ensure_writer(&mut self) -> Result<&mut ArrowWriter<File>> {
        if self.writer.is_none() {
            let file = File::create(&self.path)
                .with_context(|| format!("Failed to create file: {:?}", self.path))?;

            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .build();

            self.writer = Some(ArrowWriter::try_new(file, self.schema.clone(), Some(props))?);
        }
        Ok(self.writer.as_mut().expect("writer initialized above"))
    }

    fn build_sources_array(records: &[HashRecord]) -> ArrayRef {
        let mut all_sources: Vec<&str> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];

        for record in records {
            for source in &record.sources {
                all_sources.push(source.as_str());
            }
            offsets.push(all_sources.len() as i32);
        }

        let values = StringArray::from(all_sources);
        let offsets = OffsetBuffer::new(offsets.into());

        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            offsets,
            Arc::new(values),
            None,
        ))
    }

    fn extract_sources(list_array: &ListArray, index: usize) -> Vec<String> {
        let start = list_array.value_offsets()[index] as usize;
        let end = list_array.value_offsets()[index + 1] as usize;

        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        (start..end)
            .map(|i| values.value(i).to_string())
            .collect()
    }

    fn collect_stats(&mut self, records: &[HashRecord]) {
        self.write_stats.total_records += records.len();
        for record in records {
            self.write_stats.bloom.set(&record.hash);
            self.write_stats
                .algorithms
                .insert(record.algorithm.clone());
            for source in &record.sources {
                self.write_stats.sources.insert(source.clone());
            }
        }
    }

    fn read_stats_from_metadata(&self) -> Result<Option<Stats>> {
        let file = File::open(&self.path)
            .with_context(|| format!("Failed to open database: {:?}", self.path))?;
        let file_size = file.metadata()?.len();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata().file_metadata().key_value_metadata();

        let Some(metadata) = metadata else {
            return Ok(None);
        };

        let mut total_records = None;
        let mut algorithms = None;
        let mut sources = None;

        for kv in metadata {
            match kv.key.as_str() {
                META_TOTAL_RECORDS => {
                    total_records = kv.value.as_ref().and_then(|v| v.parse().ok());
                }
                META_ALGORITHMS => {
                    algorithms = kv.value.as_ref().map(|v| {
                        v.split(',')
                            .filter(|s| !s.is_empty())
                            .map(String::from)
                            .collect()
                    });
                }
                META_SOURCES => {
                    sources = kv.value.as_ref().map(|v| {
                        v.split(',')
                            .filter(|s| !s.is_empty())
                            .map(String::from)
                            .collect()
                    });
                }
                _ => {}
            }
        }

        match (total_records, algorithms, sources) {
            (Some(total_records), Some(algorithms), Some(sources)) => Ok(Some(Stats {
                total_records,
                algorithms,
                sources,
                file_size_bytes: file_size,
            })),
            _ => Ok(None),
        }
    }

    fn load_bloom_filter(&self) -> Result<Option<Bloom<Vec<u8>>>> {
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata().file_metadata().key_value_metadata();

        let Some(metadata) = metadata else {
            return Ok(None);
        };

        let mut bitmap: Option<Vec<u8>> = None;
        let mut keys: Option<[(u64, u64); 2]> = None;
        let mut items_count: Option<u32> = None;

        for kv in metadata {
            match kv.key.as_str() {
                META_BLOOM_BITMAP => {
                    if let Some(ref encoded) = kv.value {
                        bitmap = Some(BASE64.decode(encoded)?);
                    }
                }
                META_BLOOM_KEYS => {
                    if let Some(ref keys_str) = kv.value {
                        let parts: Vec<u64> = keys_str
                            .split(',')
                            .filter_map(|s| s.parse().ok())
                            .collect();
                        if parts.len() == 4 {
                            keys = Some([(parts[0], parts[1]), (parts[2], parts[3])]);
                        }
                    }
                }
                META_BLOOM_ITEMS => {
                    if let Some(ref count_str) = kv.value {
                        items_count = count_str.parse().ok();
                    }
                }
                _ => {}
            }
        }

        match (bitmap, keys, items_count) {
            (Some(bytes), Some(sip_keys), Some(count)) => {
                let bloom = Bloom::from_existing(
                    &bytes,
                    (bytes.len() * 8) as u64,
                    count,
                    sip_keys,
                );
                Ok(Some(bloom))
            }
            _ => Ok(None),
        }
    }

    fn is_full_hash_length(len: usize) -> bool {
        matches!(len, 16 | 20 | 32 | 64)
    }

    fn prefix_might_be_in_range(prefix: &[u8], min: &[u8], max: &[u8]) -> bool {
        if prefix.is_empty() {
            return true;
        }

        let prefix_low = prefix;
        let mut prefix_high: Vec<u8> = prefix.to_vec();
        prefix_high.resize(max.len().max(prefix.len()), 0xFF);

        max >= prefix_low && min <= prefix_high.as_slice()
    }

    pub fn add_source_hash(&mut self, hash: &str) {
        self.write_stats.source_hashes.insert(hash.to_string());
    }

    pub fn for_each_record<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(HashRecord) -> Result<()>,
    {
        if !self.path.exists() {
            return Ok(());
        }

        let file = File::open(&self.path)
            .with_context(|| format!("Failed to open database: {:?}", self.path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch_result in reader {
            let batch = batch_result?;

            let hashes = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected binary hash column"))?;
            let preimages = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected string preimage column"))?;
            let algorithms = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected string algorithm column"))?;
            let sources = batch
                .column(3)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected list sources column"))?;

            for i in 0..batch.num_rows() {
                let record = HashRecord {
                    hash: hashes.value(i).to_vec(),
                    preimage: preimages.value(i).to_string(),
                    algorithm: algorithms.value(i).to_string(),
                    sources: Self::extract_sources(sources, i),
                };
                callback(record)?;
            }
        }

        Ok(())
    }

    pub fn get_source_hashes(&self) -> Result<HashSet<String>> {
        if !self.path.exists() {
            return Ok(HashSet::new());
        }

        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata().file_metadata().key_value_metadata();

        let Some(metadata) = metadata else {
            return Ok(HashSet::new());
        };

        for kv in metadata {
            if kv.key == META_SOURCE_HASHES {
                if let Some(ref json) = kv.value {
                    return Ok(serde_json::from_str(json).unwrap_or_default());
                }
            }
        }

        Ok(HashSet::new())
    }

    fn scan_stats(&self) -> Result<Stats> {
        let file = File::open(&self.path)
            .with_context(|| format!("Failed to open database: {:?}", self.path))?;
        let file_size = file.metadata()?.len();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut total_records = 0;
        let mut algorithms = HashSet::new();
        let mut sources = HashSet::new();

        for batch_result in reader {
            let batch = batch_result?;
            total_records += batch.num_rows();

            let algo_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected string algorithm column"))?;
            let sources_array = batch
                .column(3)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected list sources column"))?;

            for i in 0..batch.num_rows() {
                algorithms.insert(algo_array.value(i).to_string());
                for source in Self::extract_sources(sources_array, i) {
                    sources.insert(source);
                }
            }
        }

        Ok(Stats {
            total_records,
            algorithms: algorithms.into_iter().collect(),
            sources: sources.into_iter().collect(),
            file_size_bytes: file_size,
        })
    }
}

impl Storage for ParquetStorage {
    fn write_batch(&mut self, records: Vec<HashRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        self.collect_stats(&records);

        let hashes: Vec<&[u8]> = records.iter().map(|r| r.hash.as_slice()).collect();
        let preimages: Vec<&str> = records.iter().map(|r| r.preimage.as_str()).collect();
        let algorithms: Vec<&str> = records.iter().map(|r| r.algorithm.as_str()).collect();
        let sources_array = Self::build_sources_array(&records);

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(BinaryArray::from(hashes)),
                Arc::new(StringArray::from(preimages)),
                Arc::new(StringArray::from(algorithms)),
                sources_array,
            ],
        )?;

        let writer = self.ensure_writer()?;
        writer.write(&batch)?;

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            let algorithms: Vec<_> = self.write_stats.algorithms.iter().collect();
            let sources: Vec<_> = self.write_stats.sources.iter().collect();

            writer.append_key_value_metadata(parquet::format::KeyValue {
                key: META_TOTAL_RECORDS.to_string(),
                value: Some(self.write_stats.total_records.to_string()),
            });
            writer.append_key_value_metadata(parquet::format::KeyValue {
                key: META_ALGORITHMS.to_string(),
                value: Some(algorithms.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")),
            });
            writer.append_key_value_metadata(parquet::format::KeyValue {
                key: META_SOURCES.to_string(),
                value: Some(sources.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")),
            });

            let bloom_bitmap = BASE64.encode(self.write_stats.bloom.bitmap());
            let bloom_keys = self.write_stats.bloom.sip_keys();
            let bloom_keys_str = format!(
                "{},{},{},{}",
                bloom_keys[0].0, bloom_keys[0].1, bloom_keys[1].0, bloom_keys[1].1
            );
            writer.append_key_value_metadata(parquet::format::KeyValue {
                key: META_BLOOM_BITMAP.to_string(),
                value: Some(bloom_bitmap),
            });
            writer.append_key_value_metadata(parquet::format::KeyValue {
                key: META_BLOOM_KEYS.to_string(),
                value: Some(bloom_keys_str),
            });
            writer.append_key_value_metadata(parquet::format::KeyValue {
                key: META_BLOOM_ITEMS.to_string(),
                value: Some(self.write_stats.total_records.to_string()),
            });

            if !self.write_stats.source_hashes.is_empty() {
                let source_hashes_json = serde_json::to_string(&self.write_stats.source_hashes)?;
                writer.append_key_value_metadata(parquet::format::KeyValue {
                    key: META_SOURCE_HASHES.to_string(),
                    value: Some(source_hashes_json),
                });
            }

            writer.close()?;
        }
        Ok(())
    }

    fn query(&self, hash_prefix: &[u8], algo: Option<&str>, limit: Option<usize>) -> Result<Vec<HashRecord>> {
        if !self.path.exists() {
            return Ok(vec![]);
        }

        if Self::is_full_hash_length(hash_prefix.len()) {
            if let Ok(Some(bloom)) = self.load_bloom_filter() {
                if !bloom.check(&hash_prefix.to_vec()) {
                    return Ok(vec![]);
                }
            }
        }

        let file = File::open(&self.path)
            .with_context(|| format!("Failed to open database: {:?}", self.path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        
        let metadata = builder.metadata().clone();
        let mut matching_row_groups = Vec::new();
        
        for (i, rg) in metadata.row_groups().iter().enumerate() {
            let dominated_by_statistics = rg.column(0).statistics().and_then(|stats| {
                if let Statistics::ByteArray(byte_stats) = stats {
                    let min = byte_stats.min_opt()?;
                    let max = byte_stats.max_opt()?;
                    Some(Self::prefix_might_be_in_range(hash_prefix, min.data(), max.data()))
                } else {
                    None
                }
            });
            
            if dominated_by_statistics.unwrap_or(true) {
                matching_row_groups.push(i);
            }
        }
        
        if matching_row_groups.is_empty() {
            return Ok(vec![]);
        }
        
        let reader = builder.with_row_groups(matching_row_groups).build()?;

        let mut results = Vec::new();

        'outer: for batch_result in reader {
            let batch = batch_result?;

            let hashes = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected binary hash column"))?;
            let preimages = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected string preimage column"))?;
            let algorithms = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected string algorithm column"))?;
            let sources = batch
                .column(3)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid schema: expected list sources column"))?;

            for i in 0..batch.num_rows() {
                let hash = hashes.value(i);

                if !hash.starts_with(hash_prefix) {
                    continue;
                }

                let algorithm = algorithms.value(i);
                if algo.is_some_and(|filter| algorithm != filter) {
                    continue;
                }

                results.push(HashRecord {
                    hash: hash.to_vec(),
                    preimage: preimages.value(i).to_string(),
                    algorithm: algorithm.to_string(),
                    sources: Self::extract_sources(sources, i),
                });

                if limit.is_some_and(|l| results.len() >= l) {
                    break 'outer;
                }
            }
        }

        Ok(results)
    }

    fn stats(&self) -> Result<Stats> {
        if !self.path.exists() {
            return Ok(Stats::default());
        }

        if let Some(stats) = self.read_stats_from_metadata()? {
            return Ok(stats);
        }

        self.scan_stats()
    }
}
