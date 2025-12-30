use std::fs;
use std::io::Write;

use shaha::hasher;
use shaha::source::{FileSource, Source, UrlSource};
use shaha::storage::{HashRecord, ParquetStorage, Storage};

#[test]
fn test_sha256_known_vector() {
    let hasher = hasher::get_hasher("sha256").unwrap();
    let hash = hasher.hash(b"hello");
    assert_eq!(
        hex::encode(&hash),
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
}

#[test]
fn test_md5_known_vector() {
    let hasher = hasher::get_hasher("md5").unwrap();
    let hash = hasher.hash(b"hello");
    assert_eq!(hex::encode(&hash), "5d41402abc4b2a76b9719d911017c592");
}

#[test]
fn test_sha1_known_vector() {
    let hasher = hasher::get_hasher("sha1").unwrap();
    let hash = hasher.hash(b"hello");
    assert_eq!(hex::encode(&hash), "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
}

#[test]
fn test_keccak256_known_vector() {
    let hasher = hasher::get_hasher("keccak256").unwrap();
    let hash = hasher.hash(b"hello");
    assert_eq!(
        hex::encode(&hash),
        "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8"
    );
}

#[test]
fn test_hash160_known_vector() {
    let hasher = hasher::get_hasher("hash160").unwrap();
    let hash = hasher.hash(b"hello");
    // hash160 = RIPEMD160(SHA256(hello))
    assert_eq!(hex::encode(&hash), "b6a9c8c230722b7c748331a8b450f05566dc7d0f");
}

#[test]
fn test_hash256_known_vector() {
    let hasher = hasher::get_hasher("hash256").unwrap();
    let hash = hasher.hash(b"hello");
    // hash256 = SHA256(SHA256(hello))
    assert_eq!(
        hex::encode(&hash),
        "9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50"
    );
}

#[test]
fn test_sha512_known_vector() {
    let hasher = hasher::get_hasher("sha512").unwrap();
    let hash = hasher.hash(b"hello");
    assert_eq!(
        hex::encode(&hash),
        "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
    );
}

#[test]
fn test_blake3_known_vector() {
    let hasher = hasher::get_hasher("blake3").unwrap();
    let hash = hasher.hash(b"hello");
    assert_eq!(
        hex::encode(&hash),
        "ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f"
    );
}

#[test]
fn test_ripemd160_known_vector() {
    let hasher = hasher::get_hasher("ripemd160").unwrap();
    let hash = hasher.hash(b"hello");
    assert_eq!(hex::encode(&hash), "108f07b8382412612c048d07d13f814118445acd");
}

#[test]
fn test_available_algorithms() {
    let algos = hasher::available_algorithms();
    assert!(algos.contains(&"sha256"));
    assert!(algos.contains(&"md5"));
    assert!(algos.contains(&"keccak256"));
    assert!(algos.contains(&"hash160"));
    assert!(algos.contains(&"hash256"));
}

#[test]
fn test_unknown_algorithm_returns_none() {
    assert!(hasher::get_hasher("unknown").is_none());
    assert!(hasher::get_hasher("sha999").is_none());
}

#[test]
fn test_file_source() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("words.txt");

    {
        let mut file = fs::File::create(&file_path).unwrap();
        writeln!(file, "hello").unwrap();
        writeln!(file, "world").unwrap();
        writeln!(file).unwrap();
        writeln!(file, "test").unwrap();
    }

    let source = FileSource::new(&file_path);
    let words: Vec<String> = source.words().unwrap().collect();

    assert_eq!(words, vec!["hello", "world", "test"]);
}

#[test]
fn test_roundtrip_write_query() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.parquet");

    let hasher = hasher::get_hasher("sha256").unwrap();
    let hash = hasher.hash(b"password");

    let records = vec![HashRecord {
        hash: hash.clone(),
        preimage: "password".to_string(),
        algorithm: "sha256".to_string(),
        sources: vec!["test".to_string()],
    }];

    let mut storage = ParquetStorage::new(&db_path);
    storage.write_batch(records).unwrap();
    storage.finish().unwrap();

    let results = storage.query(&hash, None, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].preimage, "password");
    assert_eq!(results[0].algorithm, "sha256");

    let prefix = &hash[..4];
    let results = storage.query(prefix, None, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].preimage, "password");
}

#[test]
fn test_query_with_algorithm_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.parquet");

    let sha256 = hasher::get_hasher("sha256").unwrap();
    let md5 = hasher::get_hasher("md5").unwrap();

    let records = vec![
        HashRecord {
            hash: sha256.hash(b"hello"),
            preimage: "hello".to_string(),
            algorithm: "sha256".to_string(),
            sources: vec![],
        },
        HashRecord {
            hash: md5.hash(b"hello"),
            preimage: "hello".to_string(),
            algorithm: "md5".to_string(),
            sources: vec![],
        },
    ];

    let mut storage = ParquetStorage::new(&db_path);
    storage.write_batch(records).unwrap();
    storage.finish().unwrap();

    let sha256_hash = sha256.hash(b"hello");
    let results = storage.query(&sha256_hash[..4], None, None).unwrap();
    assert_eq!(results.len(), 1);

    let results = storage.query(&sha256_hash[..4], Some("sha256"), None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].algorithm, "sha256");

    let results = storage.query(&sha256_hash[..4], Some("md5"), None).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_stats_from_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.parquet");

    let sha256 = hasher::get_hasher("sha256").unwrap();
    let md5 = hasher::get_hasher("md5").unwrap();

    let records = vec![
        HashRecord {
            hash: sha256.hash(b"hello"),
            preimage: "hello".to_string(),
            algorithm: "sha256".to_string(),
            sources: vec!["test".to_string()],
        },
        HashRecord {
            hash: md5.hash(b"hello"),
            preimage: "hello".to_string(),
            algorithm: "md5".to_string(),
            sources: vec!["test".to_string(), "other".to_string()],
        },
        HashRecord {
            hash: sha256.hash(b"world"),
            preimage: "world".to_string(),
            algorithm: "sha256".to_string(),
            sources: vec!["other".to_string()],
        },
    ];

    let mut storage = ParquetStorage::new(&db_path);
    storage.write_batch(records).unwrap();
    storage.finish().unwrap();

    let stats = storage.stats().unwrap();
    assert_eq!(stats.total_records, 3);
    assert!(stats.algorithms.contains(&"sha256".to_string()));
    assert!(stats.algorithms.contains(&"md5".to_string()));
    assert_eq!(stats.algorithms.len(), 2);
    assert!(stats.sources.contains(&"test".to_string()));
    assert!(stats.sources.contains(&"other".to_string()));
    assert_eq!(stats.sources.len(), 2);
    assert!(stats.file_size_bytes > 0);
}

#[test]
fn test_append_mode_merges_sources() {
    use std::collections::HashMap;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.parquet");

    let sha256 = hasher::get_hasher("sha256").unwrap();

    // Step 1: Create initial database with source "wordlist1"
    let initial_records = vec![
        HashRecord {
            hash: sha256.hash(b"hello"),
            preimage: "hello".to_string(),
            algorithm: "sha256".to_string(),
            sources: vec!["wordlist1".to_string()],
        },
        HashRecord {
            hash: sha256.hash(b"world"),
            preimage: "world".to_string(),
            algorithm: "sha256".to_string(),
            sources: vec!["wordlist1".to_string()],
        },
    ];

    let mut storage = ParquetStorage::new(&db_path);
    storage.write_batch(initial_records).unwrap();
    storage.finish().unwrap();

    let storage = ParquetStorage::new(&db_path);
    let existing = storage.query(&[], None, None).unwrap();
    
    let mut records_map: HashMap<(Vec<u8>, String), HashRecord> = HashMap::new();
    for record in existing {
        let key = (record.hash.clone(), record.algorithm.clone());
        records_map.insert(key, record);
    }

    // Step 3: Process new records with source "wordlist2"
    // "hello" already exists (should merge), "test" is new
    let new_words = vec!["hello", "test"];
    for word in new_words {
        let hash = sha256.hash(word.as_bytes());
        let key = (hash.clone(), "sha256".to_string());
        
        if let Some(existing) = records_map.get_mut(&key) {
            if !existing.sources.contains(&"wordlist2".to_string()) {
                existing.sources.push("wordlist2".to_string());
            }
        } else {
            records_map.insert(key, HashRecord {
                hash,
                preimage: word.to_string(),
                algorithm: "sha256".to_string(),
                sources: vec!["wordlist2".to_string()],
            });
        }
    }

    // Step 4: Write merged records
    let mut storage = ParquetStorage::new(&db_path);
    let records: Vec<HashRecord> = records_map.into_values().collect();
    storage.write_batch(records).unwrap();
    storage.finish().unwrap();

    // Step 5: Verify results
    let storage = ParquetStorage::new(&db_path);
    
    let hello_hash = sha256.hash(b"hello");
    let results = storage.query(&hello_hash, Some("sha256"), None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].preimage, "hello");
    assert!(results[0].sources.contains(&"wordlist1".to_string()));
    assert!(results[0].sources.contains(&"wordlist2".to_string()));
    assert_eq!(results[0].sources.len(), 2);

    let world_hash = sha256.hash(b"world");
    let results = storage.query(&world_hash, Some("sha256"), None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sources, vec!["wordlist1".to_string()]);

    let test_hash = sha256.hash(b"test");
    let results = storage.query(&test_hash, Some("sha256"), None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sources, vec!["wordlist2".to_string()]);

    // Total should be 3 records
    let stats = storage.stats().unwrap();
    assert_eq!(stats.total_records, 3);
}

#[test]
fn test_bloom_filter_rejects_nonexistent_hash() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.parquet");

    let sha256 = hasher::get_hasher("sha256").unwrap();

    let records = vec![
        HashRecord {
            hash: sha256.hash(b"hello"),
            preimage: "hello".to_string(),
            algorithm: "sha256".to_string(),
            sources: vec!["test".to_string()],
        },
        HashRecord {
            hash: sha256.hash(b"world"),
            preimage: "world".to_string(),
            algorithm: "sha256".to_string(),
            sources: vec!["test".to_string()],
        },
    ];

    let mut storage = ParquetStorage::new(&db_path);
    storage.write_batch(records).unwrap();
    storage.finish().unwrap();

    let storage = ParquetStorage::new(&db_path);

    let existing_hash = sha256.hash(b"hello");
    let results = storage.query(&existing_hash, None, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].preimage, "hello");

    let nonexistent_hash = sha256.hash(b"notindb");
    let results = storage.query(&nonexistent_hash, None, None).unwrap();
    assert_eq!(results.len(), 0);

    let prefix = &existing_hash[..4];
    let results = storage.query(prefix, None, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].preimage, "hello");
}

#[test]
fn test_query_with_limit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.parquet");

    let sha256 = hasher::get_hasher("sha256").unwrap();

    let records: Vec<HashRecord> = (0..100)
        .map(|i| {
            let word = format!("word{}", i);
            HashRecord {
                hash: sha256.hash(word.as_bytes()),
                preimage: word,
                algorithm: "sha256".to_string(),
                sources: vec!["test".to_string()],
            }
        })
        .collect();

    let mut storage = ParquetStorage::new(&db_path);
    storage.write_batch(records).unwrap();
    storage.finish().unwrap();

    let storage = ParquetStorage::new(&db_path);

    let results = storage.query(&[], None, None).unwrap();
    assert_eq!(results.len(), 100);

    let results = storage.query(&[], None, Some(10)).unwrap();
    assert_eq!(results.len(), 10);

    let results = storage.query(&[], None, Some(1)).unwrap();
    assert_eq!(results.len(), 1);

    let results = storage.query(&[], None, Some(1000)).unwrap();
    assert_eq!(results.len(), 100);
}

#[test]
fn test_empty_file_source() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("empty.txt");

    fs::File::create(&file_path).unwrap();

    let source = FileSource::new(&file_path);
    let words: Vec<String> = source.words().unwrap().collect();

    assert!(words.is_empty());
}

#[test]
fn test_file_source_with_long_lines() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("long.txt");

    {
        let mut file = fs::File::create(&file_path).unwrap();
        let long_word = "a".repeat(10_000);
        writeln!(file, "{}", long_word).unwrap();
        writeln!(file, "short").unwrap();
    }

    let source = FileSource::new(&file_path);
    let words: Vec<String> = source.words().unwrap().collect();

    assert_eq!(words.len(), 2);
    assert_eq!(words[0].len(), 10_000);
    assert_eq!(words[1], "short");
}

#[test]
fn test_file_source_content_hash_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("words.txt");

    {
        let mut file = fs::File::create(&file_path).unwrap();
        writeln!(file, "hello").unwrap();
        writeln!(file, "world").unwrap();
    }

    let source1 = FileSource::new(&file_path);
    let hash1 = source1.content_hash().unwrap().unwrap();

    let source2 = FileSource::new(&file_path);
    let hash2 = source2.content_hash().unwrap().unwrap();

    assert_eq!(hash1, hash2);
}

#[test]
fn test_query_nonexistent_database() {
    let storage = ParquetStorage::new("/nonexistent/path.parquet");
    let results = storage.query(&[], None, None).unwrap();
    assert!(results.is_empty());

    let stats = storage.stats().unwrap();
    assert_eq!(stats.total_records, 0);
}

#[test]
fn test_write_empty_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("empty.parquet");

    let mut storage = ParquetStorage::new(&db_path);
    storage.write_batch(vec![]).unwrap();
    storage.finish().unwrap();

    assert!(!db_path.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_url_source_content_hash_deterministic() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("hello\nworld\ntest\n"))
        .expect(2)
        .mount(&mock_server)
        .await;

    let uri = mock_server.uri();
    let (source1, source2) = tokio::task::spawn_blocking(move || {
        let s1 = UrlSource::new(&uri).unwrap();
        let s2 = UrlSource::new(&uri).unwrap();
        (s1, s2)
    })
    .await
    .unwrap();

    let hash1 = source1.content_hash().unwrap().unwrap();
    let hash2 = source2.content_hash().unwrap().unwrap();

    assert_eq!(hash1, hash2);
    assert_eq!(hash1.len(), 64);
}

#[test]
fn test_url_source_fetch_error_connection_refused() {
    let result = UrlSource::new("http://127.0.0.1:1/words.txt");
    assert!(result.is_err());

    let err = result.err().unwrap();
    assert!(err.to_string().contains("Failed to fetch URL"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_url_source_http_500_succeeds() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let uri = mock_server.uri();
    let source = tokio::task::spawn_blocking(move || UrlSource::new(&uri))
        .await
        .unwrap();

    assert!(source.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_url_source_empty_response() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .mount(&mock_server)
        .await;

    let uri = mock_server.uri();
    let source = tokio::task::spawn_blocking(move || UrlSource::new(&uri))
        .await
        .unwrap()
        .unwrap();

    let words: Vec<String> = source.words().unwrap().collect();
    assert!(words.is_empty());

    let hash = source.content_hash().unwrap().unwrap();
    assert_eq!(hash.len(), 64);

    let expected_empty_hash = blake3::hash(b"").to_hex().to_string();
    assert_eq!(hash, expected_empty_hash);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_url_source_words_parsing() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("hello\n\nworld\n\n\ntest\n"),
        )
        .mount(&mock_server)
        .await;

    let uri = mock_server.uri();
    let source = tokio::task::spawn_blocking(move || UrlSource::new(&uri))
        .await
        .unwrap()
        .unwrap();

    let words: Vec<String> = source.words().unwrap().collect();

    assert_eq!(words, vec!["hello", "world", "test"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_url_source_name_extraction() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("test"))
        .expect(4)
        .mount(&mock_server)
        .await;

    let uri = mock_server.uri();

    let url_with_file = format!("{}/words.txt", uri);
    let url_with_path = format!("{}/path/to/rockyou.txt", uri);
    let url_no_extension = format!("{}/wordlist", uri);
    let base_uri = uri.clone();

    let (name1, name2, name3, name4) = tokio::task::spawn_blocking(move || {
        let s1 = UrlSource::new(&url_with_file).unwrap();
        let s2 = UrlSource::new(&url_with_path).unwrap();
        let s3 = UrlSource::new(&url_no_extension).unwrap();
        let s4 = UrlSource::new(&base_uri).unwrap();
        (
            s1.name().to_string(),
            s2.name().to_string(),
            s3.name().to_string(),
            s4.name().to_string(),
        )
    })
    .await
    .unwrap();

    assert_eq!(name1, "words");
    assert_eq!(name2, "rockyou");
    assert_eq!(name3, "wordlist");
    assert!(!name4.is_empty());
}

#[test]
fn test_quiet_mode_toggle() {
    shaha::output::set_quiet(false);
    assert!(!shaha::output::is_quiet());
    
    shaha::output::set_quiet(true);
    assert!(shaha::output::is_quiet());
    
    shaha::output::set_quiet(false);
    assert!(!shaha::output::is_quiet());
}

#[test]
fn test_is_quiet_controls_output() {
    use std::io::Write;
    
    shaha::output::set_quiet(true);
    let mut buffer = Vec::new();
    if !shaha::output::is_quiet() {
        writeln!(buffer, "should not appear").unwrap();
    }
    assert!(buffer.is_empty());
    
    shaha::output::set_quiet(false);
    if !shaha::output::is_quiet() {
        writeln!(buffer, "should appear").unwrap();
    }
    assert!(!buffer.is_empty());
    
    shaha::output::set_quiet(false);
}

#[test]
fn test_dry_run_shows_stats_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let words_path = dir.path().join("words.txt");
    let output_path = dir.path().join("output.parquet");

    {
        let mut file = fs::File::create(&words_path).unwrap();
        writeln!(file, "hello").unwrap();
        writeln!(file, "world").unwrap();
        writeln!(file, "hello").unwrap();
    }

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_shaha"))
        .args([
            "build",
            words_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
            "-a",
            "sha256",
            "-a",
            "md5",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run shaha");

    assert!(output.status.success());
    assert!(
        !output_path.exists(),
        "File should not be created in dry-run mode"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("[dry-run]"));
    assert!(stderr.contains("Unique words: 2"));
    assert!(stderr.contains("Records to generate: 4"));
    assert!(stderr.contains("sha256"));
    assert!(stderr.contains("md5"));
}

#[test]
fn test_dry_run_shows_append_info() {
    let dir = tempfile::tempdir().unwrap();
    let words_path = dir.path().join("words.txt");
    let db_path = dir.path().join("existing.parquet");

    let sha256 = hasher::get_hasher("sha256").unwrap();
    let records = vec![HashRecord {
        hash: sha256.hash(b"existing"),
        preimage: "existing".to_string(),
        algorithm: "sha256".to_string(),
        sources: vec!["old".to_string()],
    }];
    let mut storage = ParquetStorage::new(&db_path);
    storage.write_batch(records).unwrap();
    storage.finish().unwrap();

    {
        let mut file = fs::File::create(&words_path).unwrap();
        writeln!(file, "hello").unwrap();
    }

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_shaha"))
        .args([
            "build",
            words_path.to_str().unwrap(),
            "-o",
            db_path.to_str().unwrap(),
            "--append",
            "--dry-run",
        ])
        .output()
        .expect("Failed to run shaha");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("[dry-run]"));
    assert!(
        stderr.contains("Append mode"),
        "Should show append mode info"
    );
}

#[test]
fn test_dry_run_shows_already_processed() {
    let dir = tempfile::tempdir().unwrap();
    let words_path = dir.path().join("words.txt");
    let db_path = dir.path().join("test.parquet");

    {
        let mut file = fs::File::create(&words_path).unwrap();
        writeln!(file, "hello").unwrap();
    }

    std::process::Command::new(env!("CARGO_BIN_EXE_shaha"))
        .args([
            "build",
            words_path.to_str().unwrap(),
            "-o",
            db_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run shaha");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_shaha"))
        .args([
            "build",
            words_path.to_str().unwrap(),
            "-o",
            db_path.to_str().unwrap(),
            "--dry-run",
        ])
        .output()
        .expect("Failed to run shaha");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("[dry-run]"));
    assert!(
        stderr.contains("already processed"),
        "Should indicate source was already processed"
    );
    assert!(
        stderr.contains("--force"),
        "Should mention --force option"
    );
}

#[test]
fn test_dry_run_formats_large_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let words_path = dir.path().join("words.txt");

    {
        let mut file = fs::File::create(&words_path).unwrap();
        for i in 0..1500 {
            writeln!(file, "word{}", i).unwrap();
        }
    }

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_shaha"))
        .args(["build", words_path.to_str().unwrap(), "--dry-run"])
        .output()
        .expect("Failed to run shaha");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("1,500"),
        "Should format numbers with comma separator"
    );
}
