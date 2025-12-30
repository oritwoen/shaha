# PROJECT KNOWLEDGE BASE

**Generated:** 2025-12-30
**Commit:** 542d89b
**Branch:** main

## OVERVIEW

Rust CLI for building precomputed hash databases from wordlists (Parquet format) and querying them for reverse lookups. Security research, CTF, forensics use cases.

## STRUCTURE

```
shaha/
├── src/
│   ├── cli/           # Command handlers (build, query, info, source)
│   ├── hasher/        # Hash algorithms via macro (impl_digest_hasher!)
│   ├── source/        # Data sources: file, stdin, url, seclists, aspell
│   ├── storage/       # Backends: parquet (local), r2 (S3/DuckDB)
│   ├── config.rs      # TOML config loader (.shaha.toml, XDG)
│   ├── lib.rs         # Public API exports
│   └── main.rs        # CLI entry point
└── tests/
    └── integration.rs # All tests (unit + integration)
```

## WHERE TO LOOK

| Task | Location | Pattern |
|------|----------|---------|
| Add hash algorithm | `src/hasher/mod.rs` | Use `impl_digest_hasher!` macro |
| Add data source | `src/source/` | Implement `Source` trait |
| Add storage backend | `src/storage/` | Implement `Storage` trait |
| Add CLI command | `src/cli/` | Add to `Commands` enum in mod.rs |
| Config options | `src/config.rs` | Nested TOML structure |

## ARCHITECTURE

**Three core traits:**

```rust
trait Hasher: Send + Sync {
    fn name(&self) -> &'static str;
    fn hash(&self, input: &[u8]) -> Vec<u8>;
}

trait Source {
    fn name(&self) -> &str;
    fn words(&self) -> Result<Box<dyn Iterator<Item = String>>>;
    fn content_hash(&self) -> Result<Option<String>>;  // blake3 for dedup
}

trait Storage {
    fn write_batch(&mut self, records: Vec<HashRecord>) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn query(&self, hash_prefix: &[u8], algo: Option<&str>, limit: Option<usize>) -> Result<Vec<HashRecord>>;
    fn stats(&self) -> Result<Stats>;
}
```

## CONVENTIONS

- **Hasher impl**: Use `impl_digest_hasher!` macro for Digest-based algorithms
- **Source parsing**: `provider:path` syntax (seclists:Passwords/x.txt, aspell:en)
- **Content dedup**: Sources implement `content_hash()` → blake3 of content
- **Source metadata**: Stored in parquet as `shaha:source_hashes` JSON array
- **Config priority**: CLI flags > env vars > .shaha.toml > ~/.config/shaha/config.toml
- **No doc comments**: Code should be self-documenting (enforced by hook)

## ANTI-PATTERNS

- **No `as any` / `@ts-ignore`**: Rust project, but same principle - no type suppression
- **No empty error handling**: Always propagate with `?` or handle explicitly
- **No hardcoded paths**: Use `dirs` crate for XDG compliance
- **No blocking in async**: DuckDB used for R2 is blocking, contained in r2.rs

## SOURCE PROVIDERS

| Provider | Pull | Usage |
|----------|------|-------|
| `seclists` | `shaha source pull seclists` | `--from seclists:Passwords/rockyou.txt` |
| `aspell` | System package | `--from aspell:en` |
| `file` | - | `--from file:words.txt` or positional |
| URL | - | `--from https://example.com/words.txt` |

## ALGORITHMS

md5, sha1, sha256, sha512, hash160 (Bitcoin), hash256 (Bitcoin), keccak256 (Ethereum), blake3, ripemd160

## COMMANDS

```bash
# Build
shaha build words.txt -a sha256 -a md5
shaha build --from seclists:Passwords/rockyou.txt

# Query
shaha query 5e8848                    # prefix search
shaha query <hash> -a sha256          # filter by algo
shaha query <hash> --format json

# Info
shaha info hashes.parquet

# Source management
shaha source pull seclists
shaha source list seclists Passwords
shaha source list aspell

# R2/S3 (via config or flags)
shaha build words.txt --r2
shaha query 5e8848 --r2
```

## STORAGE

**Parquet schema:**
- `hash` (Binary) - raw bytes, sorted for binary search
- `preimage` (Utf8) - original input
- `algorithm` (Utf8) - "sha256", "md5", etc.
- `sources` (List<Utf8>) - ["rockyou", "common"]

**Metadata keys:**
- `shaha:bloom_filter` - Base64-encoded bloom filter for fast rejection
- `shaha:source_hashes` - JSON array of blake3 content hashes

## NOTES

- Row group stats enable prefix search without full scan
- Bloom filter checked BEFORE parquet query for known-miss fast path
- Source hash dedup skips rebuild if content unchanged (use `--force` to override)
- R2 storage uses DuckDB's httpfs extension (not native S3 client)
