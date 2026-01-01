# shaha
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/oritwoen/shaha)

Hash database builder and reverse lookup tool. *SHA + aha!*

Build precomputed hash databases from wordlists, then query them to find preimages.

## Installation

```bash
cargo install shaha
```

Or build from source:

```bash
git clone https://github.com/oritwoen/shaha
cd shaha
cargo build --release
```

## Usage

### Build a hash database

```bash
# Single algorithm (default: sha256)
shaha build words.txt

# Multiple algorithms
shaha build words.txt -a md5 -a sha256 -a keccak256

# Custom output file
shaha build words.txt -o mydb.parquet

# With source metadata
shaha build rockyou.txt -a hash160 -s rockyou
```

### Query for preimage

```bash
# Find preimage by hash
shaha query 5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8

# Prefix search
shaha query 5e8848

# Filter by algorithm
shaha query 5e8848 -a sha256

# Output formats
shaha query 5e8848 --format plain   # default
shaha query 5e8848 --format json
shaha query 5e8848 --format table
```

### Database info

```bash
shaha info hashes.parquet
```

Output:
```
Database: "hashes.parquet"
Records:  1000000
Size:     45.32 MB
Algorithms: sha256, md5, keccak256
Sources:  rockyou
```

## Algorithms

| Name | Description | Output |
|------|-------------|--------|
| `md5` | MD5 | 128 bit |
| `sha1` | SHA-1 | 160 bit |
| `sha256` | SHA-256 | 256 bit |
| `sha512` | SHA-512 | 512 bit |
| `hash160` | RIPEMD160(SHA256(x)) - Bitcoin addresses | 160 bit |
| `hash256` | SHA256(SHA256(x)) - Bitcoin blocks/txids | 256 bit |
| `keccak256` | Keccak-256 - Ethereum | 256 bit |
| `blake3` | BLAKE3 | 256 bit |
| `ripemd160` | RIPEMD-160 | 160 bit |

## Storage Format

Databases are stored as [Apache Parquet](https://parquet.apache.org/) files with ZSTD compression.

Schema:
- `hash` (Binary) - hash bytes
- `preimage` (Utf8) - original input data
- `algorithm` (Utf8) - algorithm name
- `sources` (List<Utf8>) - wordlist origins

Parquet files can be queried with DuckDB, Polars, Spark, or Cloudflare R2 SQL.

## Use Cases

- **Security research** - reverse hash lookups
- **CTF challenges** - quick hash cracking
- **Forensics** - identify known passwords
- **Blockchain analysis** - Bitcoin/Ethereum address research

## Configuration

Configuration is loaded from (in order of priority):
1. CLI flags
2. Environment variables
3. `.shaha.toml` in current directory
4. `~/.config/shaha/config.toml`

### Example config file

```toml
[storage.r2]
endpoint = "https://account-id.r2.cloudflarestorage.com"
bucket = "my-bucket"
access_key_id = "your-access-key"
secret_access_key = "your-secret-key"
region = "auto"
path = "hashes.parquet"

[defaults]
algorithms = ["sha256", "md5"]
output = "hashes.parquet"
```

### R2/S3 Storage

Build and query directly from Cloudflare R2 or S3-compatible storage:

```bash
# Build to R2
shaha build words.txt --r2

# Query from R2
shaha query 5e8848 --r2
```

Environment variables:
- `SHAHA_R2_ENDPOINT` - S3/R2 endpoint URL
- `SHAHA_R2_BUCKET` - Bucket name
- `SHAHA_R2_ACCESS_KEY_ID` or `AWS_ACCESS_KEY_ID`
- `SHAHA_R2_SECRET_ACCESS_KEY` or `AWS_SECRET_ACCESS_KEY`
- `SHAHA_R2_PATH` - Path within bucket
- `SHAHA_R2_REGION` - Region (default: "auto")

## Roadmap

- [ ] [R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/) - Apache Iceberg integration for faster queries

## License

MIT
