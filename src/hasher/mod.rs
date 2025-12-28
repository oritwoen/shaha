use md5::Md5;
use ripemd::Ripemd160;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};
use sha3::Keccak256;

pub trait Hasher: Send + Sync {
    fn name(&self) -> &'static str;
    fn hash(&self, input: &[u8]) -> Vec<u8>;
}

/// Standard hashers using the Digest trait
macro_rules! impl_digest_hasher {
    ($struct_name:ident, $hasher_type:ty, $algo_name:literal) => {
        pub struct $struct_name;

        impl Hasher for $struct_name {
            fn name(&self) -> &'static str {
                $algo_name
            }

            fn hash(&self, input: &[u8]) -> Vec<u8> {
                <$hasher_type>::digest(input).to_vec()
            }
        }
    };
}

impl_digest_hasher!(Md5Hasher, Md5, "md5");
impl_digest_hasher!(Sha1Hasher, Sha1, "sha1");
impl_digest_hasher!(Sha256Hasher, Sha256, "sha256");
impl_digest_hasher!(Sha512Hasher, Sha512, "sha512");
impl_digest_hasher!(Keccak256Hasher, Keccak256, "keccak256");
impl_digest_hasher!(Ripemd160Hasher, Ripemd160, "ripemd160");

// BLAKE3 - different API (not Digest trait)
pub struct Blake3Hasher;

impl Hasher for Blake3Hasher {
    fn name(&self) -> &'static str {
        "blake3"
    }

    fn hash(&self, input: &[u8]) -> Vec<u8> {
        blake3::hash(input).as_bytes().to_vec()
    }
}

// Hash160 = RIPEMD160(SHA256(x)) - Bitcoin address derivation
pub struct Hash160Hasher;

impl Hasher for Hash160Hasher {
    fn name(&self) -> &'static str {
        "hash160"
    }

    fn hash(&self, input: &[u8]) -> Vec<u8> {
        let sha = Sha256::digest(input);
        Ripemd160::digest(sha).to_vec()
    }
}

// Hash256 = SHA256(SHA256(x)) - Bitcoin block/txid hashing
pub struct Hash256Hasher;

impl Hasher for Hash256Hasher {
    fn name(&self) -> &'static str {
        "hash256"
    }

    fn hash(&self, input: &[u8]) -> Vec<u8> {
        let first = Sha256::digest(input);
        Sha256::digest(first).to_vec()
    }
}

pub fn get_hasher(name: &str) -> Option<Box<dyn Hasher>> {
    match name.to_lowercase().as_str() {
        "md5" => Some(Box::new(Md5Hasher)),
        "sha1" => Some(Box::new(Sha1Hasher)),
        "sha256" => Some(Box::new(Sha256Hasher)),
        "sha512" => Some(Box::new(Sha512Hasher)),
        "hash160" => Some(Box::new(Hash160Hasher)),
        "hash256" | "dsha256" => Some(Box::new(Hash256Hasher)),
        "keccak256" | "keccak-256" => Some(Box::new(Keccak256Hasher)),
        "blake3" => Some(Box::new(Blake3Hasher)),
        "ripemd160" | "ripemd-160" => Some(Box::new(Ripemd160Hasher)),
        _ => None,
    }
}

pub fn available_algorithms() -> &'static [&'static str] {
    &[
        "md5",
        "sha1",
        "sha256",
        "sha512",
        "hash160",
        "hash256",
        "keccak256",
        "blake3",
        "ripemd160",
    ]
}
