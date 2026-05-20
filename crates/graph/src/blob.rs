use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobHashError {
    WrongByteLen(usize),
    WrongHexLen(usize),
    NotHex,
}

impl fmt::Display for BlobHashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongByteLen(len) => write!(f, "expected 32 bytes, got {len}"),
            Self::WrongHexLen(len) => write!(f, "expected 64 hex chars, got {len}"),
            Self::NotHex => f.write_str("non-hex character in input"),
        }
    }
}

impl std::error::Error for BlobHashError {}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct BlobHash([u8; 32]);

impl BlobHash {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self, BlobHashError> {
        if bytes.len() != 32 {
            return Err(BlobHashError::WrongByteLen(bytes.len()));
        }

        let mut buf = [0u8; 32];
        buf.copy_from_slice(bytes);
        Ok(Self(buf))
    }

    pub fn from_hex(s: &str) -> Result<Self, BlobHashError> {
        if s.len() != 64 {
            return Err(BlobHashError::WrongHexLen(s.len()));
        }

        let mut buf = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
            let hi = hex_nibble(chunk[0])?;
            let lo = hex_nibble(chunk[1])?;
            buf[i] = (hi << 4) | lo;
        }
        Ok(Self(buf))
    }

    pub fn hash(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for byte in &self.0 {
            s.push(nibble_hex(byte >> 4));
            s.push(nibble_hex(byte & 0x0f));
        }
        s
    }
}

fn hex_nibble(c: u8) -> Result<u8, BlobHashError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(BlobHashError::NotHex),
    }
}

fn nibble_hex(n: u8) -> char {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    char::from(HEX[usize::from(n)])
}

impl fmt::Display for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobHash({})", self.to_hex())
    }
}

impl Serialize for BlobHash {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for BlobHash {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct HexVisitor;

        impl Visitor<'_> for HexVisitor {
            type Value = BlobHash;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("64 lowercase hex chars")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<BlobHash, E> {
                BlobHash::from_hex(v).map_err(de::Error::custom)
            }
        }

        de.deserialize_str(HexVisitor)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic() {
        let a = BlobHash::hash(b"hello world");
        let b = BlobHash::hash(b"hello world");
        assert_eq!(a, b);
        assert_eq!(a.to_hex(), b.to_hex());
    }

    #[test]
    fn hex_round_trips() {
        let h = BlobHash::hash(b"round trip");
        let parsed = BlobHash::from_hex(&h.to_hex()).unwrap();
        assert_eq!(h, parsed);
    }
}
