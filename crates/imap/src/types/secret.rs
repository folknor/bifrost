//! Secret string storage for credentials and SASL payloads.

use std::fmt;
use std::ops::Deref;

use zeroize::Zeroizing;

/// A string that zeroizes its allocation on drop and redacts under `Debug`.
///
/// `as_str`, `as_bytes`, and `Deref<Target = str>` intentionally expose the
/// raw secret to authentication code. Callers should treat those borrows like
/// any other credential material and avoid formatting or logging them.
#[repr(transparent)]
#[derive(Clone, Default, Eq)]
pub(crate) struct SecretString(Zeroizing<String>);

impl SecretString {
    /// Borrow the unredacted secret as a string slice.
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Borrow the unredacted secret as bytes.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Consume the wrapper and return the zeroizing string.
    pub(crate) fn into_zeroizing(self) -> Zeroizing<String> {
        self.0
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self(Zeroizing::new(value.to_owned()))
    }
}

impl From<Zeroizing<String>> for SecretString {
    fn from(value: Zeroizing<String>) -> Self {
        Self(value)
    }
}

impl Deref for SecretString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for SecretString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for SecretString {
    fn eq(&self, other: &Self) -> bool {
        let a = self.as_bytes();
        let b = other.as_bytes();
        let mut diff = a.len() ^ b.len();
        let max_len = a.len().max(b.len());
        for index in 0..max_len {
            let lhs = a.get(index).copied().unwrap_or(0);
            let rhs = b.get(index).copied().unwrap_or(0);
            diff |= usize::from(lhs ^ rhs);
        }
        diff == 0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}
