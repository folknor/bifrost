//! Secret string storage for credentials and SASL payloads.

use std::fmt;
use std::ops::Deref;

use zeroize::Zeroizing;

/// A string that zeroizes its allocation on drop and redacts under `Debug`.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    /// Borrow the secret as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Borrow the secret as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Consume the wrapper and return the zeroizing string.
    pub fn into_zeroizing(self) -> Zeroizing<String> {
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

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Converts caller-owned secret material into zeroizing storage.
pub trait IntoSecretString {
    /// Move the secret into zeroizing storage.
    fn into_secret_string(self) -> SecretString;
}

impl IntoSecretString for String {
    fn into_secret_string(self) -> SecretString {
        SecretString::from(self)
    }
}

impl IntoSecretString for &str {
    fn into_secret_string(self) -> SecretString {
        SecretString::from(self)
    }
}

impl IntoSecretString for SecretString {
    fn into_secret_string(self) -> SecretString {
        self
    }
}

impl IntoSecretString for Zeroizing<String> {
    fn into_secret_string(self) -> SecretString {
        SecretString::from(self)
    }
}
