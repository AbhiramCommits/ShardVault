//! Shared error type for the store.

use std::fmt;

#[derive(Debug)]
pub enum StoreError {
    Io(std::io::Error),
    Corrupt(String),
    RecordTooLarge,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "io error: {e}"),
            StoreError::Corrupt(msg) => write!(f, "store corruption: {msg}"),
            StoreError::RecordTooLarge => write!(f, "record exceeds block payload capacity"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}
