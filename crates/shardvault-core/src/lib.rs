//! ShardVault core: on-disk block layer, WAL, and segment metadata.

pub mod error;
pub mod ffi;
pub mod rollup;
pub mod segment;
pub mod stripe;
pub mod wal;

pub use ffi::block;
