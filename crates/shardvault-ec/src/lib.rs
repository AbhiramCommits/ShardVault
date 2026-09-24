//! Hand-rolled Reed-Solomon erasure coding over GF(2^8).
//!
//! No external coding crates: the field arithmetic, matrix operations, and
//! encoding/reconstruction are implemented here from scratch.

pub mod gf;
pub mod matrix;
pub mod rs;
