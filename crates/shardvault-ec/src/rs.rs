//! Reed-Solomon encoding and reconstruction.

use crate::gf;
use crate::matrix::Matrix;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcError {
    TooFewShards,
    InconsistentShards,
}

impl std::fmt::Display for EcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EcError::TooFewShards => write!(f, "fewer than k surviving shards"),
            EcError::InconsistentShards => {
                write!(f, "shard count or lengths are inconsistent")
            }
        }
    }
}

impl std::error::Error for EcError {}

pub struct ReedSolomon {
    k: usize,
    m: usize,
    encode_matrix: Matrix,
    mul_tables: Vec<Vec<[u8; 256]>>,
}

impl ReedSolomon {
    pub fn new(k: usize, m: usize) -> ReedSolomon {
        assert!(k >= 1, "k must be at least 1");
        assert!(m >= 1, "m must be at least 1");
        assert!(k + m <= 255, "k + m must not exceed 255");
        let encode_matrix = Matrix::vandermonde_systematic(k, m);
        let mut mul_tables = vec![vec![[0u8; 256]; k]; m];
        for (j, row) in mul_tables.iter_mut().enumerate() {
            for (i, table) in row.iter_mut().enumerate() {
                let coeff = encode_matrix.get(k + j, i);
                for (x, slot) in table.iter_mut().enumerate() {
                    *slot = gf::mul(coeff, x as u8);
                }
            }
        }
        ReedSolomon {
            k,
            m,
            encode_matrix,
            mul_tables,
        }
    }

    pub fn data_shard_count(&self) -> usize {
        self.k
    }

    pub fn parity_shard_count(&self) -> usize {
        self.m
    }

    pub fn shard_count(&self) -> usize {
        self.k + self.m
    }

    /// Encodes k equal-length data shards and returns the m parity shards.
    /// The caller is responsible for padding shards to equal length.
    pub fn encode(&self, data_shards: &[Vec<u8>]) -> Vec<Vec<u8>> {
        assert_eq!(data_shards.len(), self.k, "need exactly k data shards");
        let shard_len = data_shards[0].len();
        assert!(
            data_shards.iter().all(|s| s.len() == shard_len),
            "all data shards must be equal length"
        );
        let mut parity: Vec<Vec<u8>> = (0..self.m).map(|_| vec![0u8; shard_len]).collect();
        for (j, par) in parity.iter_mut().enumerate() {
            for (i, data) in data_shards.iter().enumerate() {
                let table = &self.mul_tables[j][i];
                for (pos, out) in par.iter_mut().enumerate() {
                    *out ^= table[data[pos] as usize];
                }
            }
        }
        parity
    }

    /// Rebuilds any missing shards (entries that are `None`) given at least k
    /// survivors. Surviving shards must all have the same length.
    pub fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), EcError> {
        let n = self.k + self.m;
        if shards.len() != n {
            return Err(EcError::InconsistentShards);
        }
        let survivors: Vec<usize> = (0..n).filter(|&i| shards[i].is_some()).collect();
        if survivors.len() < self.k {
            return Err(EcError::TooFewShards);
        }
        let survivors = &survivors[..self.k];
        let shard_len = shards[survivors[0]].as_ref().unwrap().len();
        if survivors
            .iter()
            .any(|&i| shards[i].as_ref().unwrap().len() != shard_len)
        {
            return Err(EcError::InconsistentShards);
        }

        let mut sub = Matrix::new(self.k, self.k);
        for (r, &si) in survivors.iter().enumerate() {
            for c in 0..self.k {
                sub.set(r, c, self.encode_matrix.get(si, c));
            }
        }
        let inv = sub.invert().ok_or(EcError::InconsistentShards)?;

        let missing: Vec<usize> = (0..n).filter(|&i| shards[i].is_none()).collect();
        for &mi in &missing {
            let mut coeffs = vec![0u8; self.k];
            for (j, slot) in coeffs.iter_mut().enumerate() {
                let mut acc = 0u8;
                for t in 0..self.k {
                    acc = gf::add(acc, gf::mul(self.encode_matrix.get(mi, t), inv.get(t, j)));
                }
                *slot = acc;
            }
            let mut rebuilt = vec![0u8; shard_len];
            for (j, &si) in survivors.iter().enumerate() {
                let coeff = coeffs[j];
                if coeff == 0 {
                    continue;
                }
                let s = shards[si].as_ref().unwrap();
                for pos in 0..shard_len {
                    rebuilt[pos] ^= gf::mul(coeff, s[pos]);
                }
            }
            shards[mi] = Some(rebuilt);
        }
        Ok(())
    }
}
