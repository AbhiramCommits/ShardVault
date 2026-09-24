//! Dense GF(2^8) matrices: multiplication, Gaussian-elimination inversion,
//! and the systematic Vandermonde encoding matrix.

use crate::gf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matrix {
    rows: usize,
    cols: usize,
    data: Vec<u8>,
}

impl Matrix {
    pub fn new(rows: usize, cols: usize) -> Matrix {
        Matrix {
            rows,
            cols,
            data: vec![0; rows * cols],
        }
    }

    pub fn identity(n: usize) -> Matrix {
        let mut m = Matrix::new(n, n);
        for i in 0..n {
            m.set(i, i, 1);
        }
        m
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn cols(&self) -> usize {
        self.cols
    }

    pub fn get(&self, r: usize, c: usize) -> u8 {
        self.data[r * self.cols + c]
    }

    pub fn set(&mut self, r: usize, c: usize, v: u8) {
        self.data[r * self.cols + c] = v;
    }

    pub fn mul(&self, other: &Matrix) -> Matrix {
        assert_eq!(self.cols, other.rows, "incompatible matrix dimensions");
        let mut out = Matrix::new(self.rows, other.cols);
        for r in 0..self.rows {
            for c in 0..other.cols {
                let mut acc = 0u8;
                for k in 0..self.cols {
                    acc = gf::add(acc, gf::mul(self.get(r, k), other.get(k, c)));
                }
                out.set(r, c, acc);
            }
        }
        out
    }

    pub fn invert(&self) -> Option<Matrix> {
        if self.rows != self.cols {
            return None;
        }
        let n = self.rows;
        let mut aug = Matrix::new(n, 2 * n);
        for r in 0..n {
            for c in 0..n {
                aug.set(r, c, self.get(r, c));
            }
            aug.set(r, n + r, 1);
        }
        for col in 0..n {
            let mut pivot = None;
            for r in col..n {
                if aug.get(r, col) != 0 {
                    pivot = Some(r);
                    break;
                }
            }
            let pivot = pivot?;
            if pivot != col {
                for c in 0..2 * n {
                    let tmp = aug.get(pivot, c);
                    aug.set(pivot, c, aug.get(col, c));
                    aug.set(col, c, tmp);
                }
            }
            let pv_inv = gf::inv(aug.get(col, col));
            for c in 0..2 * n {
                aug.set(col, c, gf::mul(aug.get(col, c), pv_inv));
            }
            for r in 0..n {
                if r == col {
                    continue;
                }
                let factor = aug.get(r, col);
                if factor == 0 {
                    continue;
                }
                for c in 0..2 * n {
                    aug.set(
                        r,
                        c,
                        gf::add(aug.get(r, c), gf::mul(factor, aug.get(col, c))),
                    );
                }
            }
        }
        let mut out = Matrix::new(n, n);
        for r in 0..n {
            for c in 0..n {
                out.set(r, c, aug.get(r, n + c));
            }
        }
        Some(out)
    }

    /// Systematic (k+m) x k encoding matrix: the top k rows are the identity
    /// (data shards pass through) and the bottom m rows are coding rows.
    ///
    /// Built as `V * T^-1`, where `V` is the full (k+m) x k Vandermonde
    /// matrix over the points 1..=k+m and `T` is its top k x k block. Since
    /// every k-row submatrix of `V` is invertible, every k-row submatrix of
    /// the result is invertible too — a plain `[I; V]` stack is not (e.g.
    /// k=4, m=3, rows {2,4,5,6}).
    pub fn vandermonde_systematic(k: usize, m: usize) -> Matrix {
        assert!(k + m <= 255, "k + m must not exceed 255");
        let n = k + m;
        let mut v = Matrix::new(n, k);
        for i in 0..n {
            let point = i as u8 + 1;
            let mut x = 1u8;
            for c in 0..k {
                v.set(i, c, x);
                x = gf::mul(x, point);
            }
        }
        let mut top = Matrix::new(k, k);
        for r in 0..k {
            for c in 0..k {
                top.set(r, c, v.get(r, c));
            }
        }
        let top_inv = top
            .invert()
            .expect("Vandermonde top block must be invertible");
        v.mul(&top_inv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_mul() {
        let a = Matrix::vandermonde_systematic(3, 2);
        let id = Matrix::identity(a.cols());
        assert_eq!(a.mul(&id), a);
        let id = Matrix::identity(a.rows());
        assert_eq!(id.mul(&a), a);
    }

    #[test]
    fn every_k_row_submatrix_is_invertible() {
        for &(k, m) in &[(2usize, 2usize), (3, 2), (4, 3), (6, 3)] {
            let mat = Matrix::vandermonde_systematic(k, m);
            let n = k + m;
            for mask in 0u32..(1u32 << n) {
                if mask.count_ones() != k as u32 {
                    continue;
                }
                let rows: Vec<usize> = (0..n).filter(|&i| mask & (1 << i) != 0).collect();
                let mut sub = Matrix::new(k, k);
                for (r, &ri) in rows.iter().enumerate() {
                    for c in 0..k {
                        sub.set(r, c, mat.get(ri, c));
                    }
                }
                let inv = sub.invert().expect("submatrix must be invertible");
                let prod = sub.mul(&inv);
                for r in 0..k {
                    for c in 0..k {
                        assert_eq!(prod.get(r, c), if r == c { 1 } else { 0 });
                    }
                }
            }
        }
    }

    #[test]
    fn singular_matrix_returns_none() {
        let mut m = Matrix::new(2, 2);
        m.set(0, 0, 1);
        m.set(0, 1, 1);
        m.set(1, 0, 1);
        m.set(1, 1, 1);
        assert!(m.invert().is_none());
    }
}
