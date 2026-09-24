//! GF(2^8) arithmetic with primitive polynomial `0x11D`.
//!
//! `exp`/`log` tables are built once on first use ([`OnceLock`]) and shared
//! across threads. Multiplication is a table lookup; addition is XOR.

use std::sync::OnceLock;

const POLY: u16 = 0x11D;

struct GfTables {
    exp: [u8; 512],
    log: [u8; 256],
}

fn build_tables() -> GfTables {
    let mut exp = [0u8; 512];
    let mut log = [0u8; 256];
    let mut x: u16 = 1;
    for i in 0..255u16 {
        exp[i as usize] = x as u8;
        log[x as usize] = i as u8;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= POLY;
        }
    }
    for i in 0..255 {
        exp[255 + i] = exp[i];
    }
    GfTables { exp, log }
}

static TABLES: OnceLock<GfTables> = OnceLock::new();

fn tables() -> &'static GfTables {
    TABLES.get_or_init(build_tables)
}

pub fn add(a: u8, b: u8) -> u8 {
    a ^ b
}

pub fn mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    let t = tables();
    t.exp[t.log[a as usize] as usize + t.log[b as usize] as usize]
}

pub fn inv(a: u8) -> u8 {
    if a == 0 {
        return 0;
    }
    let t = tables();
    t.exp[255 - t.log[a as usize] as usize]
}

pub fn div(a: u8, b: u8) -> u8 {
    mul(a, inv(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exp_log_consistency() {
        let t = tables();
        for a in 1..=255u16 {
            assert_eq!(t.exp[t.log[a as usize] as usize], a as u8);
        }
        for i in 0..255 {
            assert_eq!(t.log[t.exp[i] as usize] as usize, i);
        }
    }

    #[test]
    fn mul_identity_and_zero() {
        for a in 0..=255u16 {
            let a = a as u8;
            assert_eq!(mul(a, 1), a);
            assert_eq!(mul(1, a), a);
            assert_eq!(mul(a, 0), 0);
            assert_eq!(mul(0, a), 0);
        }
    }

    #[test]
    fn add_is_xor() {
        assert_eq!(add(0x5A, 0x3C), 0x5A ^ 0x3C);
        assert_eq!(add(0x5A, 0x5A), 0);
    }

    #[test]
    fn inv_of_zero_is_zero() {
        assert_eq!(inv(0), 0);
        assert_eq!(div(3, 0), 0);
    }
}
