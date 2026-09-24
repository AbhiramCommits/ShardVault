//! Raw FFI declarations for the C block layer (`csrc/block.c`) plus the safe
//! [`block`] wrapper. Every `unsafe` operation in the crate lives in this file.

const SV_BLOCK_SIZE: usize = 4096;
const SV_BLOCK_HEADER_SIZE: usize = 24;
const SV_BLOCK_PAYLOAD_MAX: usize = SV_BLOCK_SIZE - SV_BLOCK_HEADER_SIZE;

const SV_OK: i32 = 0;
const SV_ERR_INVAL: i32 = -1;
const SV_ERR_CRC: i32 = -2;
const SV_ERR_LEN: i32 = -3;

#[repr(C)]
struct SVBlock {
    lsn: u64,
    payload_len: u32,
    flags: u8,
    payload: [u8; SV_BLOCK_PAYLOAD_MAX],
}

extern "C" {
    fn sv_crc32c(buf: *const u8, len: usize) -> u32;
    fn sv_block_encode(out: *mut u8, lsn: u64, payload: *const u8, len: u32, flags: u8) -> i32;
    fn sv_block_decode(input: *const u8, out: *mut SVBlock) -> i32;
}

pub mod block {
    use super::{
        sv_block_decode, sv_block_encode, sv_crc32c, SVBlock, SV_BLOCK_HEADER_SIZE,
        SV_BLOCK_PAYLOAD_MAX, SV_BLOCK_SIZE, SV_ERR_CRC, SV_ERR_INVAL, SV_ERR_LEN, SV_OK,
    };
    use std::mem::MaybeUninit;

    pub const BLOCK_SIZE: usize = SV_BLOCK_SIZE;
    pub const HEADER_SIZE: usize = SV_BLOCK_HEADER_SIZE;
    pub const PAYLOAD_MAX: usize = SV_BLOCK_PAYLOAD_MAX;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BlockError {
        Invalid,
        Crc,
        Len,
        Other(i32),
    }

    impl std::fmt::Display for BlockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                BlockError::Invalid => write!(f, "invalid argument"),
                BlockError::Crc => write!(f, "CRC-32C verification failed"),
                BlockError::Len => write!(f, "payload length out of range"),
                BlockError::Other(code) => write!(f, "unknown C error code: {code}"),
            }
        }
    }

    impl std::error::Error for BlockError {}

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Block {
        pub lsn: u64,
        pub flags: u8,
        pub payload: Vec<u8>,
    }

    pub fn crc32c(buf: &[u8]) -> u32 {
        // SAFETY: `buf.as_ptr()` is valid for reads of `buf.len()` bytes, which is
        // exactly the range the C function consumes.
        unsafe { sv_crc32c(buf.as_ptr(), buf.len()) }
    }

    pub fn encode(lsn: u64, payload: &[u8], flags: u8) -> Result<[u8; BLOCK_SIZE], BlockError> {
        if payload.len() > PAYLOAD_MAX {
            return Err(BlockError::Len);
        }
        let mut out = [0u8; BLOCK_SIZE];
        // SAFETY: `out.as_mut_ptr()` is valid for `BLOCK_SIZE` writes and
        // `payload.as_ptr()` is valid for reads of `payload.len()` bytes. The length
        // was checked against `PAYLOAD_MAX` above, and the C function only writes
        // the 24-byte header and the first `len` payload bytes of `out`.
        let rc = unsafe {
            sv_block_encode(
                out.as_mut_ptr(),
                lsn,
                payload.as_ptr(),
                payload.len() as u32,
                flags,
            )
        };
        rc_to_result(rc).map(|()| out)
    }

    pub fn decode(blk: &[u8; BLOCK_SIZE]) -> Result<Block, BlockError> {
        let mut raw = MaybeUninit::<SVBlock>::uninit();
        // SAFETY: `blk.as_ptr()` is valid for reads of `BLOCK_SIZE` bytes, and
        // `raw.as_mut_ptr()` is a valid, properly aligned out-pointer for `SVBlock`.
        // `raw` is only read via `assume_init` after the C function reports success,
        // which guarantees a fully initialized struct.
        let rc = unsafe { sv_block_decode(blk.as_ptr(), raw.as_mut_ptr()) };
        rc_to_result(rc)?;
        // SAFETY: guarded by `rc_to_result(rc)?` above — the C function returned
        // success, so `raw` holds an initialized `SVBlock`.
        let raw = unsafe { raw.assume_init() };
        Ok(Block {
            lsn: raw.lsn,
            flags: raw.flags,
            payload: raw.payload[..raw.payload_len as usize].to_vec(),
        })
    }

    fn rc_to_result(rc: i32) -> Result<(), BlockError> {
        match rc {
            SV_OK => Ok(()),
            SV_ERR_INVAL => Err(BlockError::Invalid),
            SV_ERR_CRC => Err(BlockError::Crc),
            SV_ERR_LEN => Err(BlockError::Len),
            other => Err(BlockError::Other(other)),
        }
    }
}
