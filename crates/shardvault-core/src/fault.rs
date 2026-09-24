//! Fault injection for crash-recovery testing.
//!
//! Enabled by the `fault-injection` cargo feature. When `SV_FAIL_AT_FSYNC=n`
//! is set in the environment, the process hard-aborts on the nth fsync.
//! Without the feature (or the variable) this module is a no-op.

#[cfg(feature = "fault-injection")]
mod imp {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    static COUNT: AtomicU64 = AtomicU64::new(0);
    static FAIL_AT: OnceLock<Option<u64>> = OnceLock::new();

    pub fn maybe_fail() {
        let fail_at = FAIL_AT.get_or_init(|| {
            std::env::var("SV_FAIL_AT_FSYNC")
                .ok()
                .and_then(|v| v.parse().ok())
        });
        let n = COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(target) = fail_at.as_ref() {
            if n == *target {
                std::process::abort();
            }
        }
    }

    pub fn count() -> u64 {
        COUNT.load(Ordering::SeqCst)
    }
}

#[cfg(not(feature = "fault-injection"))]
mod imp {
    pub fn maybe_fail() {}
    pub fn count() -> u64 {
        0
    }
}

pub use imp::{count, maybe_fail};
