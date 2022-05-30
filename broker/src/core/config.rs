//! Simple file for holding configs for the ReplicaManager

pub const DEFAULT_SEGMENT_SIZE: usize = 2usize.pow(17); // ~100K
pub const MAX_LAG: u128 = 10000; // ms
pub const MAX_DELAY: u128 = 2500; // ms
pub const POLL_INTERVAL: u64 = 2000; // ms
pub const ISR_INTERVAL: u64 = 2500; // ms
pub const ISR_MAX_RETRIES: u8 = 5;
pub const WATERMARK_INTERVAL: u64 = 500; // ms
