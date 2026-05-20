use std::time::Duration;

#[cfg(feature = "tokio")]
pub(super) mod async_impl;
pub(super) mod sync_impl;

/// Configuration for a connection pool
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub struct PoolConfig {
    min_idle: u32,
    max_size: u32,
    idle_timeout: Duration,
}

impl PoolConfig {
    /// Create a new pool configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Minimum number of idle connections kept warm by the background pool.
    ///
    /// Setting this above zero allows the pool task to open replacement
    /// connections in the background, including any DNS lookup needed for the
    /// configured server.
    ///
    /// Defaults to `0`
    pub fn min_idle(mut self, min_idle: u32) -> Self {
        self.min_idle = min_idle;
        self
    }

    /// Maximum number of pooled connections
    ///
    /// Defaults to `10`
    pub fn max_size(mut self, max_size: u32) -> Self {
        self.max_size = max_size;
        self
    }

    /// Connection idle timeout
    ///
    /// Defaults to `60 seconds`
    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_idle: 0,
            max_size: 10,
            idle_timeout: Duration::from_secs(60),
        }
    }
}
