use std::time::Duration;

pub(super) mod async_impl;

/// Configuration for a connection pool
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
// pub: users tune SMTP and LMTP pool size and idle behavior.
pub struct PoolConfig {
    min_idle: u32,
    max_size: u32,
    idle_timeout: Duration,
    test_on_checkout: bool,
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

    /// Whether reused connections are probed with `NOOP` before checkout.
    ///
    /// Defaults to `true`. Set this to `false` to avoid the checkout round
    /// trip when the caller is willing to retry a send on a server-closed idle
    /// connection. Connections already marked broken are always discarded.
    ///
    /// This buys nothing for LMTP transports: every LMTP delivery retires its
    /// connection at recycle, so each transaction starts on a fresh
    /// connection that is never probed anyway.
    pub fn test_on_checkout(mut self, test_on_checkout: bool) -> Self {
        self.test_on_checkout = test_on_checkout;
        self
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_idle: 0,
            max_size: 10,
            idle_timeout: Duration::from_secs(60),
            test_on_checkout: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PoolConfig;

    #[test]
    fn checkout_probe_defaults_to_enabled_and_can_be_disabled() {
        assert!(PoolConfig::default().test_on_checkout);
        assert!(!PoolConfig::new().test_on_checkout(false).test_on_checkout);
    }
}
