use std::time::Duration;

/// Result of an HTTP request, as seen by the rate limiter.
pub enum RequestOutcome {
    /// Request succeeded (any non-throttled response).
    Success,
    /// Server returned 429 or 403 without a Retry-After header.
    Throttled,
    /// Server returned 429 with a Retry-After header specifying a wait duration.
    ThrottledWithRetryAfter(Duration),
}

/// Configuration for the rate limiter.
pub struct RetryConfig {
    /// The steady-state interval between requests. This is the target rate
    /// derived from documented API limits (e.g., 4s for 15 req/min). The
    /// limiter never goes faster than this.
    pub safe_interval: Duration,

    /// How much to decrease interval per success during recovery (e.g., 50ms).
    /// Only applies when interval is above safe_interval after a backoff.
    pub recovery_step: Duration,

    /// Factor to multiply interval on error (e.g., 2.0 = double the wait).
    pub backoff_multiplier: f64,
}

/// Rate limiter that respects a documented API rate limit.
///
/// Pure state machine — no async, no sleeping, no network. Takes events,
/// returns how long to wait before the next request. The caller is responsible
/// for actually sleeping.
///
/// Steady state: requests are evenly spaced at `safe_interval`. On error:
/// multiplicative backoff. On subsequent successes: linear recovery back to
/// `safe_interval`, then hold. Never goes faster than `safe_interval`.
pub struct RespectfulRetry {
    interval: Duration,
    config: RetryConfig,
}

impl RespectfulRetry {
    pub fn new(config: RetryConfig) -> Self {
        let interval = config.safe_interval;
        Self { interval, config }
    }

    /// Report the outcome of a request and get the delay before the next one.
    pub fn update(&mut self, outcome: RequestOutcome) -> Duration {
        match outcome {
            RequestOutcome::Success => self.on_success(),
            RequestOutcome::Throttled => self.on_throttled(),
            RequestOutcome::ThrottledWithRetryAfter(retry_after) => {
                self.on_throttled_with_retry_after(retry_after)
            }
        }
    }

    fn on_success(&mut self) -> Duration {
        // Recover toward safe_interval, but never go below it.
        self.interval = self
            .interval
            .saturating_sub(self.config.recovery_step)
            .max(self.config.safe_interval);
        self.interval
    }

    fn on_throttled(&mut self) -> Duration {
        self.interval =
            Duration::from_secs_f64(self.interval.as_secs_f64() * self.config.backoff_multiplier);
        self.interval
    }

    fn on_throttled_with_retry_after(&mut self, retry_after: Duration) -> Duration {
        let backed_off =
            Duration::from_secs_f64(self.interval.as_secs_f64() * self.config.backoff_multiplier);
        self.interval = backed_off.max(retry_after);
        self.interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RetryConfig {
        RetryConfig {
            safe_interval: Duration::from_secs(4),
            recovery_step: Duration::from_millis(100),
            backoff_multiplier: 2.0,
        }
    }

    #[test]
    fn steady_state_holds_at_safe_interval() {
        let mut limiter = RespectfulRetry::new(test_config());
        // Already at safe_interval — successes should not change it.
        for _ in 0..10 {
            let delay = limiter.update(RequestOutcome::Success);
            assert_eq!(delay, Duration::from_secs(4));
        }
    }

    #[test]
    fn never_goes_below_safe_interval() {
        let mut limiter = RespectfulRetry::new(test_config());
        // 100 successes should all return safe_interval.
        for _ in 0..100 {
            let delay = limiter.update(RequestOutcome::Success);
            assert_eq!(delay, Duration::from_secs(4));
        }
    }

    #[test]
    fn multiplicative_backoff_on_error() {
        let mut limiter = RespectfulRetry::new(test_config());
        let delay = limiter.update(RequestOutcome::Throttled);
        assert_eq!(delay, Duration::from_secs(8));
    }

    #[test]
    fn consecutive_errors_keep_doubling() {
        let mut limiter = RespectfulRetry::new(test_config());
        let d1 = limiter.update(RequestOutcome::Throttled);
        let d2 = limiter.update(RequestOutcome::Throttled);
        let d3 = limiter.update(RequestOutcome::Throttled);
        assert_eq!(d1, Duration::from_secs(8));
        assert_eq!(d2, Duration::from_secs(16));
        assert_eq!(d3, Duration::from_secs(32));
    }

    #[test]
    fn recovery_after_error() {
        let mut limiter = RespectfulRetry::new(test_config());
        // Error: 4s → 8s.
        limiter.update(RequestOutcome::Throttled);

        // Recovery: each success decreases by 100ms.
        // (8.0 - 4.0) / 0.1 = 40 successes to recover.
        let mut delay = Duration::ZERO;
        for _ in 0..39 {
            delay = limiter.update(RequestOutcome::Success);
        }
        // After 39 successes: 8.0 - 3.9 = 4.1s
        assert_eq!(delay, Duration::from_millis(4100));

        // 40th success: 4.1 - 0.1 = 4.0 = safe_interval.
        delay = limiter.update(RequestOutcome::Success);
        assert_eq!(delay, Duration::from_secs(4));

        // 41st success: stays at safe_interval.
        delay = limiter.update(RequestOutcome::Success);
        assert_eq!(delay, Duration::from_secs(4));
    }

    #[test]
    fn retry_after_respects_server_delay() {
        let mut limiter = RespectfulRetry::new(test_config());
        // Server says wait 25s. Multiplicative backoff would give 8s.
        let delay = limiter.update(RequestOutcome::ThrottledWithRetryAfter(
            Duration::from_secs(25),
        ));
        assert_eq!(delay, Duration::from_secs(25));
    }

    #[test]
    fn retry_after_uses_backoff_when_larger() {
        let mut config = test_config();
        config.safe_interval = Duration::from_secs(20);
        let mut limiter = RespectfulRetry::new(config);

        // Interval is 20s. Backoff gives 40s. Retry-After is 5s.
        let delay = limiter.update(RequestOutcome::ThrottledWithRetryAfter(
            Duration::from_secs(5),
        ));
        assert_eq!(delay, Duration::from_secs(40));
    }

    #[test]
    fn recovery_after_retry_after() {
        let mut limiter = RespectfulRetry::new(test_config());
        // Throttled with retry-after 25s.
        limiter.update(RequestOutcome::ThrottledWithRetryAfter(
            Duration::from_secs(25),
        ));

        // Recovery: (25.0 - 4.0) / 0.1 = 210 successes.
        let mut delay = Duration::ZERO;
        for _ in 0..300 {
            delay = limiter.update(RequestOutcome::Success);
        }
        assert_eq!(delay, Duration::from_secs(4));
    }

    #[test]
    fn error_during_recovery() {
        let mut limiter = RespectfulRetry::new(test_config());
        // Error: 4s → 8s.
        limiter.update(RequestOutcome::Throttled);

        // Partial recovery: 10 successes → 8.0 - 1.0 = 7.0s.
        for _ in 0..10 {
            limiter.update(RequestOutcome::Success);
        }

        // Another error at 7s → 14s.
        let delay = limiter.update(RequestOutcome::Throttled);
        assert_eq!(delay, Duration::from_secs(14));

        // Full recovery back to safe_interval.
        let mut d = Duration::ZERO;
        for _ in 0..200 {
            d = limiter.update(RequestOutcome::Success);
        }
        assert_eq!(d, Duration::from_secs(4));
    }
}
