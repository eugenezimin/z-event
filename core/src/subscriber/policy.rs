//! Retry and reconnect timing: [`Backoff`] and [`RetryPolicy`].

use std::time::Duration;

/// Exponential backoff with optional jitter.
#[derive(Clone, Debug, PartialEq)]
pub struct Backoff {
    /// Delay before the first retry.
    pub initial: Duration,
    /// Upper bound for any delay.
    pub max: Duration,
    /// Growth factor per attempt.
    pub multiplier: f64,
    /// Scale each delay by a random factor in `[0.5, 1.0]`, so many
    /// consumers do not retry in lockstep.
    pub jitter: bool,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

impl Backoff {
    /// A backoff from `initial` to `max`, doubling, with jitter.
    pub fn new(initial: Duration, max: Duration) -> Self {
        Backoff {
            initial,
            max,
            ..Backoff::default()
        }
    }

    /// The delay before retry number `attempt` (1-based).
    pub fn delay(&self, attempt: u32) -> Duration {
        let exp = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
        let base = self.initial.as_secs_f64() * self.multiplier.max(1.0).powi(exp);
        let capped = base.min(self.max.as_secs_f64());
        let factor = if self.jitter {
            0.5 + fastrand::f64() * 0.5
        } else {
            1.0
        };
        Duration::from_secs_f64((capped * factor).max(0.0))
    }
}

/// How often a failing handler is retried before its event is parked.
#[derive(Clone, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Attempts in total, including the first (default 5).
    pub max_attempts: u32,
    /// Delay between attempts (default 0.1 s to 30 s).
    pub backoff: Backoff,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 5,
            backoff: Backoff::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_and_caps() {
        let b = Backoff {
            jitter: false,
            ..Backoff::new(Duration::from_millis(100), Duration::from_secs(1))
        };
        assert_eq!(b.delay(1), Duration::from_millis(100));
        assert_eq!(b.delay(2), Duration::from_millis(200));
        assert_eq!(b.delay(10), Duration::from_secs(1));
        assert_eq!(b.delay(u32::MAX), Duration::from_secs(1));
    }

    #[test]
    fn jitter_stays_in_bounds() {
        let b = Backoff::new(Duration::from_millis(100), Duration::from_secs(1));
        for attempt in 1..20 {
            let d = b.delay(attempt);
            assert!(d <= Duration::from_secs(1));
            assert!(d >= Duration::from_millis(50));
        }
    }
}
