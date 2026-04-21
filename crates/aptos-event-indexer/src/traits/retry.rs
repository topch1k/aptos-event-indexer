use std::time::Duration;

use backon::ExponentialBuilder;

/// Retry configuration for an [`crate::EventHandler`].
///
/// On failure the runner retries up to `max_attempts - 1` times with
/// exponential backoff starting at `initial_backoff` and capped at
/// `max_backoff`. After the final attempt the batch is dropped (with an
/// error log + counter) and indexing continues — handlers are best-effort.
///
/// Converted to a [`backon::ExponentialBuilder`] at call time via
/// [`RetryPolicy::to_backoff`]; the library uses `backon` to drive the
/// actual retry loop.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// Materialize this policy as a [`backon::ExponentialBuilder`].
    ///
    /// `max_attempts` maps to `with_max_times(max_attempts - 1)` because
    /// backon counts *retries* (not total attempts): a policy with
    /// `max_attempts = 3` means "try once, then retry up to 2 more times".
    pub fn to_backoff(&self) -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(self.initial_backoff)
            .with_max_delay(self.max_backoff)
            .with_factor(2.0)
            .with_max_times(self.max_attempts.saturating_sub(1) as usize)
    }
}
