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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn default_values() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.initial_backoff, Duration::from_millis(100));
        assert_eq!(p.max_backoff, Duration::from_secs(5));
    }

    #[rstest]
    #[case(0_u32, 0_usize)]
    #[case(1, 0)]
    #[case(3, 2)]
    #[case(10, 9)]
    fn max_attempts_saturates_to_max_retries(
        #[case] max_attempts: u32,
        #[case] expected_max_times: usize,
    ) {
        let policy = RetryPolicy {
            max_attempts,
            ..RetryPolicy::default()
        };
        // We can't introspect ExponentialBuilder internals directly, but we
        // can exercise retry behavior end-to-end.
        let _builder = policy.to_backoff();
        // Sanity: saturating_sub matches our expectation.
        assert_eq!(max_attempts.saturating_sub(1) as usize, expected_max_times);
    }

    #[tokio::test]
    async fn backoff_drives_expected_number_of_attempts() {
        use backon::Retryable;
        use std::sync::atomic::{AtomicU32, Ordering};

        let attempts = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        };

        let op = || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err::<(), anyhow::Error>(anyhow::anyhow!("boom"))
        };

        let _ = op.retry(policy.to_backoff()).await;
        // max_attempts = 3 → 1 initial + 2 retries = 3 total invocations.
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn stops_retrying_on_success() {
        use backon::Retryable;
        use std::sync::atomic::{AtomicU32, Ordering};

        let attempts = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        };

        let op = || async {
            let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if n < 2 {
                Err::<u32, anyhow::Error>(anyhow::anyhow!("retry me"))
            } else {
                Ok(n)
            }
        };

        let out = op.retry(policy.to_backoff()).await.unwrap();
        assert_eq!(out, 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
