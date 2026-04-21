use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::context::EventContext;

/// A typed, reusable side-effect attached to a single event type.
///
/// Handlers are registered on a [`crate::TypedEventProcessor<E>`] via the
/// builder; the processor downcasts once per batch and invokes every
/// registered handler with the typed slice.
///
/// Handler errors are **logged and counted** by the runner but never stall
/// indexing. See [`RetryPolicy`] for per-handler retry tuning.
#[async_trait]
pub trait EventHandler<E>: Send + Sync + 'static
where
    E: Send + Sync + 'static,
{
    /// Human-readable name for logs/metrics.
    fn name(&self) -> &'static str;

    /// Process a batch of typed events (all from the same transaction-stream
    /// batch, ordered by `(transaction_version, event_index)`).
    async fn handle(&self, batch: &[(E, EventContext)]) -> Result<()>;

    /// Retry policy for this handler. Default is 3 attempts with exponential
    /// backoff (100ms → 1s → 5s).
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }
}

/// Retry configuration for a handler.
///
/// On failure the runner sleeps `initial_backoff`, doubles up to
/// `max_backoff`, and retries up to `max_attempts` times. After that the
/// batch is dropped (with an error log + counter) and indexing continues.
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
    /// Compute the backoff duration before attempt `attempt` (0-indexed).
    pub fn backoff(&self, attempt: u32) -> Duration {
        let factor = 1u64 << attempt.min(20); // avoid overflow
        let ms = self.initial_backoff.as_millis() as u64 * factor;
        Duration::from_millis(ms).min(self.max_backoff)
    }
}
