use anyhow::Result;
use async_trait::async_trait;

use crate::context::EventContext;
use crate::traits::retry::RetryPolicy;

/// A typed, reusable side-effect attached to a single event type.
///
/// Handlers are registered on a [`crate::TypedEventProcessor<E>`] via the
/// builder; the processor downcasts once per batch and invokes every
/// registered handler with the typed slice.
///
/// Handler errors are **logged and counted** by the runner but never stall
/// indexing. Retries are driven by the handler's [`RetryPolicy`]
/// (exponential backoff via the `backon` crate).
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
