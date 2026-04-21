use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use diesel_migrations::EmbeddedMigrations;

use crate::context::EventContext;

/// A type-erased parsed event together with its on-chain context.
///
/// Produced by [`EventProcessor::parse`] and consumed by
/// [`EventProcessor::store`] / [`EventProcessor::handle`].
pub struct ParsedItem {
    pub parsed: Box<dyn Any + Send + Sync>,
    pub ctx: EventContext,
}

impl ParsedItem {
    pub fn new<T: Any + Send + Sync>(value: T, ctx: EventContext) -> Self {
        Self {
            parsed: Box::new(value),
            ctx,
        }
    }
}

/// Core extension point: a single on-chain event type.
///
/// Implement this trait directly for advanced cases (e.g. events needing
/// multi-table inserts or cross-event joins). For the simple
/// one-event-one-table case, prefer the [`crate::Indexable`] sugar trait and
/// wrap it in [`crate::TypedEventProcessor`].
#[async_trait]
pub trait EventProcessor: Send + Sync + 'static {
    /// Fully-qualified Move type, e.g. `"0xADDR::marketplace::Listed"`.
    fn type_str(&self) -> &'static str;

    /// Short name used for logging, metrics, and migration ordering.
    fn name(&self) -> &'static str;

    /// Parse one on-chain event into a type-erased, processor-owned value.
    ///
    /// Errors are logged and counted by the dispatcher but do not abort the
    /// batch.
    fn parse(&self, event: &Event, ctx: &EventContext) -> Result<Box<dyn Any + Send + Sync>>;

    /// Persist a batch of parsed items (all belonging to this processor).
    ///
    /// This is the **authoritative** step: failures bubble up and halt the
    /// pipeline so that the checkpoint never advances past data we could not
    /// persist.
    async fn store(&self, pool: &ArcDbPool, items: &[ParsedItem]) -> Result<()>;

    /// Optional side-effects (event bus, HTTP, webhook, …).
    ///
    /// Failures are logged but **never** propagate — they must not block
    /// indexing. The default impl is a no-op.
    async fn handle(&self, _items: &[ParsedItem]) -> Result<()> {
        Ok(())
    }

    /// Optional user migrations that create the processor's tables.
    ///
    /// The runner applies them after the SDK's built-in migrations, in
    /// registration order.
    fn migrations(&self) -> Option<EmbeddedMigrations> {
        None
    }
}

/// Convenience alias — processors are always stored behind `Arc<dyn>` so the
/// registry can hand clones to every pipeline step without copying.
pub type ArcEventProcessor = Arc<dyn EventProcessor>;
