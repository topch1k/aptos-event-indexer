use anyhow::Result;
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;

use crate::context::EventContext;
use crate::traits::indexable::Indexable;

/// Persistence adapter for an [`Indexable`] event.
///
/// Kept separate from [`Indexable`] itself so users can plug any diesel
/// schema / row representation without the library forcing a particular
/// shape. Storers are the **authoritative** write path — failures bubble
/// up to halt the pipeline so the checkpoint never advances past
/// unpersisted data.
#[async_trait]
pub trait Storer<E: Indexable>: Send + Sync + 'static {
    async fn store(&self, pool: &ArcDbPool, items: &[(E, EventContext)]) -> Result<()>;
}
