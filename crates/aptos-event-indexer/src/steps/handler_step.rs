use std::sync::Arc;

use aptos_indexer_processor_sdk::traits::{AsyncRunType, AsyncStep, NamedStep, Processable};
use aptos_indexer_processor_sdk::types::transaction_context::TransactionContext;
use aptos_indexer_processor_sdk::utils::errors::ProcessorError;
use async_trait::async_trait;
use tracing::{debug, error, instrument};

use crate::registry::EventRegistry;
use crate::steps::dispatcher::DispatchedBatch;
use crate::steps::storer::group_by_processor;

/// Pipeline step: invokes `EventProcessor::handle` for every processor group
/// in the batch. Errors are logged and counted but **never** propagated —
/// indexing continues regardless of handler health. Handler-internal retry is
/// the responsibility of [`crate::TypedEventProcessor`] / user impls.
pub struct RegistryHandlerStep {
    registry: Arc<EventRegistry>,
}

impl RegistryHandlerStep {
    pub fn new(registry: Arc<EventRegistry>) -> Self {
        Self { registry }
    }
}

impl AsyncStep for RegistryHandlerStep {}

impl NamedStep for RegistryHandlerStep {
    fn name(&self) -> String {
        "RegistryHandlerStep".to_string()
    }
}

#[async_trait]
impl Processable for RegistryHandlerStep {
    type Input = DispatchedBatch;
    type Output = DispatchedBatch;
    type RunType = AsyncRunType;

    #[instrument(
        level = "debug",
        name = "handle_batch",
        skip_all,
        fields(
            start_version = ctx.metadata.start_version,
            end_version = ctx.metadata.end_version,
            group_count = tracing::field::Empty,
        ),
    )]
    async fn process(
        &mut self,
        ctx: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let groups = group_by_processor(ctx.data);
        tracing::Span::current().record("group_count", groups.len());

        for (pid, items) in &groups {
            let processor = self.registry.processor(*pid);
            if let Err(e) = processor.handle(items).await {
                error!(
                    processor = processor.name(),
                    error = %e,
                    "handle() returned error; continuing pipeline"
                );
            }
        }

        debug!(groups = groups.len(), "handled batch");

        // Passthrough so downstream `VersionTrackerStep` can advance.
        let mut out: DispatchedBatch = Vec::new();
        for (pid, items) in groups {
            out.extend(items.into_iter().map(|it| (pid, it)));
        }

        Ok(Some(TransactionContext {
            data: out,
            metadata: ctx.metadata,
        }))
    }
}
