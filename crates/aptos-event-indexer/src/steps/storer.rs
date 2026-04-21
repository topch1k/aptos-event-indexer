use std::collections::HashMap;
use std::sync::Arc;

use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use aptos_indexer_processor_sdk::traits::{AsyncRunType, AsyncStep, NamedStep, Processable};
use aptos_indexer_processor_sdk::types::transaction_context::TransactionContext;
use aptos_indexer_processor_sdk::utils::errors::ProcessorError;
use async_trait::async_trait;
use futures::future::try_join_all;
use tracing::{debug, instrument, Instrument};

use crate::registry::{EventRegistry, ProcessorId};
use crate::steps::dispatcher::DispatchedBatch;
use crate::traits::event_processor::ParsedItem;

/// Pipeline step: groups the dispatched batch by [`ProcessorId`] and writes
/// each group through its processor's `store` method in parallel across
/// processors. This is the **authoritative** branch — failures propagate and
/// halt the pipeline so the checkpoint never advances past unpersisted data.
pub struct RegistryStorerStep {
    registry: Arc<EventRegistry>,
    pool: ArcDbPool,
}

impl RegistryStorerStep {
    pub fn new(registry: Arc<EventRegistry>, pool: ArcDbPool) -> Self {
        Self { registry, pool }
    }
}

impl AsyncStep for RegistryStorerStep {}

impl NamedStep for RegistryStorerStep {
    fn name(&self) -> String {
        "RegistryStorerStep".to_string()
    }
}

#[async_trait]
impl Processable for RegistryStorerStep {
    type Input = DispatchedBatch;
    type Output = DispatchedBatch;
    type RunType = AsyncRunType;

    #[instrument(
        level = "debug",
        name = "store_batch",
        skip_all,
        fields(
            start_version = ctx.metadata.start_version,
            end_version = ctx.metadata.end_version,
            group_count = tracing::field::Empty,
        ),
        err,
    )]
    async fn process(
        &mut self,
        ctx: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let groups = group_by_processor(ctx.data);
        tracing::Span::current().record("group_count", groups.len());

        // Parallel across processors; each processor may chunk internally.
        let futures = groups.iter().map(|(pid, items)| {
            let processor = self.registry.processor(*pid).clone();
            let pool = self.pool.clone();
            let span = tracing::debug_span!(
                "processor_store",
                processor = processor.name(),
                batch_size = items.len(),
            );
            async move {
                processor
                    .store(&pool, items)
                    .await
                    .map_err(|e| ProcessorError::ProcessError {
                        message: format!("processor `{}` store failed: {e:#}", processor.name()),
                    })
            }
            .instrument(span)
        });

        try_join_all(futures).await?;

        debug!(groups = groups.len(), "stored batch");

        // Reassemble passthrough payload so downstream steps (handlers,
        // OrderBy, VersionTracker) can still see the items. Preserve
        // deterministic order: by `(processor_id, (txn_version, event_index))`.
        let mut out: DispatchedBatch = Vec::new();
        for (pid, mut items) in groups {
            items.sort_by_key(|it| (it.ctx.transaction_version, it.ctx.event_index));
            out.extend(items.into_iter().map(|it| (pid, it)));
        }

        Ok(Some(TransactionContext {
            data: out,
            metadata: ctx.metadata,
        }))
    }
}

pub(crate) fn group_by_processor(batch: DispatchedBatch) -> Vec<(ProcessorId, Vec<ParsedItem>)> {
    let mut by_pid: HashMap<ProcessorId, Vec<ParsedItem>> = HashMap::new();
    for (pid, item) in batch {
        by_pid.entry(pid).or_default().push(item);
    }
    // Stable order across runs
    let mut entries: Vec<_> = by_pid.into_iter().collect();
    entries.sort_by_key(|(pid, _)| pid.0);
    entries
}
