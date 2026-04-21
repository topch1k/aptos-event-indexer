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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::EventContext;
    use crate::traits::event_processor::ParsedItem;

    fn ctx(v: u64, i: u64) -> EventContext {
        EventContext {
            transaction_version: v,
            event_index: i,
            sequence_number: 0,
            transaction_timestamp: None,
            account_address: None,
            creation_number: None,
        }
    }

    fn item(v: u64, i: u64) -> ParsedItem {
        ParsedItem::new((v, i), ctx(v, i))
    }

    #[test]
    fn empty_batch_produces_no_groups() {
        assert!(group_by_processor(vec![]).is_empty());
    }

    #[test]
    fn single_processor_gets_single_group() {
        let batch = vec![
            (ProcessorId(7), item(1, 0)),
            (ProcessorId(7), item(1, 1)),
            (ProcessorId(7), item(2, 0)),
        ];
        let groups = group_by_processor(batch);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, ProcessorId(7));
        assert_eq!(groups[0].1.len(), 3);
    }

    #[test]
    fn groups_are_sorted_by_processor_id() {
        let batch = vec![
            (ProcessorId(5), item(1, 0)),
            (ProcessorId(1), item(1, 1)),
            (ProcessorId(3), item(1, 2)),
            (ProcessorId(1), item(1, 3)),
        ];
        let groups = group_by_processor(batch);
        let ids: Vec<u16> = groups.iter().map(|(pid, _)| pid.0).collect();
        assert_eq!(ids, vec![1, 3, 5]);
        assert_eq!(groups[0].1.len(), 2);
    }

    #[test]
    fn preserves_intra_group_insertion_order() {
        let batch = vec![
            (ProcessorId(0), item(10, 0)),
            (ProcessorId(0), item(20, 0)),
            (ProcessorId(0), item(30, 0)),
        ];
        let groups = group_by_processor(batch);
        let versions: Vec<u64> = groups[0]
            .1
            .iter()
            .map(|it| it.ctx.transaction_version)
            .collect();
        assert_eq!(versions, vec![10, 20, 30]);
    }
}
