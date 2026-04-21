use std::sync::Arc;

use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::{
    transaction::TxnData, Event, Transaction,
};
use aptos_indexer_processor_sdk::traits::{AsyncRunType, AsyncStep, NamedStep, Processable};
use aptos_indexer_processor_sdk::types::transaction_context::TransactionContext;
use aptos_indexer_processor_sdk::utils::errors::ProcessorError;
use async_trait::async_trait;
use chrono::DateTime;
use tracing::{debug, instrument, trace, warn};

use crate::context::EventContext;
use crate::registry::{EventRegistry, ProcessorId};
use crate::traits::event_processor::ParsedItem;

/// Batch of parsed events, grouped by originating [`ProcessorId`].
pub type DispatchedBatch = Vec<(ProcessorId, ParsedItem)>;

/// Pipeline step: takes a batch of raw [`Transaction`]s, iterates every event,
/// looks it up in the [`EventRegistry`] and — on match — delegates parsing to
/// the processor. Unknown events are ignored; parse errors are logged and
/// counted but do not fail the batch.
pub struct RegistryDispatcherStep {
    registry: Arc<EventRegistry>,
}

impl RegistryDispatcherStep {
    pub fn new(registry: Arc<EventRegistry>) -> Self {
        Self { registry }
    }
}

impl AsyncStep for RegistryDispatcherStep {}

impl NamedStep for RegistryDispatcherStep {
    fn name(&self) -> String {
        "RegistryDispatcherStep".to_string()
    }
}

#[async_trait]
impl Processable for RegistryDispatcherStep {
    type Input = Vec<Transaction>;
    type Output = DispatchedBatch;
    type RunType = AsyncRunType;

    #[instrument(
        level = "debug",
        name = "dispatch_batch",
        skip_all,
        fields(
            start_version = ctx.metadata.start_version,
            end_version = ctx.metadata.end_version,
            txn_count = ctx.data.len(),
        ),
    )]
    async fn process(
        &mut self,
        ctx: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let mut out: DispatchedBatch = Vec::new();

        for txn in &ctx.data {
            let txn_timestamp = txn
                .timestamp
                .as_ref()
                .and_then(|ts| DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
                .map(|dt| dt.naive_utc());
            let events = extract_events(txn);

            for (event_index, event) in events.iter().enumerate() {
                let Some(pid) = self.registry.lookup(&event.type_str) else {
                    trace!(
                        type_str = event.type_str.as_str(),
                        version = txn.version,
                        event_index,
                        "unmatched event type, skipping"
                    );
                    continue;
                };
                let processor = self.registry.processor(pid);

                let (account_address, creation_number) = event
                    .key
                    .as_ref()
                    .map(|k| (Some(k.account_address.clone()), Some(k.creation_number)))
                    .unwrap_or((None, None));

                let ev_ctx = EventContext {
                    transaction_version: txn.version,
                    event_index: event_index as u64,
                    sequence_number: event.sequence_number,
                    transaction_timestamp: txn_timestamp,
                    account_address,
                    creation_number,
                };

                match processor.parse(event, &ev_ctx) {
                    Ok(parsed) => out.push((
                        pid,
                        ParsedItem {
                            parsed,
                            ctx: ev_ctx,
                        },
                    )),
                    Err(e) => warn!(
                        processor = processor.name(),
                        type_str = event.type_str.as_str(),
                        version = txn.version,
                        event_index,
                        error = %e,
                        "event parse failed; skipping"
                    ),
                }
            }
        }

        debug!(matched = out.len(), "dispatched batch");

        Ok(Some(TransactionContext {
            data: out,
            metadata: ctx.metadata,
        }))
    }
}

/// Collect events from every `TxnData` variant that can carry them.
fn extract_events(txn: &Transaction) -> Vec<&Event> {
    match txn.txn_data.as_ref() {
        Some(TxnData::User(u)) => u.events.iter().collect(),
        Some(TxnData::BlockMetadata(b)) => b.events.iter().collect(),
        Some(TxnData::Genesis(g)) => g.events.iter().collect(),
        Some(TxnData::Validator(v)) => v.events.iter().collect(),
        Some(TxnData::BlockEpilogue(_)) | Some(TxnData::StateCheckpoint(_)) | None => Vec::new(),
    }
}
