use anyhow::Result;
use aptos_event_indexer::{EventContext, EventHandler};
use async_trait::async_trait;
use tracing::info;

use crate::events::{CounterDecrementedEvent, CounterIncrementedEvent};

/// Logs each event to stdout. Demonstrates a side-effect handler; swap for a
/// real Kafka/Service Bus/webhook client in your own project.
pub struct StdoutLoggerHandler;

#[async_trait]
impl EventHandler<CounterIncrementedEvent> for StdoutLoggerHandler {
    fn name(&self) -> &'static str {
        "stdout_logger(incremented)"
    }

    async fn handle(&self, batch: &[(CounterIncrementedEvent, EventContext)]) -> Result<()> {
        for (event, ctx) in batch {
            info!(
                version = ctx.transaction_version,
                event_index = ctx.event_index,
                account = event.account.as_str(),
                old_value = event.old_value,
                new_value = event.new_value,
                by = event.increment_by,
                "CounterIncremented"
            );
        }
        Ok(())
    }
}

#[async_trait]
impl EventHandler<CounterDecrementedEvent> for StdoutLoggerHandler {
    fn name(&self) -> &'static str {
        "stdout_logger(decremented)"
    }

    async fn handle(&self, batch: &[(CounterDecrementedEvent, EventContext)]) -> Result<()> {
        for (event, ctx) in batch {
            info!(
                version = ctx.transaction_version,
                event_index = ctx.event_index,
                account = event.account.as_str(),
                old_value = event.old_value,
                new_value = event.new_value,
                by = event.decrement_by,
                "CounterDecremented"
            );
        }
        Ok(())
    }
}
