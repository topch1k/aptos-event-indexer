use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use backon::Retryable;
use diesel_migrations::EmbeddedMigrations;
use tracing::{error, warn};

use crate::context::EventContext;
use crate::traits::event_handler::EventHandler;
use crate::traits::event_processor::{EventProcessor, ParsedItem};
use crate::traits::indexable::Indexable;
use crate::traits::storer::Storer;

/// Ready-made [`EventProcessor`] wrapping an [`Indexable`] event, a
/// [`Storer`], and any number of [`EventHandler`]s.
///
/// Build it via [`TypedEventProcessor::new`] then chain `.with_handler(...)`
/// / `.with_migrations(...)` before registering with the
/// [`crate::EventRegistry`].
pub struct TypedEventProcessor<E: Indexable> {
    type_str: String,
    storer: Arc<dyn Storer<E>>,
    handlers: Vec<Arc<dyn EventHandler<E>>>,
    migrations_fn: Option<fn() -> EmbeddedMigrations>,
    _marker: PhantomData<fn() -> E>,
}

impl<E: Indexable> TypedEventProcessor<E> {
    /// Build a processor bound to `type_str` — the fully-qualified Move
    /// struct tag to match on, e.g.
    /// `format!("{addr}::counter::CounterIncrementedEvent")`.
    pub fn new<S: Storer<E>>(type_str: impl Into<String>, storer: S) -> Self {
        Self {
            type_str: type_str.into(),
            storer: Arc::new(storer),
            handlers: Vec::new(),
            migrations_fn: None,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub fn with_handler<H: EventHandler<E>>(mut self, handler: H) -> Self {
        self.handlers.push(Arc::new(handler));
        self
    }

    /// Attach a migrations provider. Takes a function pointer (usually a
    /// free function returning a `const EmbeddedMigrations`) because
    /// [`EmbeddedMigrations`] is not `Clone`/`Copy` — storing a closure lets
    /// us re-materialize it when the runner applies migrations.
    #[must_use]
    pub fn with_migrations(mut self, migrations: fn() -> EmbeddedMigrations) -> Self {
        self.migrations_fn = Some(migrations);
        self
    }

    /// Downcast a slice of [`ParsedItem`] into the typed pairs expected by
    /// handlers and the storer.
    fn downcast_items(items: &[ParsedItem]) -> Result<Vec<(&E, EventContext)>> {
        items
            .iter()
            .map(|item| {
                let typed = item
                    .parsed
                    .downcast_ref::<E>()
                    .ok_or_else(|| anyhow!("TypedEventProcessor<{}>: downcast failed — registry routed mismatched type", E::NAME))?;
                Ok((typed, item.ctx.clone()))
            })
            .collect()
    }

    /// Drive a single handler to completion using the handler's configured
    /// [`RetryPolicy`](crate::RetryPolicy), converted to a
    /// [`backon::ExponentialBuilder`]. Final failure is logged + swallowed
    /// (handlers are best-effort).
    async fn run_handler(handler: &Arc<dyn EventHandler<E>>, batch: &[(E, EventContext)]) {
        let policy = handler.retry_policy();
        let backoff = policy.to_backoff();
        let handler_name = handler.name();

        let outcome = (|| async { handler.handle(batch).await })
            .retry(backoff)
            .notify(|err: &anyhow::Error, dur: Duration| {
                warn!(
                    handler = handler_name,
                    retry_after_ms = dur.as_millis() as u64,
                    error = %err,
                    "handler failed, retrying"
                );
            })
            .await;

        if let Err(err) = outcome {
            error!(
                handler = handler_name,
                max_attempts = policy.max_attempts,
                error = %err,
                "handler giving up; batch dropped"
            );
        }
    }
}

#[async_trait]
impl<E: Indexable> EventProcessor for TypedEventProcessor<E> {
    fn type_str(&self) -> &str {
        &self.type_str
    }

    fn name(&self) -> &'static str {
        E::NAME
    }

    fn parse(&self, event: &Event, ctx: &EventContext) -> Result<Box<dyn Any + Send + Sync>> {
        let parsed =
            E::from_event(event, ctx).with_context(|| format!("parsing {}", self.type_str))?;
        Ok(Box::new(parsed))
    }

    async fn store(&self, pool: &ArcDbPool, items: &[ParsedItem]) -> Result<()> {
        let typed = Self::downcast_items(items)?;
        let owned: Vec<(E, EventContext)> =
            typed.into_iter().map(|(e, ctx)| (e.clone(), ctx)).collect();
        self.storer.store(pool, &owned).await
    }

    async fn handle(&self, items: &[ParsedItem]) -> Result<()> {
        if self.handlers.is_empty() {
            return Ok(());
        }
        let typed = Self::downcast_items(items)?;
        let owned: Vec<(E, EventContext)> =
            typed.into_iter().map(|(e, ctx)| (e.clone(), ctx)).collect();
        for handler in &self.handlers {
            Self::run_handler(handler, &owned).await;
        }
        Ok(())
    }

    fn migrations(&self) -> Option<EmbeddedMigrations> {
        self.migrations_fn.map(|f| f())
    }
}
