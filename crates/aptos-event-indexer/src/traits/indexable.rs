use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use diesel_migrations::EmbeddedMigrations;
use tracing::{error, warn};

use crate::context::EventContext;
use crate::traits::event_handler::EventHandler;
use crate::traits::event_processor::{EventProcessor, ParsedItem};

/// Sugar trait for the common "one Move event → one Postgres table" case.
///
/// Implement this plus an associated [`Storer`] and you get a ready-made
/// [`EventProcessor`] by wrapping it in [`TypedEventProcessor`].
pub trait Indexable: Sized + Clone + Send + Sync + 'static {
    /// Fully-qualified Move struct tag, e.g.
    /// `"0xMARKET::marketplace::Listed"`.
    const TYPE_STR: &'static str;

    /// Human-readable processor name (e.g. `"marketplace_listed"`).
    const NAME: &'static str;

    /// Parse the raw on-chain event into the typed Rust value.
    fn from_event(event: &Event, ctx: &EventContext) -> Result<Self>;
}

/// Persistence adapter for an [`Indexable`].
///
/// Kept as a separate trait so users can plug diesel schemas without the
/// library forcing a particular table/row representation.
#[async_trait]
pub trait Storer<E: Indexable>: Send + Sync + 'static {
    async fn store(&self, pool: &ArcDbPool, items: &[(E, EventContext)]) -> Result<()>;
}

/// Ready-made [`EventProcessor`] wrapping an [`Indexable`] event, a
/// [`Storer`], and any number of [`EventHandler`]s.
///
/// Build it via [`TypedEventProcessor::new`] then chain `.with_handler(...)`
/// / `.with_migrations(...)` before registering with the
/// [`crate::EventRegistry`].
pub struct TypedEventProcessor<E: Indexable> {
    storer: Arc<dyn Storer<E>>,
    handlers: Vec<Arc<dyn EventHandler<E>>>,
    migrations_fn: Option<fn() -> EmbeddedMigrations>,
    _marker: PhantomData<fn() -> E>,
}

impl<E: Indexable> TypedEventProcessor<E> {
    pub fn new<S: Storer<E>>(storer: S) -> Self {
        Self {
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

    async fn run_handler(handler: &Arc<dyn EventHandler<E>>, batch: &[(E, EventContext)]) {
        let policy = handler.retry_policy();
        let mut attempt: u32 = 0;
        loop {
            match handler.handle(batch).await {
                Ok(()) => return,
                Err(e) if attempt + 1 < policy.max_attempts => {
                    warn!(
                        handler = handler.name(),
                        attempt, error = %e,
                        "handler failed, retrying"
                    );
                    tokio::time::sleep(policy.backoff(attempt)).await;
                    attempt += 1;
                }
                Err(e) => {
                    error!(
                        handler = handler.name(),
                        attempts = attempt + 1,
                        error = %e,
                        "handler giving up; batch dropped"
                    );
                    return;
                }
            }
        }
    }
}

#[async_trait]
impl<E: Indexable> EventProcessor for TypedEventProcessor<E> {
    fn type_str(&self) -> &'static str {
        E::TYPE_STR
    }

    fn name(&self) -> &'static str {
        E::NAME
    }

    fn parse(&self, event: &Event, ctx: &EventContext) -> Result<Box<dyn Any + Send + Sync>> {
        let parsed = E::from_event(event, ctx)
            .with_context(|| format!("parsing {}", E::TYPE_STR))?;
        Ok(Box::new(parsed))
    }

    async fn store(&self, pool: &ArcDbPool, items: &[ParsedItem]) -> Result<()> {
        let typed = Self::downcast_items(items)?;
        let owned: Vec<(E, EventContext)> = typed
            .into_iter()
            .map(|(e, ctx)| (e.clone(), ctx))
            .collect();
        self.storer.store(pool, &owned).await
    }

    async fn handle(&self, items: &[ParsedItem]) -> Result<()> {
        if self.handlers.is_empty() {
            return Ok(());
        }
        let typed = Self::downcast_items(items)?;
        let owned: Vec<(E, EventContext)> = typed
            .into_iter()
            .map(|(e, ctx)| (e.clone(), ctx))
            .collect();
        for handler in &self.handlers {
            Self::run_handler(handler, &owned).await;
        }
        Ok(())
    }

    fn migrations(&self) -> Option<EmbeddedMigrations> {
        self.migrations_fn.map(|f| f())
    }
}
