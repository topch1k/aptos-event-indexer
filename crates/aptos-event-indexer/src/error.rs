//! Library error type for the indexer runner.
//!
//! User-facing traits ([`Indexable::from_event`](crate::Indexable::from_event),
//! [`Storer::store`](crate::Storer::store),
//! [`EventHandler::handle`](crate::EventHandler::handle)) intentionally keep
//! [`anyhow::Result`] so consumer code can return any error without coupling
//! to this crate. [`IndexerError`] is used only by
//! [`EventIndexer::run`](crate::EventIndexer::run) to surface startup and
//! pipeline-wiring failures with structured variants.

use thiserror::Error;

/// Shorthand `Result` using [`IndexerError`] as the default error.
pub type Result<T, E = IndexerError> = std::result::Result<T, E>;

/// Errors returned by [`EventIndexer::run`](crate::EventIndexer::run).
#[derive(Debug, Error)]
pub enum IndexerError {
    /// No processors were registered before `run()` was invoked.
    #[error("EventRegistry is empty — register at least one EventProcessor before calling run()")]
    EmptyRegistry,

    /// Building the Postgres connection pool failed.
    #[error("failed to build Postgres pool: {0}")]
    PoolBuild(#[source] anyhow::Error),

    /// Verifying or persisting the chain id failed.
    #[error("chain id check failed: {0}")]
    ChainIdCheck(#[source] anyhow::Error),

    /// Resolving the starting version from checkpoint state failed.
    #[error("failed to resolve starting version: {0}")]
    StartingVersion(#[source] anyhow::Error),

    /// Constructing the SDK's transaction-stream step failed.
    #[error("failed to build transaction stream: {0}")]
    TransactionStream(#[source] anyhow::Error),
}
