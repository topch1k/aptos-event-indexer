//! # aptos-event-indexer
//!
//! A generic, registry-based indexer library for Aptos on-chain events.
//!
//! A single pipeline subscribes to the Aptos Transaction Stream once and
//! dispatches every event to the [`EventProcessor`](traits::EventProcessor)
//! whose [`EventProcessor::type_str`](traits::EventProcessor::type_str) matches.
//! Each processor is responsible for parsing, Postgres persistence, and
//! optional custom side-effects (Kafka / Service Bus / HTTP / …).
//!
//! ## Tracing
//!
//! Every pipeline step is instrumented with `tracing` spans — `event_indexer`
//! (info) at the top, per-batch `dispatch_batch` / `store_batch` /
//! `handle_batch` (debug) for each stream batch, and per-processor child
//! spans `processor_store` / `typed_store` / `typed_handle` / `run_handler`
//! carrying the processor name, fully-qualified Move type, and batch size.
//! Consumers initialize their own `tracing_subscriber`; see the
//! `counter-indexer` example.
//!
//! See the `counter-indexer` example for end-to-end usage.

pub mod config;
pub mod context;
pub mod registry;
pub mod runner;
pub mod steps;
pub mod traits;

// Re-export commonly needed SDK types so users don't have to track the git dep
// independently.
pub use aptos_indexer_processor_sdk::{
    self as sdk, aptos_indexer_transaction_stream,
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    aptos_protos,
    postgres::utils::database::{execute_in_chunks, new_db_pool, ArcDbPool, MAX_DIESEL_PARAM_SIZE},
    utils::errors::ProcessorError,
};

pub use config::{DbConfig, IndexerConfig, RunMode};
pub use context::EventContext;
pub use registry::{EventRegistry, EventRegistryBuilder, ProcessorId};
pub use runner::EventIndexer;
pub use traits::{
    event_handler::EventHandler,
    event_processor::{ArcEventProcessor, EventProcessor, ParsedItem},
    indexable::Indexable,
    retry::RetryPolicy,
    storer::Storer,
    typed_processor::TypedEventProcessor,
};
