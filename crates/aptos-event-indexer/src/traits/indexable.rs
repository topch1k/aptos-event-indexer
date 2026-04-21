use anyhow::Result;
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;

use crate::context::EventContext;

/// Sugar trait for the common "one Move event → one Postgres table" case.
///
/// Implement this plus an associated [`crate::Storer`] and you get a
/// ready-made [`crate::EventProcessor`] by wrapping the pair in
/// [`crate::TypedEventProcessor`].
///
/// The fully-qualified Move type string is supplied at **registration time**
/// via [`crate::TypedEventProcessor::new`] — it is intentionally not a
/// trait constant so the same Rust type can be bound to different on-chain
/// deployments (localnet / testnet / mainnet) without recompiling.
pub trait Indexable: Sized + Clone + Send + Sync + 'static {
    /// Human-readable processor name (e.g. `"marketplace_listed"`).
    const NAME: &'static str;

    /// Parse the raw on-chain event into the typed Rust value.
    fn from_event(event: &Event, ctx: &EventContext) -> Result<Self>;
}
