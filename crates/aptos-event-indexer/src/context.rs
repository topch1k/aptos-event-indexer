use chrono::NaiveDateTime;

/// Context attached to every parsed event.
///
/// Carries enough information for storers and handlers to key and order rows
/// deterministically: `(transaction_version, event_index)` is unique per event.
#[derive(Debug, Clone)]
pub struct EventContext {
    pub transaction_version: u64,
    pub event_index: u64,
    pub sequence_number: u64,
    pub transaction_timestamp: Option<NaiveDateTime>,
    pub account_address: Option<String>,
    pub creation_number: Option<u64>,
}
