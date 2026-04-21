use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
use serde::{Deserialize, Serialize};

/// Top-level YAML config for a single indexer deployment.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexerConfig {
    /// Unique name for this indexer. Used as the key in the SDK's
    /// `processor_status` table.
    pub processor_name: String,
    pub transaction_stream: TransactionStreamConfig,
    pub db: DbConfig,
    #[serde(default)]
    pub mode: RunMode,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DbConfig {
    pub postgres_connection_string: String,
    /// Max connections in the bb8 pool. Defaults to the SDK's
    /// `DEFAULT_MAX_POOL_SIZE` (150) when unset.
    #[serde(default)]
    pub pool_size: Option<u32>,
}

/// Indexer run mode.
///
/// `Head` follows the tip of the chain and advances the main
/// `processor_status` row. `Backfill` scans a bounded version range under a
/// distinct `alias` (its own row in `processor_status`), leaving the head
/// processor untouched so they can run concurrently.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunMode {
    #[default]
    Head,
    Backfill { alias: String, ending_version: u64 },
}

impl IndexerConfig {
    /// Resolve the `processor_status` key (head name or backfill alias).
    pub fn status_key(&self) -> &str {
        match &self.mode {
            RunMode::Head => &self.processor_name,
            RunMode::Backfill { alias, .. } => alias,
        }
    }

    /// Apply mode-specific overrides (e.g. setting `request_ending_version`
    /// for backfill) to the transaction-stream config.
    pub fn effective_stream_config(&self) -> TransactionStreamConfig {
        let mut cfg = self.transaction_stream.clone();
        if let RunMode::Backfill { ending_version, .. } = &self.mode {
            cfg.request_ending_version = Some(*ending_version);
        }
        cfg
    }
}
