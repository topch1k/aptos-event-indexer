use std::sync::Arc;

use anyhow::{Context, Result};
use aptos_indexer_processor_sdk::{
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    postgres::{
        utils::{
            checkpoint::{
                get_starting_version, PostgresChainIdChecker, PostgresProcessorStatusSaver,
            },
            database::{new_db_pool, run_migrations, ArcDbPool},
        },
        SDK_MIGRATIONS,
    },
    traits::IntoRunnableStep,
    utils::chain_id_check::check_or_update_chain_id,
};
use tracing::{info, instrument};

use crate::config::IndexerConfig;
use crate::registry::EventRegistry;
use crate::steps::{RegistryDispatcherStep, RegistryHandlerStep, RegistryStorerStep};

/// Channel size between pipeline steps. Small because per-item payload is a
/// whole batch.
const CHANNEL_SIZE: usize = 10;

/// Top-level runner that wires the SDK pipeline with the registry-based
/// dispatcher, storer, and handler steps.
pub struct EventIndexer {
    config: IndexerConfig,
    registry: EventRegistry,
}

impl EventIndexer {
    pub fn new(config: IndexerConfig, registry: EventRegistry) -> Self {
        Self { config, registry }
    }

    /// Run until ctrl-c or the stream reaches `request_ending_version`.
    ///
    /// The indexer process is the entire library's entry point: it applies
    /// migrations, verifies the chain id, resolves the starting version, and
    /// drives the SDK's `ProcessorBuilder` to exhaustion.
    #[instrument(
        level = "info",
        name = "event_indexer",
        skip_all,
        fields(
            processor = self.config.processor_name.as_str(),
            status_key = self.config.status_key(),
            mode = ?self.config.mode,
        ),
        err,
    )]
    pub async fn run(self) -> Result<()> {
        assert!(
            !self.registry.is_empty(),
            "EventRegistry is empty — register at least one EventProcessor before calling run()"
        );

        let Self { config, registry } = self;
        let registry = Arc::new(registry);

        info!(processors = registry.len(), "starting indexer");

        // 1. Pool + migrations (library + every registered processor).
        let pool = new_db_pool(&config.db.postgres_connection_string, config.db.pool_size)
            .await
            .context("building Postgres pool")?;
        apply_migrations(&config, &pool, &registry).await;

        // 2. Chain-id sanity check.
        let stream_cfg = config.effective_stream_config();
        let chain_checker = PostgresChainIdChecker::new(pool.clone());
        check_or_update_chain_id(&stream_cfg, &chain_checker).await?;

        // 3. Resolve starting version (respects checkpoint or backfill alias).
        let starting_version =
            get_starting_version(config.status_key(), stream_cfg.clone(), pool.clone())
                .await
                .context("resolving starting version")?;
        let stream_cfg = with_starting_version(stream_cfg, starting_version);
        info!(
            starting_version,
            ending_version = ?stream_cfg.request_ending_version,
            "resolved version range"
        );

        // 4. Wire the pipeline.
        let tx_stream = TransactionStreamStep::new(stream_cfg).await?;
        let dispatcher = RegistryDispatcherStep::new(registry.clone());
        let storer = RegistryStorerStep::new(registry.clone(), pool.clone());
        let handlers = RegistryHandlerStep::new(registry);
        let status_saver = PostgresProcessorStatusSaver::new(config.status_key(), pool.clone());
        let version_tracker =
            VersionTrackerStep::new(status_saver, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS);

        let (_builder, output_receiver) =
            ProcessorBuilder::new_with_inputless_first_step(tx_stream.into_runnable_step())
                .connect_to(dispatcher.into_runnable_step(), CHANNEL_SIZE)
                .connect_to(storer.into_runnable_step(), CHANNEL_SIZE)
                .connect_to(handlers.into_runnable_step(), CHANNEL_SIZE)
                .connect_to(version_tracker.into_runnable_step(), CHANNEL_SIZE)
                .end_and_return_output_receiver(CHANNEL_SIZE);

        // 5. Drain. The VersionTrackerStep's cleanup() flushes the final
        //    checkpoint on shutdown.
        loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    info!("ctrl-c received, shutting down");
                    break;
                }
                msg = output_receiver.recv() => match msg {
                    Ok(batch) => tracing::debug!(
                        start_version = batch.metadata.start_version,
                        end_version = batch.metadata.end_version,
                        "checkpoint batch drained"
                    ),
                    Err(_) => {
                        info!("pipeline ended");
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

async fn apply_migrations(config: &IndexerConfig, pool: &ArcDbPool, registry: &EventRegistry) {
    info!("applying SDK migrations");
    run_migrations(
        config.db.postgres_connection_string.clone(),
        pool.clone(),
        SDK_MIGRATIONS,
    )
    .await;

    for processor in registry.processors() {
        if let Some(m) = processor.migrations() {
            info!(
                processor = processor.name(),
                "applying processor migrations"
            );
            run_migrations(
                config.db.postgres_connection_string.clone(),
                pool.clone(),
                m,
            )
            .await;
        }
    }
}

fn with_starting_version(
    mut cfg: aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig,
    starting_version: u64,
) -> aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig {
    cfg.starting_version = Some(starting_version);
    cfg
}
