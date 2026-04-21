mod events;
mod handlers;
mod schema;

use std::path::PathBuf;

use anyhow::{Context, Result};
use aptos_event_indexer::{EventIndexer, EventRegistry, IndexerConfig, TypedEventProcessor};
use clap::Parser;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};

use crate::events::{
    CounterDecrementedEvent, CounterIncrementedEvent, DecrementedStorer, GreetedEvent,
    GreeterStorer, IncrementedStorer,
};
use crate::handlers::StdoutLoggerHandler;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/");

fn counter_migrations() -> EmbeddedMigrations {
    MIGRATIONS
}

#[derive(Parser)]
struct Args {
    /// Path to the indexer YAML config.
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,aptos_event_indexer=info".into()),
        )
        .init();

    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading config from {}", args.config.display()))?;
    let config: IndexerConfig = serde_yaml::from_str(&raw).context("parsing indexer config")?;

    // Resolve the on-chain module address at runtime so the same binary can
    // point at localnet / testnet / mainnet deployments without recompiling.
    // Each Move module is addressed independently — the counter and greeter
    // modules are published by different accounts, so they can (and in the
    // localnet workflow, do) live at different addresses.
    let counter_addr = std::env::var("COUNTER_MODULE_ADDR")
        .context("COUNTER_MODULE_ADDR env var must be set (e.g. 0xbd38...)")?;
    let greeter_addr = std::env::var("GREETER_MODULE_ADDR")
        .context("GREETER_MODULE_ADDR env var must be set (e.g. 0xab12...)")?;
    let incremented_ts = format!("{counter_addr}::counter::CounterIncrementedEvent");
    let decremented_ts = format!("{counter_addr}::counter::CounterDecrementedEvent");
    let greeted_ts = format!("{greeter_addr}::greeter::GreetedEvent");

    // All processors share the same migration set; attach it to one of them
    // — the runner applies each processor's migrations, so attaching twice
    // is harmless but redundant.
    let registry = EventRegistry::builder()
        .register(
            TypedEventProcessor::<CounterIncrementedEvent>::new(incremented_ts, IncrementedStorer)
                .with_handler(StdoutLoggerHandler)
                .with_migrations(counter_migrations),
        )
        .register(
            TypedEventProcessor::<CounterDecrementedEvent>::new(decremented_ts, DecrementedStorer)
                .with_handler(StdoutLoggerHandler),
        )
        .register(
            TypedEventProcessor::<GreetedEvent>::new(greeted_ts, GreeterStorer)
                .with_handler(StdoutLoggerHandler),
        )
        .build();

    EventIndexer::new(config, registry)
        .run()
        .await
        .map_err(anyhow::Error::from)
}
