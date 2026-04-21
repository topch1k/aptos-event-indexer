mod events;
mod handlers;
mod schema;

use std::path::PathBuf;

use anyhow::{Context, Result};
use aptos_event_indexer::{EventIndexer, EventRegistry, IndexerConfig, TypedEventProcessor};
use clap::Parser;
use diesel_migrations::{EmbeddedMigrations, embed_migrations};

use crate::events::{
    CounterDecrementedEvent, CounterIncrementedEvent, DecrementedStorer, IncrementedStorer,
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

    // Both processors share the same migration set; attach it to one of them
    // — the runner applies each processor's migrations, so attaching twice
    // is harmless but redundant.
    let registry = EventRegistry::builder()
        .register(
            TypedEventProcessor::<CounterIncrementedEvent>::new(IncrementedStorer)
                .with_handler(StdoutLoggerHandler)
                .with_migrations(counter_migrations),
        )
        .register(
            TypedEventProcessor::<CounterDecrementedEvent>::new(DecrementedStorer)
                .with_handler(StdoutLoggerHandler),
        )
        .build();

    EventIndexer::new(config, registry).run().await
}
