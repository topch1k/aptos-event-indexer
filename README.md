# aptos-event-indexer

Generic, registry-based indexer library for Aptos on-chain events.

A **single** process subscribes to the Aptos Transaction Stream once and
dispatches every event to the `EventProcessor` whose `type_str` matches. Each
processor parses the event, persists it to Postgres (authoritative, drives the
shared checkpoint), and optionally fans out to user-supplied `EventHandler`s
(Kafka / Service Bus / webhooks / …). Adding a new event type costs one
`Indexable` impl + one migration — no extra gRPC subscription, no extra
checkpoint.

## Workspace

- [`crates/aptos-event-indexer`](crates/aptos-event-indexer) — the library.
- [`examples/counter-contract`](examples/counter-contract) — Move package
  emitting `CounterIncrementedEvent` and `CounterDecrementedEvent`.
- [`examples/counter-indexer`](examples/counter-indexer) — runnable example
  indexing both counter events from a single stream.

## Quick start

```sh
cp .env.example .env   # fill in APTOS_AUTH_TOKEN
just up                # start Postgres
# edit examples/counter-indexer/config.testnet.yaml with your auth token,
# and examples/counter-indexer/src/events.rs with your contract address
just run-example
```

## Library design

### Traits

- **`EventProcessor`** — primary extension point. Owns `type_str`, parsing,
  Postgres persistence, and optional side-effects. Register on the
  `EventRegistry` builder.
- **`Indexable` + `TypedEventProcessor<E>`** — sugar for the one-event /
  one-table case: implement `Indexable::from_event`, a `Storer<E>` impl, and
  attach any number of `EventHandler<E>`s via `.with_handler(...)`.
- **`EventHandler<E>`** — typed side-effect trait. Errors never block
  indexing; default retry is 3 attempts with exp-backoff (100ms → 1s → 5s).

### Pipeline

```
TransactionStreamStep (SDK)                 () -> Vec<Transaction>
  -> RegistryDispatcherStep                 routes by type_str
  -> RegistryStorerStep                     authoritative: parallel DB writes
  -> RegistryHandlerStep                    best-effort side-effects
  -> VersionTrackerStep (SDK)               single processor_status row
```

### Delivery semantics

- **Postgres is authoritative.** Storer failures halt the pipeline; the
  checkpoint never advances past unpersisted data.
- **Handlers are best-effort.** Failures are logged + counted but never stall
  ingestion. Users needing at-least-once delivery should write an outbox row
  inside their `Storer::store` implementation and publish from there.

### Backfill

Run the same binary with `mode: { type: backfill, alias: "…", ending_version: N }`.
The alias becomes the `processor_status` key so the head instance is
untouched and both can run concurrently.

## Building a new indexer

Implement the event struct and storer, then register it:

```rust
use aptos_event_indexer::{EventIndexer, EventRegistry, TypedEventProcessor};

let registry = EventRegistry::builder()
    .register(
        TypedEventProcessor::<MyEvent>::new(MyStorer)
            .with_handler(MyKafkaHandler::new(...))
            .with_migrations(my_migrations),
    )
    .build();

EventIndexer::new(config, registry).run().await?;
```

See `examples/counter-indexer/src/` for a complete reference.
