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
# export COUNTER_MODULE_ADDR=0x<counter module address>
# export GREETER_MODULE_ADDR=0x<greeter module address>
just run-example
```

## Local end-to-end testing (localnet)

For a fully offline loop — Aptos node + faucet + indexer gRPC + Postgres +
your indexer — use the bundled localnet recipes. You need the `aptos` CLI
and Docker installed.

```sh
# Terminal 1 — Aptos localnet with faucet + indexer gRPC (:50051).
just localnet

# Terminal 2 — everything else.
just localnet-init             # one-time: creates CLI profile `local`
just localnet-init-greeter     # one-time: creates CLI profile `local_greeter`
just up                        # start Postgres
just contract-publish-local    # publishes counter under `local`'s address
just greeter-publish-local     # publishes greeter under `local_greeter`'s address
```

Each publish prints the address the module was deployed to. You do **not**
need to edit any Rust source — the `just run-example-local` recipe
auto-injects both `COUNTER_MODULE_ADDR` and `GREETER_MODULE_ADDR` from
their respective CLI profiles, so the indexer picks up whichever addresses
the contracts were most recently published to. Skipping
`localnet-init-greeter` is fine: the recipe silently reuses the counter
address and the greeter processor simply never matches anything.

Run the indexer against the localnet config:

```sh
just run-example-local         # uses examples/counter-indexer/config.localnet.yaml
```

Generate some events in a third terminal:

```sh
just counter-init              # one-time per account
just by=5 counter-increment 
just by=2 counter-decrement 
just msg="hi" greeter-hello 
```

Within a few seconds you should see `CounterIncremented` / `CounterDecremented` /
`Greeted` log lines from the `StdoutLoggerHandler`, and rows in the
`counter_incremented_events` / `counter_decremented_events` / `greeted_events`
tables:

```sh
psql postgresql://indexer:indexer@localhost:5432/indexer \
  -c 'select transaction_version, account, old_value, new_value, increment_by
        from counter_incremented_events order by transaction_version desc limit 5;'
psql postgresql://indexer:indexer@localhost:5432/indexer \
  -c 'select transaction_version, who, message
        from greeted_events order by transaction_version desc limit 5;'
```

To reset the local state, stop terminal 1 (Ctrl-C), then `just clean` to
wipe the Postgres volume. Restarting `just localnet` gives you a fresh
chain.

### Multiple modules at different addresses

The `EventRegistry` dispatches purely on the `type_str` each processor
registers, so a single indexer can consume events from any number of Move
modules published at any number of addresses. The bundled example
demonstrates this by running one indexer over two independently-published
modules — `counter` (under the `local` CLI profile) and `greeter` (under
`local_greeter`):

```rust
let counter_addr = std::env::var("COUNTER_MODULE_ADDR")?;
let greeter_addr = std::env::var("GREETER_MODULE_ADDR")?;

let registry = EventRegistry::builder()
    .register(
        TypedEventProcessor::<CounterIncrementedEvent>::new(
            format!("{counter_addr}::counter::CounterIncrementedEvent"),
            IncrementedStorer,
        )
        .with_migrations(counter_migrations),
    )
    .register(TypedEventProcessor::<CounterDecrementedEvent>::new(
        format!("{counter_addr}::counter::CounterDecrementedEvent"),
        DecrementedStorer,
    ))
    .register(TypedEventProcessor::<GreetedEvent>::new(
        format!("{greeter_addr}::greeter::GreetedEvent"),
        GreeterStorer,
    ))
    .build();
```

Scale out by adding more `.register(...)` calls — the fully-qualified type
string each processor takes at registration time is the only coupling to a
specific on-chain deployment, so the same binary can follow localnet /
testnet / mainnet by just changing env vars.

### Testnet instead of localnet

Skip the `localnet*` recipes and use `just contract-publish` with an
`aptos init --network testnet` profile, fill in `auth_token` in
`config.testnet.yaml`, and run `just run-example`.

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
