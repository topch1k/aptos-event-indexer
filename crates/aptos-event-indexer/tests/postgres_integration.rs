//! End-to-end integration test: spin up Postgres via testcontainers, build
//! a real [`ArcDbPool`], and exercise [`TypedEventProcessor::store`] through
//! a user-supplied [`Storer`] impl.
//!
//! These tests require Docker; skip with `--skip postgres` on machines that
//! don't have it running.

use std::sync::Arc;

use anyhow::Result;
use aptos_event_indexer::{
    new_db_pool, ArcDbPool, EventContext, EventProcessor, Indexable, ParsedItem, Storer,
    TypedEventProcessor,
};
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;
use async_trait::async_trait;
use diesel::sql_query;
use diesel_async::RunQueryDsl;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;

/// Tiny `Indexable` that carries an id we can assert on in the DB.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CounterBumped {
    id: i64,
}

impl Indexable for CounterBumped {
    const NAME: &'static str = "counter_bumped";
    fn from_event(_event: &Event, _ctx: &EventContext) -> Result<Self> {
        Ok(Self { id: 0 })
    }
}

/// In-DB storer: persists the id + version into a dedicated table.
struct SqlStorer;

#[async_trait]
impl Storer<CounterBumped> for SqlStorer {
    async fn store(&self, pool: &ArcDbPool, items: &[(CounterBumped, EventContext)]) -> Result<()> {
        use diesel::sql_types::BigInt;
        let mut conn = pool.get().await?;
        for (ev, ctx) in items {
            diesel::sql_query("INSERT INTO test_counter_bumped (id, version) VALUES ($1, $2)")
                .bind::<BigInt, _>(ev.id)
                .bind::<BigInt, _>(ctx.transaction_version as i64)
                .execute(&mut conn)
                .await?;
        }
        Ok(())
    }
}

/// Always-fails storer — used to prove that store errors bubble up out of
/// `TypedEventProcessor::store`.
struct FailingStorer {
    called: Arc<Mutex<u32>>,
}

#[async_trait]
impl Storer<CounterBumped> for FailingStorer {
    async fn store(
        &self,
        _pool: &ArcDbPool,
        _items: &[(CounterBumped, EventContext)],
    ) -> Result<()> {
        *self.called.lock().await += 1;
        Err(anyhow::anyhow!("persistent storage outage"))
    }
}

fn ctx(v: u64, i: u64) -> EventContext {
    EventContext {
        transaction_version: v,
        event_index: i,
        sequence_number: 0,
        transaction_timestamp: None,
        account_address: None,
        creation_number: None,
    }
}

fn item(id: i64, v: u64, i: u64) -> ParsedItem {
    ParsedItem::new(CounterBumped { id }, ctx(v, i))
}

/// Boots a Postgres container, builds an `ArcDbPool`, creates the test
/// table, and returns (container, pool). The container is kept alive by
/// the caller via `_container` — dropping it stops the DB.
async fn pg_pool() -> (testcontainers::ContainerAsync<Postgres>, ArcDbPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = new_db_pool(&url, Some(4))
        .await
        .expect("build db pool against testcontainer");

    // Create the target table.
    {
        let mut conn = pool.get().await.expect("checkout connection");
        sql_query(
            "CREATE TABLE IF NOT EXISTS test_counter_bumped (
                id      BIGINT NOT NULL,
                version BIGINT NOT NULL
             )",
        )
        .execute(&mut conn)
        .await
        .expect("create table");
    }

    (container, pool)
}

#[tokio::test]
#[cfg_attr(
    not(feature = "docker-tests"),
    ignore = "requires docker; run with --ignored or `cargo test --features docker-tests`"
)]
async fn store_persists_items_via_typed_processor() {
    let (_container, pool) = pg_pool().await;

    let processor = TypedEventProcessor::<CounterBumped>::new("0x1::counter::Bumped", SqlStorer);

    let items = vec![item(1, 100, 0), item(2, 100, 1), item(3, 101, 0)];
    processor
        .store(&pool, &items)
        .await
        .expect("store must succeed against real DB");

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        version: i64,
    }

    let mut conn = pool.get().await.unwrap();
    let rows: Vec<Row> = sql_query("SELECT id, version FROM test_counter_bumped ORDER BY id")
        .load(&mut conn)
        .await
        .expect("select rows");

    assert_eq!(rows.len(), 3);
    assert_eq!((rows[0].id, rows[0].version), (1, 100));
    assert_eq!((rows[1].id, rows[1].version), (2, 100));
    assert_eq!((rows[2].id, rows[2].version), (3, 101));
}

#[tokio::test]
#[cfg_attr(
    not(feature = "docker-tests"),
    ignore = "requires docker; run with --ignored or `cargo test --features docker-tests`"
)]
async fn store_propagates_storer_errors() {
    let (_container, pool) = pg_pool().await;

    let called = Arc::new(Mutex::new(0u32));
    let processor = TypedEventProcessor::<CounterBumped>::new(
        "0x1::counter::Bumped",
        FailingStorer {
            called: called.clone(),
        },
    );

    let err = processor
        .store(&pool, &[item(1, 10, 0)])
        .await
        .expect_err("must propagate storer error");
    assert!(
        format!("{err:#}").contains("persistent storage outage"),
        "unexpected error: {err:#}"
    );
    assert_eq!(*called.lock().await, 1);
}
