//! In-process tests for [`TypedEventProcessor::handle`] — focuses on
//! downcasting, per-handler retry via [`RetryPolicy`], and best-effort
//! error swallowing. The `store` path needs a real Postgres pool and is
//! covered by `postgres_integration.rs`.

use std::any::Any;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use aptos_event_indexer::{
    EventContext, EventHandler, EventProcessor, Indexable, ParsedItem, RetryPolicy, Storer,
    TypedEventProcessor,
};
use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;
use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
use async_trait::async_trait;
use mockall::mock;
use rstest::rstest;

// ---------- Test event type ----------

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestEvent {
    id: u64,
}

impl Indexable for TestEvent {
    const NAME: &'static str = "test_event";
    fn from_event(_event: &Event, _ctx: &EventContext) -> Result<Self> {
        Ok(Self { id: 0 })
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

fn parsed(ev: TestEvent, v: u64, i: u64) -> ParsedItem {
    ParsedItem::new(ev, ctx(v, i))
}

// ---------- Mocks ----------

mock! {
    pub StorerImpl {}
    #[async_trait]
    impl Storer<TestEvent> for StorerImpl {
        async fn store(
            &self,
            pool: &ArcDbPool,
            items: &[(TestEvent, EventContext)],
        ) -> Result<()>;
    }
}

mock! {
    pub HandlerImpl {}
    #[async_trait]
    impl EventHandler<TestEvent> for HandlerImpl {
        fn name(&self) -> &'static str;
        async fn handle(&self, batch: &[(TestEvent, EventContext)]) -> Result<()>;
        fn retry_policy(&self) -> RetryPolicy;
    }
}

// ---------- Tests ----------

#[tokio::test]
async fn type_str_returned_as_supplied() {
    let p = TypedEventProcessor::new("0xabc::foo::Bar", MockStorerImpl::new());
    assert_eq!(p.type_str(), "0xabc::foo::Bar");
    assert_eq!(p.name(), TestEvent::NAME);
}

#[tokio::test]
async fn handle_is_noop_when_no_handlers_registered() {
    let storer = MockStorerImpl::new(); // no expectations — must not be called
    let p = TypedEventProcessor::<TestEvent>::new("0x1::m::E", storer);

    let items = vec![parsed(TestEvent { id: 1 }, 10, 0)];
    p.handle(&items).await.expect("handle should be ok");
}

#[tokio::test]
async fn handle_invokes_every_registered_handler_once() {
    let mut h1 = MockHandlerImpl::new();
    h1.expect_name().return_const("h1");
    h1.expect_retry_policy().returning(RetryPolicy::default);
    h1.expect_handle().times(1).returning(|batch| {
        assert_eq!(batch.len(), 2);
        Ok(())
    });

    let mut h2 = MockHandlerImpl::new();
    h2.expect_name().return_const("h2");
    h2.expect_retry_policy().returning(RetryPolicy::default);
    h2.expect_handle().times(1).returning(|_| Ok(()));

    let p = TypedEventProcessor::<TestEvent>::new("0x1::m::E", MockStorerImpl::new())
        .with_handler(h1)
        .with_handler(h2);

    let items = vec![
        parsed(TestEvent { id: 1 }, 10, 0),
        parsed(TestEvent { id: 2 }, 10, 1),
    ];
    p.handle(&items).await.unwrap();
}

#[tokio::test]
async fn handler_retries_then_succeeds() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();

    let mut h = MockHandlerImpl::new();
    h.expect_name().return_const("flaky");
    h.expect_retry_policy().returning(|| RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
    });
    h.expect_handle().returning(move |_| {
        let n = a.fetch_add(1, Ordering::SeqCst) + 1;
        if n < 2 {
            Err(anyhow::anyhow!("transient"))
        } else {
            Ok(())
        }
    });

    let p =
        TypedEventProcessor::<TestEvent>::new("0x1::m::E", MockStorerImpl::new()).with_handler(h);
    p.handle(&[parsed(TestEvent { id: 1 }, 10, 0)])
        .await
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn handler_giving_up_is_swallowed() {
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();

    let mut h = MockHandlerImpl::new();
    h.expect_name().return_const("always_fails");
    h.expect_retry_policy().returning(|| RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
    });
    h.expect_handle().returning(move |_| {
        a.fetch_add(1, Ordering::SeqCst);
        Err(anyhow::anyhow!("nope"))
    });

    let p =
        TypedEventProcessor::<TestEvent>::new("0x1::m::E", MockStorerImpl::new()).with_handler(h);

    // Must NOT propagate the error — handlers are best-effort.
    p.handle(&[parsed(TestEvent { id: 1 }, 10, 0)])
        .await
        .unwrap();
    // 1 initial attempt + 2 retries.
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn handle_returns_err_when_items_are_of_wrong_type() {
    let p =
        TypedEventProcessor::<TestEvent>::new("0x1::m::E", MockStorerImpl::new()).with_handler({
            let mut h = MockHandlerImpl::new();
            h.expect_handle().never();
            h.expect_name().return_const("h");
            h.expect_retry_policy().returning(RetryPolicy::default);
            h
        });

    // Wrong payload type — downcast must fail.
    let wrong = ParsedItem {
        parsed: Box::new(String::from("not a TestEvent")) as Box<dyn Any + Send + Sync>,
        ctx: ctx(1, 0),
    };
    let err = p.handle(&[wrong]).await.expect_err("downcast should fail");
    assert!(
        format!("{err:#}").contains("downcast failed"),
        "unexpected error: {err:#}"
    );
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(5)]
#[tokio::test]
async fn handle_preserves_batch_size_passed_to_handler(#[case] n: usize) {
    let mut h = MockHandlerImpl::new();
    h.expect_name().return_const("size_check");
    h.expect_retry_policy().returning(RetryPolicy::default);
    let expected = n;
    h.expect_handle().returning(move |batch| {
        assert_eq!(batch.len(), expected);
        Ok(())
    });
    // With no handlers at all, handle short-circuits. Ensure at least one
    // handler is registered so downcasting & batch plumbing run.

    let p =
        TypedEventProcessor::<TestEvent>::new("0x1::m::E", MockStorerImpl::new()).with_handler(h);

    let items: Vec<_> = (0..n)
        .map(|i| parsed(TestEvent { id: i as u64 }, 10, i as u64))
        .collect();
    p.handle(&items).await.unwrap();
}
