use anyhow::{Context, Result};
use aptos_event_indexer::aptos_protos::transaction::v1::Event as EventPb;
use aptos_event_indexer::ArcDbPool;
use aptos_event_indexer::{EventContext, Indexable, Storer};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::prelude::Insertable;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::Deserialize;

use crate::schema::{counter_decremented_events, counter_incremented_events, greeted_events};

/// Move `u64` fields are JSON-encoded as decimal strings.
fn parse_u64(s: &str) -> Result<u64> {
    s.parse::<u64>()
        .with_context(|| format!("parsing u64 from `{s}`"))
}

// ─────────────────────────── Incremented ───────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct IncrementedPayload {
    account: String,
    old_value: String,
    new_value: String,
    increment_by: String,
}

#[derive(Debug, Clone)]
pub struct CounterIncrementedEvent {
    pub account: String,
    pub old_value: u64,
    pub new_value: u64,
    pub increment_by: u64,
}

impl Indexable for CounterIncrementedEvent {
    const NAME: &'static str = "counter_incremented";

    fn from_event(event: &EventPb, _ctx: &EventContext) -> Result<Self> {
        let p: IncrementedPayload = serde_json::from_str(&event.data)
            .with_context(|| format!("parsing CounterIncrementedEvent from {}", event.data))?;
        Ok(Self {
            account: p.account,
            old_value: parse_u64(&p.old_value)?,
            new_value: parse_u64(&p.new_value)?,
            increment_by: parse_u64(&p.increment_by)?,
        })
    }
}

#[derive(Debug, Insertable, FieldCount)]
#[diesel(table_name = counter_incremented_events)]
struct IncrementedRow {
    transaction_version: i64,
    event_index: i64,
    transaction_timestamp: Option<NaiveDateTime>,
    account: String,
    old_value: i64,
    new_value: i64,
    increment_by: i64,
}

pub struct IncrementedStorer;

#[async_trait]
impl Storer<CounterIncrementedEvent> for IncrementedStorer {
    async fn store(
        &self,
        pool: &ArcDbPool,
        items: &[(CounterIncrementedEvent, EventContext)],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let rows: Vec<IncrementedRow> = items
            .iter()
            .map(|(e, ctx)| IncrementedRow {
                transaction_version: ctx.transaction_version as i64,
                event_index: ctx.event_index as i64,
                transaction_timestamp: ctx.transaction_timestamp,
                account: e.account.clone(),
                old_value: e.old_value as i64,
                new_value: e.new_value as i64,
                increment_by: e.increment_by as i64,
            })
            .collect();

        let mut conn = pool.get().await.context("checkout conn")?;
        diesel::insert_into(counter_incremented_events::table)
            .values(&rows)
            .on_conflict((
                counter_incremented_events::transaction_version,
                counter_incremented_events::event_index,
            ))
            .do_nothing()
            .execute(&mut conn)
            .await
            .context("insert counter_incremented_events")?;
        Ok(())
    }
}

// ─────────────────────────── Decremented ───────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct DecrementedPayload {
    account: String,
    old_value: String,
    new_value: String,
    decrement_by: String,
}

#[derive(Debug, Clone)]
pub struct CounterDecrementedEvent {
    pub account: String,
    pub old_value: u64,
    pub new_value: u64,
    pub decrement_by: u64,
}

impl Indexable for CounterDecrementedEvent {
    const NAME: &'static str = "counter_decremented";

    fn from_event(event: &EventPb, _ctx: &EventContext) -> Result<Self> {
        let p: DecrementedPayload = serde_json::from_str(&event.data)
            .with_context(|| format!("parsing CounterDecrementedEvent from {}", event.data))?;
        Ok(Self {
            account: p.account,
            old_value: parse_u64(&p.old_value)?,
            new_value: parse_u64(&p.new_value)?,
            decrement_by: parse_u64(&p.decrement_by)?,
        })
    }
}

#[derive(Debug, Insertable, FieldCount)]
#[diesel(table_name = counter_decremented_events)]
struct DecrementedRow {
    transaction_version: i64,
    event_index: i64,
    transaction_timestamp: Option<NaiveDateTime>,
    account: String,
    old_value: i64,
    new_value: i64,
    decrement_by: i64,
}

pub struct DecrementedStorer;

#[async_trait]
impl Storer<CounterDecrementedEvent> for DecrementedStorer {
    async fn store(
        &self,
        pool: &ArcDbPool,
        items: &[(CounterDecrementedEvent, EventContext)],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let rows: Vec<DecrementedRow> = items
            .iter()
            .map(|(e, ctx)| DecrementedRow {
                transaction_version: ctx.transaction_version as i64,
                event_index: ctx.event_index as i64,
                transaction_timestamp: ctx.transaction_timestamp,
                account: e.account.clone(),
                old_value: e.old_value as i64,
                new_value: e.new_value as i64,
                decrement_by: e.decrement_by as i64,
            })
            .collect();

        let mut conn = pool.get().await.context("checkout conn")?;
        diesel::insert_into(counter_decremented_events::table)
            .values(&rows)
            .on_conflict((
                counter_decremented_events::transaction_version,
                counter_decremented_events::event_index,
            ))
            .do_nothing()
            .execute(&mut conn)
            .await
            .context("insert counter_decremented_events")?;
        Ok(())
    }
}

// ─────────────────────────── Greeted ───────────────────────────
//
// From a *different* Move module published at a *different* on-chain
// address. Registered alongside the counter events to demonstrate that one
// `EventRegistry` can fan out across modules and addresses.

#[derive(Debug, Clone, Deserialize)]
struct GreetedPayload {
    who: String,
    message: String,
}

#[derive(Debug, Clone)]
pub struct GreetedEvent {
    pub who: String,
    pub message: String,
}

impl Indexable for GreetedEvent {
    const NAME: &'static str = "greeted";

    fn from_event(event: &EventPb, _ctx: &EventContext) -> Result<Self> {
        let p: GreetedPayload = serde_json::from_str(&event.data)
            .with_context(|| format!("parsing GreetedEvent from {}", event.data))?;
        Ok(Self {
            who: p.who,
            message: p.message,
        })
    }
}

#[derive(Debug, Insertable, FieldCount)]
#[diesel(table_name = greeted_events)]
struct GreetedRow {
    transaction_version: i64,
    event_index: i64,
    transaction_timestamp: Option<NaiveDateTime>,
    who: String,
    message: String,
}

pub struct GreeterStorer;

#[async_trait]
impl Storer<GreetedEvent> for GreeterStorer {
    async fn store(
        &self,
        pool: &ArcDbPool,
        items: &[(GreetedEvent, EventContext)],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let rows: Vec<GreetedRow> = items
            .iter()
            .map(|(e, ctx)| GreetedRow {
                transaction_version: ctx.transaction_version as i64,
                event_index: ctx.event_index as i64,
                transaction_timestamp: ctx.transaction_timestamp,
                who: e.who.clone(),
                message: e.message.clone(),
            })
            .collect();

        let mut conn = pool.get().await.context("checkout conn")?;
        diesel::insert_into(greeted_events::table)
            .values(&rows)
            .on_conflict((
                greeted_events::transaction_version,
                greeted_events::event_index,
            ))
            .do_nothing()
            .execute(&mut conn)
            .await
            .context("insert greeted_events")?;
        Ok(())
    }
}
