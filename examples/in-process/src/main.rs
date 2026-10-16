//! orders → billing in one process.
//!
//! - `orders` confirms orders. Each confirmation writes the order and an
//!   `OrderConfirmed` event in **one transaction** (the outbox).
//! - `billing` subscribes to `orders` and creates an invoice per confirmed
//!   order. The invoice and billing's cursor commit in **one transaction**,
//!   so each order is invoiced exactly once.
//!
//! Both services use the in-memory store and talk through the in-process
//! transport. Swap in `z-event-postgres` and `z-event-grpc` to split them
//! into two processes; the business code stays the same.
//!
//! Run with `cargo run -p z-event-example-in-process`.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use z_event::{
    AllowAll, BoxError, CursorKey, CursorStore, Event, Handler, HandlerError, InProcess, NewEvent,
    OutboxStore, Producer, ProducerConfig, Store, Subscriber,
};
use z_event_memory::{MemoryStore, MemoryTx};

/// The event `orders` publishes.
#[derive(Debug, Serialize, Deserialize)]
struct OrderConfirmed {
    order_id: String,
    amount_cents: u64,
}

/// The record `billing` keeps.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Invoice {
    order_id: String,
    amount_cents: u64,
    correlation_id: Option<String>,
}

/// billing's handler: runs in billing's transaction, next to the cursor.
struct CreateInvoice;

impl Handler<MemoryStore> for CreateInvoice {
    async fn handle(&self, tx: &mut MemoryTx, ev: &Event) -> Result<(), HandlerError> {
        let order: OrderConfirmed = ev.json().map_err(HandlerError::permanent)?;
        let invoice = Invoice {
            order_id: order.order_id.clone(),
            amount_cents: order.amount_cents,
            correlation_id: ev.correlation_id.clone(),
        };
        let bytes = serde_json::to_vec(&invoice).map_err(HandlerError::permanent)?;
        tx.put(format!("invoices/{}", order.order_id), bytes);
        Ok(())
    }
}

/// orders' business operation: the change and its event commit together.
async fn confirm_order(
    producer: &Producer<MemoryStore>,
    order_id: &str,
    amount_cents: u64,
    request_id: &str,
) -> Result<(), BoxError> {
    let store = producer.store();
    let mut tx = store.begin().await?;
    let aggregate = format!("orders/{order_id}");
    tx.lock(&aggregate).await; // serialise changes to this order
    tx.put(aggregate.clone(), "confirmed");
    let event = NewEvent::json(
        "OrderConfirmed",
        aggregate,
        1,
        &OrderConfirmed {
            order_id: order_id.to_owned(),
            amount_cents,
        },
    )?
    .correlation(request_id)
    .header("tenant", "acme");
    producer.append(&mut tx, event).await?;
    store.commit(tx).await?;
    Ok(())
}

/// Runs the scenario and returns billing's invoices.
async fn run() -> Result<Vec<Invoice>, BoxError> {
    let orders_db = MemoryStore::new();
    let billing_db = MemoryStore::new();

    // orders: producer, sequencer, and its feed on the in-process transport.
    let producer = Producer::new(orders_db.clone(), ProducerConfig::new("orders"));
    let orders_running = producer.start();
    let transport = InProcess::new();
    transport.register("orders", producer.feed_source(AllowAll));

    // billing: subscribes once; events are pushed as they are sequenced.
    let billing = Subscriber::builder(billing_db.clone(), transport, CreateInvoice)
        .consumer("billing")
        .producer("orders")
        .types(["OrderConfirmed"])
        .spawn();

    for i in 1..=5u64 {
        confirm_order(&producer, &format!("o{i}"), i * 1000, &format!("req-{i}")).await?;
    }

    // Wait until billing has applied everything orders has sequenced.
    let key = CursorKey::new("billing", "orders");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let head = orders_db.head().await?.seq;
            let cursor = billing_db.cursor(&key).await?.seq;
            if head.get() == 5 && cursor == head {
                return Ok::<_, BoxError>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;

    billing.shutdown().await?;
    orders_running.shutdown().await;

    billing_db
        .scan_prefix("invoices/")
        .into_iter()
        .map(|(_, v)| serde_json::from_slice(&v).map_err(Into::into))
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    for invoice in run().await? {
        println!(
            "invoice for {}: {}.{:02} (request {})",
            invoice.order_id,
            invoice.amount_cents / 100,
            invoice.amount_cents % 100,
            invoice.correlation_id.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_order_is_invoiced_once() {
        let invoices = run().await.unwrap();
        assert_eq!(invoices.len(), 5);
        assert_eq!(invoices[0].order_id, "o1");
        assert_eq!(invoices[0].correlation_id.as_deref(), Some("req-1"));
        assert_eq!(invoices[4].amount_cents, 5000);
    }
}
