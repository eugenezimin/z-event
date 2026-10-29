//! orders → billing as two processes.
//!
//! - `orders` keeps orders in PostgreSQL (schema `orders`) and its z-event
//!   tables in schema `orders_zevent` of the same database. Each
//!   confirmation writes the order row and an `OrderConfirmed` event in one
//!   transaction, begun on the service's own pool, and the feed is served
//!   over gRPC.
//! - `billing` keeps invoices in schema `billing` and its cursor in
//!   `billing_zevent`. It subscribes to the orders feed over gRPC and writes
//!   one invoice per confirmed order, in the same transaction as its cursor.
//!
//! Each service gets a small second pool for z-event's background work
//! (sequencer, feed reads, acks, retention, applying events), so it never
//! competes with the request path for connections.
//!
//! ```text
//! DATABASE_URL=postgres://… z-event-example-split orders  --listen 127.0.0.1:50051 --count 20
//! DATABASE_URL=postgres://… z-event-example-split billing --orders http://127.0.0.1:50051 --until 20
//! ```
//!
//! billing authenticates with a bearer token. For mutual TLS, pass
//! `ServerTls` to `serve_standalone_with_shutdown`, `ClientTls` to
//! `Client::connect`, and use `MtlsIdentity`.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres};
use z_event::{
    AllowAll, BoxError, Event, Handler, HandlerError, NewEvent, Producer, ProducerConfig,
    Subscriber,
};
use z_event_grpc::{Client, TokenIdentity, serve_standalone_with_shutdown};
use z_event_postgres::PgStore;

const TOKEN: &str = "billing-demo-token";

#[derive(Serialize, Deserialize)]
struct OrderConfirmed {
    order_id: String,
    amount_cents: i64,
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

async fn pool(max_connections: u32) -> Result<PgPool, BoxError> {
    let url = std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL")?;
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await?)
}

/// The producer: confirm `count` orders, serve the feed until Ctrl-C.
async fn orders(listen: &str, count: u64) -> Result<(), BoxError> {
    let app = pool(10).await?;
    let store = z_event_postgres::migrate(&pool(4).await?, "orders_zevent").await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS orders")
        .execute(&app)
        .await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS orders.orders (id text PRIMARY KEY, amount_cents bigint NOT NULL)")
        .execute(&app)
        .await?;
    let producer = Producer::new(store.clone(), ProducerConfig::new("orders"));
    let running = producer.start();

    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!("orders: serving the feed on {}", listener.local_addr()?);
    let server = tokio::spawn(serve_standalone_with_shutdown(
        producer.feed_source(AllowAll),
        TokenIdentity::new().token(TOKEN, "billing"),
        listener,
        None,
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    ));

    for i in 1..=count {
        let id = format!("o{i}");
        let amount = i64::try_from(i)? * 1000;
        // The service's own pool; the append joins its transaction.
        let mut tx = app.begin().await?;
        sqlx::query("INSERT INTO orders.orders (id, amount_cents) VALUES ($1, $2)")
            .bind(&id)
            .bind(amount)
            .execute(&mut *tx)
            .await?;
        let event = NewEvent::json(
            "OrderConfirmed",
            format!("orders/{id}"),
            1,
            &OrderConfirmed {
                order_id: id,
                amount_cents: amount,
            },
        )?
        .correlation(format!("req-{i}"));
        producer.append(&mut tx, event).await?;
        tx.commit().await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    println!("orders: confirmed {count} orders");
    server.await??;
    running.shutdown().await;
    Ok(())
}

/// billing's handler: the invoice and the cursor commit together.
struct CreateInvoice;

impl Handler<PgStore> for CreateInvoice {
    async fn handle(
        &self,
        tx: &mut sqlx::Transaction<'static, Postgres>,
        ev: &Event,
    ) -> Result<(), HandlerError> {
        let order: OrderConfirmed = ev.json().map_err(HandlerError::permanent)?;
        sqlx::query(
            "INSERT INTO billing.invoices (order_id, amount_cents, event_id) VALUES ($1, $2, $3)",
        )
        .bind(order.order_id)
        .bind(order.amount_cents)
        .bind(ev.id.to_string())
        .execute(&mut **tx)
        .await
        .map_err(HandlerError::transient)?;
        Ok(())
    }
}

/// The consumer: invoice confirmed orders until `until` invoices exist.
async fn billing(orders_url: &str, until: i64) -> Result<(), BoxError> {
    let app = pool(10).await?;
    // The subscriber applies events through this pool: handler writes and
    // cursor share its transactions.
    let store = z_event_postgres::migrate(&pool(4).await?, "billing_zevent").await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS billing")
        .execute(&app)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS billing.invoices \
         (order_id text PRIMARY KEY, amount_cents bigint NOT NULL, event_id text NOT NULL)",
    )
    .execute(&app)
    .await?;

    // Wait for the orders service to come up.
    let client = loop {
        match Client::connect(orders_url.to_owned(), None).await {
            Ok(client) => break client.bearer_token(TOKEN)?,
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };
    let subscriber = Subscriber::builder(store, client, CreateInvoice)
        .consumer("billing")
        .producer("orders")
        .types(["OrderConfirmed"])
        .spawn();

    loop {
        let invoices: i64 = sqlx::query_scalar("SELECT count(*) FROM billing.invoices")
            .fetch_one(&app)
            .await?;
        if invoices >= until {
            println!("billing: {invoices} invoices");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    subscriber.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("orders") => {
            let listen = arg(&args, "--listen").unwrap_or_else(|| "127.0.0.1:50051".into());
            let count = arg(&args, "--count").map_or(Ok(20), |c| c.parse())?;
            orders(&listen, count).await
        }
        Some("billing") => {
            let url = arg(&args, "--orders").unwrap_or_else(|| "http://127.0.0.1:50051".into());
            let until = arg(&args, "--until").map_or(Ok(20), |c| c.parse())?;
            billing(&url, until).await
        }
        _ => Err("usage: z-event-example-split (orders --listen ADDR --count N | billing --orders URL --until N)".into()),
    }
}
