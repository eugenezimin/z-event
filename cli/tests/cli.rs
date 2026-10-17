//! Every command, against PostgreSQL (a container, or `ZEVENT_PG_URL`).

#![allow(clippy::unwrap_used)]

use clap::Parser;
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use z_event::{
    CursorKey, CursorStore, DeadLetter, Event, Handler, HandlerError, InProcess, NewEvent,
    OutboxStore, Seq, Store, Subscriber,
};
use z_event_cli::{Cli, CliError, NoReplay, Replay, execute};
use z_event_postgres::PgStore;

struct Db {
    url: String,
    pool: PgPool,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn db() -> Db {
    let (url, container) = match std::env::var("ZEVENT_PG_URL") {
        Ok(url) => (url, None),
        Err(_) => {
            let c = Postgres::default()
                .with_tag("16-alpine")
                .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
                .start()
                .await
                .expect("start a PostgreSQL container (is Docker running?)");
            let port = c.get_host_port_ipv4(5432).await.unwrap();
            (
                format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
                Some(c),
            )
        }
    };
    let pool = PgPool::connect(&url).await.unwrap();
    Db {
        url,
        pool,
        _container: container,
    }
}

fn schema(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("cli_{name}_{nanos:x}")
}

/// Parses `args` like the binary and runs the command.
async fn run<R: Replay>(
    store: &PgStore,
    args: &[&str],
    replay: Option<&R>,
) -> Result<String, CliError> {
    let mut argv = vec![
        "z-event-cli",
        "--database-url",
        "unused",
        "--schema",
        store.schema(),
    ];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).unwrap();
    let mut out = Vec::new();
    execute(store, cli.command, &mut out, replay).await?;
    Ok(String::from_utf8(out).unwrap())
}

async fn ok(store: &PgStore, args: &[&str]) -> String {
    run(store, args, None::<&NoReplay>).await.unwrap()
}

const SUB: &[&str] = &["--consumer", "billing", "--producer", "orders"];

fn with(args: &[&'static str]) -> Vec<&'static str> {
    let mut v = args.to_vec();
    v.extend_from_slice(SUB);
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_side_commands() {
    let db = db().await;
    let orders = z_event_postgres::migrate(&db.pool, &schema("orders"))
        .await
        .unwrap();
    let mut tx = orders.begin().await.unwrap();
    orders
        .append(
            &mut tx,
            &[
                NewEvent::new("T", "o/1", 1, "{}"),
                NewEvent::new("T", "o/2", 1, "{}"),
            ],
        )
        .await
        .unwrap();
    orders.commit(tx).await.unwrap();
    let lease = orders.try_lead().await.unwrap().unwrap();
    orders.sequence_batch(&lease, 10).await.unwrap();
    orders.release(lease).await.unwrap();
    orders.register_consumer("billing", Seq(1)).await.unwrap();

    let head = ok(&orders, &["head"]).await;
    assert!(head.contains("head            2"), "{head}");
    assert!(head.contains("unsequenced     0"), "{head}");

    let list = ok(&orders, &["consumers", "list"]).await;
    assert!(list.starts_with("billing\tacked 1\tat "), "{list}");
    assert!(
        ok(&orders, &["consumers", "remove", "billing"])
            .await
            .contains("removed")
    );
    assert!(
        ok(&orders, &["consumers", "list"])
            .await
            .contains("no registered consumers")
    );

    let before = orders.head().await.unwrap().epoch;
    assert!(
        ok(&orders, &["epoch", "show"])
            .await
            .contains(before.as_str())
    );
    let rotated = ok(&orders, &["epoch", "rotate"]).await;
    let after = orders.head().await.unwrap().epoch;
    assert_ne!(before, after);
    assert!(rotated.contains(after.as_str()), "{rotated}");
}

struct Apply;
impl Handler<PgStore> for Apply {
    async fn handle(
        &self,
        _tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
        _ev: &Event,
    ) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_side_commands() {
    let db = db().await;
    let billing = z_event_postgres::migrate(&db.pool, &schema("billing"))
        .await
        .unwrap();
    let key = CursorKey::new("billing", "orders");

    // cursor show / reset (keep, set and clear the epoch)
    assert!(
        ok(&billing, &with(&["cursor", "show"]))
            .await
            .contains("seq    0")
    );
    ok(
        &billing,
        &with(&["cursor", "reset", "--seq", "5", "--epoch", "e1"]),
    )
    .await;
    let shown = ok(&billing, &with(&["cursor", "show"])).await;
    assert!(
        shown.contains("seq    5") && shown.contains("epoch  e1"),
        "{shown}"
    );
    ok(&billing, &with(&["cursor", "reset", "--seq", "7"])).await;
    assert_eq!(
        billing.cursor(&key).await.unwrap().epoch.unwrap().as_str(),
        "e1"
    );
    ok(
        &billing,
        &with(&["cursor", "reset", "--seq", "0", "--clear-epoch"]),
    )
    .await;
    assert_eq!(
        billing.cursor(&key).await.unwrap(),
        z_event::Cursor::start()
    );

    // parked list / resolve
    let mut tx = billing.begin().await.unwrap();
    for seq in [3u64, 4] {
        let ev = NewEvent::new("OrderConfirmed", "orders/o1", 1, "{}").into_event(Seq(seq));
        let letter = DeadLetter::failed(&key, &ev, &HandlerError::permanent("bad payload"), 1);
        billing.park(&mut tx, &letter).await.unwrap();
    }
    billing.commit(tx).await.unwrap();
    let listed = ok(&billing, &with(&["parked", "list"])).await;
    assert_eq!(listed.lines().count(), 2, "{listed}");
    assert!(
        listed.starts_with("3\torders/o1\tOrderConfirmed\tattempts 1\topen\t"),
        "{listed}"
    );
    assert!(
        ok(&billing, &with(&["parked", "resolve", "--seq", "3"]))
            .await
            .contains("resolved 3")
    );
    assert_eq!(
        ok(&billing, &with(&["parked", "list"]))
            .await
            .lines()
            .count(),
        1
    );
    let all = ok(&billing, &with(&["parked", "list", "--all"])).await;
    assert!(all.contains("resolved"), "{all}");
    let one = ok(
        &billing,
        &with(&["parked", "list", "--all", "--limit", "1"]),
    )
    .await;
    assert_eq!(one.lines().count(), 1);

    // parked replay needs the service's handlers
    let err = run(&billing, &with(&["parked", "replay"]), None::<&NoReplay>)
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::ReplayUnavailable), "{err}");
    let subscriber = Subscriber::builder(billing.clone(), InProcess::new(), Apply)
        .consumer("billing")
        .producer("orders")
        .build();
    let replayed = run(&billing, &with(&["parked", "replay"]), Some(&subscriber))
        .await
        .unwrap();
    assert!(replayed.contains("replayed 1"), "{replayed}");
    assert!(
        ok(&billing, &with(&["parked", "list"]))
            .await
            .contains("no parked events")
    );
    // A service only replays its own subscription.
    let wrong = run(
        &billing,
        &[
            "parked",
            "replay",
            "--consumer",
            "search",
            "--producer",
            "orders",
        ],
        Some(&subscriber),
    )
    .await
    .unwrap_err();
    assert!(matches!(wrong, CliError::Invalid(_)), "{wrong}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_binary_migrates_and_reports() {
    let db = db().await;
    let schema = schema("bin");
    let bin = env!("CARGO_BIN_EXE_z-event-cli");
    let run = |args: &[&str]| {
        std::process::Command::new(bin)
            .args(["--database-url", &db.url, "--schema", &schema])
            .args(args)
            .output()
            .unwrap()
    };
    let migrated = run(&["migrate"]);
    assert!(migrated.status.success(), "{migrated:?}");
    let head = run(&["head"]);
    assert!(
        String::from_utf8_lossy(&head.stdout).contains("head            0"),
        "{head:?}"
    );
    let replay = run(&[
        "parked",
        "replay",
        "--consumer",
        "billing",
        "--producer",
        "orders",
    ]);
    assert!(!replay.status.success());
    assert!(String::from_utf8_lossy(&replay.stderr).contains("replay runs the service's handlers"));
}
