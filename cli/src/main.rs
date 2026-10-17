//! The `z-event-cli` binary: z-event operations on PostgreSQL.

use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use z_event_cli::{Cli, Command, NoReplay, execute};
use z_event_postgres::PgStore;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&cli.database_url)
        .await?;
    let store = PgStore::new(pool, &cli.schema)?;
    if let Command::Migrate = cli.command {
        store.migrate().await?;
        println!("migrated schema {}", cli.schema);
        return Ok(());
    }
    execute(
        &store,
        cli.command,
        &mut std::io::stdout(),
        None::<&NoReplay>,
    )
    .await?;
    Ok(())
}
