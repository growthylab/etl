use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use pg_escape::quote_identifier;
use rand::Rng;
use sqlx::{AssertSqlSafe, PgPool};

use crate::ducklake::DuckLakeTableName;

/// Replay epoch assigned to rows written before epoch tracking existed.
pub(super) const LEGACY_REPLAY_EPOCH: &str = "__legacy__";

/// Metadata catalog table storing replay epoch transitions per DuckLake table.
const REPLAY_EPOCHS_TABLE: &str = "__etl_replay_epochs";

/// Returns the quoted metadata table name for replay epochs.
fn replay_epochs_table_name(metadata_schema: &str) -> String {
    format!("{}.{}", quote_identifier(metadata_schema), quote_identifier(REPLAY_EPOCHS_TABLE))
}

/// Generates a new opaque replay epoch identifier.
fn new_replay_epoch() -> String {
    format!("{:032x}", rand::rng().random::<u128>())
}

/// Ensures the Postgres-backed replay epoch table exists in the DuckLake
/// metadata schema.
///
/// Destinations that start together against one catalog all run this. Postgres
/// `create table if not exists` is not safe under concurrency: two sessions can
/// both pass the existence check, and the loser fails on the catalog itself
/// (`23505` on `pg_type_typname_nsp_index`, or `42P07`), and `add column if not
/// exists` can fail the same way. A transaction-scoped advisory lock keyed by
/// the table name serializes the whole create-and-upgrade; the lock is released
/// with the transaction, so a later caller always sees the committed table and
/// both statements are plain no-ops for it.
pub(super) async fn ensure_replay_epoch_table_exists(
    pool: &PgPool,
    metadata_schema: &str,
) -> EtlResult<()> {
    let table_name = replay_epochs_table_name(metadata_schema);
    let mut transaction = pool.begin().await.map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake replay epoch table creation failed",
            source: source
        )
    })?;

    sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("etl.ducklake.replay_epochs:{table_name}"))
        .execute(&mut *transaction)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake replay epoch table creation failed",
                source: source
            )
        })?;

    let sql = format!(
        r#"create table if not exists {table_name} (
             table_name text primary key,
             replay_epoch text not null,
             pending_replay_epoch text,
             updated_at timestamptz not null default now()
           );"#
    );

    sqlx::query(AssertSqlSafe(sql)).execute(&mut *transaction).await.map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake replay epoch table creation failed",
            source: source
        )
    })?;

    // Older catalogs only have the committed replay epoch. Adding the pending
    // column here upgrades them before any reset can start a recoverable epoch
    // transition.
    let sql =
        format!("alter table {table_name} add column if not exists pending_replay_epoch text;");
    sqlx::query(AssertSqlSafe(sql)).execute(&mut *transaction).await.map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake pending replay epoch column creation failed",
            source: source
        )
    })?;

    transaction.commit().await.map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake replay epoch table creation failed",
            source: source
        )
    })?;

    Ok(())
}

/// Reads the current replay epoch for a table.
pub(super) async fn read_table_replay_epoch(
    pool: &PgPool,
    metadata_schema: &str,
    table_name: &DuckLakeTableName,
) -> EtlResult<String> {
    let epochs_table = replay_epochs_table_name(metadata_schema);
    let sql = format!("select replay_epoch from {epochs_table} where table_name = $1;");
    let table_id = table_name.id();

    let replay_epoch = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql))
        .bind(&table_id)
        .fetch_optional(pool)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake replay epoch lookup failed",
                format!("table={table_id}"),
                source: source
            )
        })?
        .unwrap_or_else(|| LEGACY_REPLAY_EPOCH.to_owned());

    Ok(replay_epoch)
}

/// Starts or resumes a replay epoch transition and returns its pending value.
pub(super) async fn begin_table_replay_epoch_transition(
    pool: &PgPool,
    metadata_schema: &str,
    table_name: &DuckLakeTableName,
) -> EtlResult<String> {
    let epochs_table = replay_epochs_table_name(metadata_schema);
    let sql = format!(
        r#"insert into {epochs_table} as epochs
             (table_name, replay_epoch, pending_replay_epoch, updated_at)
           values ($1, $2, $3, now())
           on conflict (table_name)
           do update set
             pending_replay_epoch = coalesce(
               epochs.pending_replay_epoch,
               excluded.pending_replay_epoch
             ),
             updated_at = now()
           returning pending_replay_epoch;"#
    );
    let table_id = table_name.id();
    let new_pending_replay_epoch = new_replay_epoch();

    let pending_replay_epoch = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql))
        .bind(&table_id)
        .bind(LEGACY_REPLAY_EPOCH)
        .bind(&new_pending_replay_epoch)
        .fetch_one(pool)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake replay epoch transition failed to begin",
                format!("table={table_id}"),
                source: source
            )
        })?;

    Ok(pending_replay_epoch)
}

/// Promotes a pending replay epoch after its DuckLake reset commits.
pub(super) async fn complete_table_replay_epoch_transition(
    pool: &PgPool,
    metadata_schema: &str,
    table_name: &DuckLakeTableName,
    pending_replay_epoch: &str,
) -> EtlResult<()> {
    let epochs_table = replay_epochs_table_name(metadata_schema);
    let sql = format!(
        r#"update {epochs_table}
           set replay_epoch = $2, pending_replay_epoch = null, updated_at = now()
           where table_name = $1
             and (
               pending_replay_epoch = $2
               or (pending_replay_epoch is null and replay_epoch = $2)
             )
           returning replay_epoch;"#
    );
    let table_id = table_name.id();

    let completed_replay_epoch = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql))
        .bind(&table_id)
        .bind(pending_replay_epoch)
        .fetch_optional(pool)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake replay epoch transition failed to complete",
                format!("table={table_id}"),
                source: source
            )
        })?;

    if completed_replay_epoch.is_none() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake replay epoch transition is inconsistent",
            format!("table={table_id}")
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Arc};

    use etl::config::{PgConnectionConfig, TcpKeepaliveConfig};
    use etl_config::ducklake_catalog_metadata_connect_options;
    use etl_postgres::{test_utils::local_tls_config_from_env, tokio::test_utils::PgDatabase};
    use sqlx::postgres::PgPoolOptions;
    use tokio::sync::Barrier;
    use tokio_postgres::Client;
    use url::Url;
    use uuid::Uuid;

    use super::*;

    const CONCURRENT_CALLERS: usize = 16;

    /// Destinations that start together on a fresh catalog must all succeed.
    /// Each caller has its own pool, as separate replicator processes do, and
    /// every one starts on the same barrier so the existence checks overlap.
    /// Without serialization, the losers fail with `23505` on
    /// `pg_type_typname_nsp_index` (or `42P07`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_callers_all_create_the_table_on_a_fresh_schema() {
        let database_name = Uuid::new_v4().to_string();
        let host = env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set");
        let port: u16 = env::var("TESTS_DATABASE_PORT")
            .expect("TESTS_DATABASE_PORT must be set")
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number");
        let username =
            env::var("TESTS_DATABASE_USERNAME").expect("TESTS_DATABASE_USERNAME must be set");
        let password = env::var("TESTS_DATABASE_PASSWORD").ok();
        // Keeps the database alive; dropping it removes it.
        let _database: PgDatabase<Client> = PgDatabase::new(PgConnectionConfig {
            host: host.clone(),
            hostaddr: None,
            port,
            name: database_name.clone(),
            username: username.clone(),
            password: password.clone().map(Into::into),
            tls: local_tls_config_from_env(),
            keepalive: TcpKeepaliveConfig::default(),
        })
        .await;

        let mut catalog_url = Url::parse("postgres://localhost").expect("failed to parse url");
        catalog_url.set_host(Some(&host)).expect("failed to set host");
        catalog_url.set_port(Some(port)).expect("failed to set port");
        catalog_url.set_username(&username).expect("failed to set username");
        catalog_url.set_password(password.as_deref()).expect("failed to set password");
        catalog_url.set_path(&database_name);

        let start = Arc::new(Barrier::new(CONCURRENT_CALLERS));
        let mut callers = Vec::with_capacity(CONCURRENT_CALLERS);
        for _ in 0..CONCURRENT_CALLERS {
            let pool = PgPoolOptions::new().max_connections(1).connect_lazy_with(
                ducklake_catalog_metadata_connect_options(&catalog_url)
                    .expect("failed to build catalog connect options"),
            );
            let start = Arc::clone(&start);
            callers.push(tokio::spawn(async move {
                // Connect before the barrier so the race is the DDL, not the
                // connection handshake.
                sqlx::query("select 1").execute(&pool).await.expect("failed to connect");
                start.wait().await;
                ensure_replay_epoch_table_exists(&pool, "public").await.map_err(|e| e.to_string())
            }));
        }

        let mut failures = Vec::new();
        for caller in callers {
            if let Err(error) = caller.await.expect("caller task panicked") {
                failures.push(error);
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {CONCURRENT_CALLERS} callers failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}
