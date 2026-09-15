//! Table batches are the atomic per-table write units used by DuckLake writes.
//! Copy, mutation, and truncate inputs are normalized into prepared batches so
//! each attempt can replay the same SQL and replay bookkeeping. Copy batches
//! receive opaque upstream ids and retain them across destination-local
//! retries. Streaming mutation and truncate batches advance a per-table
//! progress watermark.
//! Bounded batch sizes preserve table-local ordering without letting one
//! transaction grow unbounded.

#[cfg(feature = "test-utils")]
use std::sync::LazyLock;
use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    error, fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use etl::{
    data::{Cell, OldTableRow, PartialTableRow, SizeHint, TableRow, UpdatedTableRow},
    destination::TableCopyBatchId,
    error::{ErrorKind, EtlResult},
    etl_error,
    event::EventSequenceKey,
    schema::ReplicatedTableSchema,
};
use metrics::{counter, histogram};
#[cfg(feature = "test-utils")]
use parking_lot::Mutex;
use pg_escape::quote_literal;
use rand::Rng;
use tokio::{sync::Semaphore, time::Instant};
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, trace, warn};

use crate::{
    ducklake::{
        DUCKLAKE_COLUMN_NAME_MAPPING, DuckLakeStreamingBatchConfig, DuckLakeTableName,
        LAKE_CATALOG,
        client::{
            DuckLakeBlockingOperationContext, DuckLakeConnectionManager, format_query_error_detail,
            is_ducklake_shutdown_requested_error, run_duckdb_blocking,
            run_duckdb_blocking_with_context,
        },
        core::is_create_table_conflict,
        diagnostics::query_log_detail,
        encoding::{
            PreparedRows, cell_to_sql_literal_ref, prepare_copy_rows, prepare_rows,
            table_row_to_sql_literal_ref,
        },
        key_set::{DeleteKeySet, KeyComponent},
        metrics::{
            BATCH_KIND_LABEL, DELETE_ORIGIN_LABEL, ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
            ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS, ETL_DUCKLAKE_DELETE_PREDICATES,
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL, ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
            ETL_DUCKLAKE_RETRIES_TOTAL, ETL_DUCKLAKE_UPSERT_ROWS, PREPARED_ROWS_KIND_LABEL,
            RETRY_SCOPE_LABEL, SUB_BATCH_KIND_LABEL,
        },
        partial_update::{
            PartialUpdateRecovery, PartialUpdateRecoveryKey, PartialUpdateRecoveryOutcome,
            PartialUpdateRecoveryRequest, RecoveredPartialRows, StoredRowRecovery,
            identity_predicate,
        },
        replay_epoch::LEGACY_REPLAY_EPOCH,
        sql::{qualified_lake_table_name, quote_identifier},
    },
    retry::{RetryAttempt, RetryDecision, RetryPolicy, retry_with_backoff},
};

/// Maximum number of rows per SQL `INSERT ... VALUES` batch when nested values
/// force the staging path to bypass DuckDB's appender API.
const SQL_INSERT_BATCH_SIZE: usize = 128;
/// Maximum number of primary-key predicates per SQL `DELETE` batch.
///
/// Keep this small so each delete statement remains cheap while still avoiding
/// one round-trip per deleted row.
const SQL_DELETE_BATCH_SIZE: usize = 16;
/// ETL-managed marker table storing per-table applied copy batches.
const APPLIED_BATCHES_TABLE: &str = "__etl_applied_table_batches";
/// Data inlining limit for append-only DuckLake helper tables.
///
/// This per-table option intentionally overrides the COPY pool's attach-level
/// limit of zero. Helper markers and progress stay inline, while rows written
/// to the replicated table during COPY still become Parquet files. Maintenance
/// performs helper-table deletions while foreground mutations are paused.
const HELPER_TABLE_DATA_INLINING_ROW_LIMIT: usize = 256;
/// Replay epoch column shared by ETL helper tables.
const REPLAY_EPOCH_COLUMN: &str = "replay_epoch";

/// Formats an optional LSN without using debug output.
fn format_optional_lsn(lsn: Option<PgLsn>) -> String {
    lsn.map_or_else(|| "none".to_owned(), |lsn| lsn.to_string())
}

/// Formats a sequence key using DuckLake's existing fixed-width hexadecimal
/// representation.
fn format_sequence_key(sequence_key: EventSequenceKey) -> String {
    let commit_lsn = u64::from(sequence_key.commit_lsn);
    format!("{commit_lsn:016x}/{:016x}", sequence_key.tx_ordinal)
}

/// Formats an optional sequence key without using debug output.
fn format_optional_sequence_key(sequence_key: Option<EventSequenceKey>) -> String {
    sequence_key.map_or_else(|| "none".to_owned(), format_sequence_key)
}

/// Returns whether one DuckDB error is the standard interrupted query error.
fn is_duckdb_interrupt_error(error: &duckdb::Error) -> bool {
    error.to_string().contains("INTERRUPT Error: Interrupted")
}

/// Query failure whose value-bearing diagnostics require explicit opt-in.
struct DuckDbSensitiveQueryError {
    error: duckdb::Error,
    sql: String,
}
impl fmt::Display for DuckDbSensitiveQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if cfg!(feature = "ducklake-query-error-details") {
            write!(f, "{}; SQL: {}", self.error, self.sql)
        } else {
            write!(
                f,
                "DuckDB query failed; error message omitted because it may contain row values"
            )
        }
    }
}
impl fmt::Debug for DuckDbSensitiveQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl error::Error for DuckDbSensitiveQueryError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        if cfg!(feature = "ducklake-query-error-details") { Some(&self.error) } else { None }
    }
}

/// Formats query context for a delete mutation without row values.
fn format_delete_mutation_error_detail(
    target_table: &str,
    predicate_count: usize,
    chunk_index: usize,
    chunk_count: usize,
    chunk_predicate_count: usize,
) -> String {
    format!(
        "sql: DELETE FROM {target_table} WHERE [redacted predicates]; predicate_count: \
         {predicate_count}; chunk_index: {chunk_index}; chunk_count: {chunk_count}; \
         chunk_predicate_count: {chunk_predicate_count}"
    )
}

/// Formats query context for an update mutation without row values.
fn format_update_mutation_error_detail(
    target_table: &str,
    assignment_count: usize,
    has_predicate: bool,
) -> String {
    format!(
        "sql: UPDATE {target_table} SET [redacted assignments] WHERE [redacted predicate]; \
         assignment_count: {assignment_count}; has_predicate: {has_predicate}"
    )
}

/// ETL-managed per-table streaming replay progress for steady-state CDC
/// retries.
const STREAMING_PROGRESS_TABLE: &str = "__etl_streaming_progress";
/// Maximum number of times a failed write attempt is retried before giving up.
const MAX_COMMIT_RETRIES: u32 = 10;
/// Initial backoff duration before the first retry.
const INITIAL_RETRY_DELAY_MS: u64 = 50;
/// Upper bound on backoff duration.
const MAX_RETRY_DELAY_MS: u64 = 2_000;
/// Minimum retry delay for transient delete-file visibility failures.
const TRANSIENT_DELETE_FILE_RETRY_DELAY_MS: u64 = 5_000;

/// Decides whether DuckLake-owned retry loops should retry one failure.
fn ducklake_retry_decision(error: &etl::error::EtlError) -> RetryDecision {
    if is_ducklake_shutdown_requested_error(error) {
        RetryDecision::Stop
    } else {
        RetryDecision::Retry
    }
}

/// Event-level table mutations that must be applied in order.
pub(super) enum TableMutation {
    Insert(TableRow),
    Delete(OldTableRow),
    Update { delete_row: OldTableRow, new_row: UpdatedTableRow },
    Replace(TableRow),
}

/// Prepared table mutations ready for execution and retries.
#[derive(Debug)]
enum PreparedTableMutation {
    Upsert(PreparedRows),
    Delete {
        // For WHERE clause predicates used in DELETE statements.
        predicates: Vec<String>,
        // Null-safe set join for canonical integer/UUID identities.
        key_set: Option<String>,
        // To know if it's coming from an update or delete operation.
        origin: &'static str,
    },
    Update {
        // For SET clause assignments used in UPDATE statements. Example: "value=1, id=3"
        assignments: Vec<String>,
        // For the WHERE clause predicate used in the UPDATE statement. Example: "id = 4 AND
        // content = 'hello'"
        predicate: String,
    },
}

/// Borrowed row shape used to build delete predicates.
enum DeletePredicateRowRef<'a> {
    Full(&'a TableRow),
    Key(&'a TableRow),
}

impl<'a> From<&'a TableRow> for DeletePredicateRowRef<'a> {
    fn from(value: &'a TableRow) -> Self {
        Self::Full(value)
    }
}

impl<'a> From<&'a OldTableRow> for DeletePredicateRowRef<'a> {
    fn from(value: &'a OldTableRow) -> Self {
        match value {
            OldTableRow::Full(row) => Self::Full(row),
            OldTableRow::Key(row) => Self::Key(row),
        }
    }
}

/// Event-level table mutation annotated for idempotent replay.
pub(super) struct TrackedTableMutation {
    sequence_key: EventSequenceKey,
    mutation: TableMutation,
}

impl TrackedTableMutation {
    /// Creates one tracked mutation preserved for retry-safe replay.
    pub(super) fn new(sequence_key: EventSequenceKey, mutation: TableMutation) -> Self {
        Self { sequence_key, mutation }
    }

    /// Returns the stable event sequence key for this mutation.
    fn sequence_key(&self) -> EventSequenceKey {
        self.sequence_key
    }
}

/// Truncate event metadata preserved for idempotent replay.
#[derive(Clone, Copy)]
pub(super) struct TrackedTruncateEvent {
    sequence_key: EventSequenceKey,
    options: i8,
}

impl TrackedTruncateEvent {
    /// Creates one tracked truncate event preserved for retry-safe replay.
    pub(super) fn new(sequence_key: EventSequenceKey, options: i8) -> Self {
        Self { sequence_key, options }
    }

    /// Returns the stable event sequence key for this truncate.
    fn sequence_key(&self) -> EventSequenceKey {
        self.sequence_key
    }
}

/// Stable hash used to derive per-table batch identifiers.
struct BatchIdHasher(u64);

impl BatchIdHasher {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }
}

impl Default for BatchIdHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for BatchIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }
}

/// Atomic DuckLake batch kinds used by replay bookkeeping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckLakeTableBatchKind {
    Copy,
    CopyComplete,
    Mutation,
    Truncate,
}

impl DuckLakeTableBatchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::CopyComplete => "copy_complete",
            Self::Mutation => "mutation",
            Self::Truncate => "truncate",
        }
    }
}

/// Deterministic identity for one table batch.
struct DuckLakeBatchIdentity {
    batch_id: String,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
}

/// Returns destination-visible column names in replicated write order.
fn replicated_column_names(replicated_table_schema: &ReplicatedTableSchema) -> Vec<String> {
    replicated_table_schema
        .destination_column_schemas(DUCKLAKE_COLUMN_NAME_MAPPING)
        .map(|column| column.name)
        .collect()
}

/// Prepared per-table work executed atomically in one DuckLake transaction.
enum PreparedDuckLakeTableBatchAction {
    Mutation(Vec<PreparedTableMutation>),
    Truncate,
}

/// Prepared atomic DuckLake table batch with replay metadata.
pub(super) struct PreparedDuckLakeTableBatch {
    table_name: DuckLakeTableName,
    replay_epoch: String,
    batch_id: String,
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    first_sequence_key: Option<EventSequenceKey>,
    last_sequence_key: Option<EventSequenceKey>,
    insert_column_names: Vec<String>,
    action: PreparedDuckLakeTableBatchAction,
}

/// Compact durable marker retained while COPY rows remain staged.
struct PreparedDuckLakeBatchMarker {
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
}

/// Prepared initial-copy rows with a lightweight size estimate for buffering.
pub(super) struct PreparedDuckLakeCopyBatch {
    batch: PreparedDuckLakeTableBatch,
    estimated_bytes: u64,
}

impl PreparedDuckLakeCopyBatch {
    /// Returns the approximate decoded bytes owned by this row batch.
    pub(super) fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    /// Converts this prepared copy payload into the existing atomic-batch path.
    pub(super) fn into_atomic_batch(self) -> PreparedDuckLakeTableBatch {
        self.batch
    }

    /// Separates staged row data from its durable applied-batch marker.
    fn into_buffer_parts(
        mut self,
    ) -> EtlResult<(String, PreparedDuckLakeBatchMarker, PreparedRows)> {
        let PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) = &mut self.batch.action
        else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake buffered copy batch is not a mutation"
            ));
        };
        if prepared_mutations.len() != 1 {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake buffered copy batch has an invalid mutation count",
                format!("Expected 1 mutation, got {}", prepared_mutations.len())
            ));
        }
        let prepared_mutation = prepared_mutations.pop().ok_or_else(|| {
            etl_error!(ErrorKind::InvalidState, "DuckLake buffered copy mutation is missing")
        })?;
        let PreparedTableMutation::Upsert(prepared_rows) = prepared_mutation else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake buffered copy mutation is not an upsert"
            ));
        };

        let marker = PreparedDuckLakeBatchMarker {
            batch_kind: self.batch.batch_kind,
            first_start_lsn: self.batch.first_start_lsn,
            last_commit_lsn: self.batch.last_commit_lsn,
        };

        Ok((self.batch.batch_id, marker, prepared_rows))
    }
}

impl PreparedDuckLakeTableBatch {
    /// Returns the destination table this batch targets.
    pub(super) fn table_name(&self) -> &DuckLakeTableName {
        &self.table_name
    }

    /// Returns whether this batch uses the streaming progress replay path.
    fn uses_streaming_progress(&self) -> bool {
        matches!(
            self.batch_kind,
            DuckLakeTableBatchKind::Mutation | DuckLakeTableBatchKind::Truncate
        )
    }
}

/// One table-local streaming replay watermark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TableStreamingProgress {
    last_sequence_key: EventSequenceKey,
}

/// Ensures the ETL-managed replay marker table exists.
pub(super) async fn ensure_applied_batches_table_exists(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    applied_batches_table_created: Arc<AtomicBool>,
) -> EtlResult<()> {
    if applied_batches_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let _table_creation_permit = table_creation_slots.acquire_owned().await.map_err(|_| {
        etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
    })?;

    if applied_batches_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}" (
             table_name VARCHAR NOT NULL,
             replay_epoch VARCHAR,
             batch_id VARCHAR NOT NULL,
             batch_kind VARCHAR NOT NULL,
             first_start_lsn UBIGINT,
             last_commit_lsn UBIGINT,
             applied_at TIMESTAMPTZ NOT NULL
             );"#
    );
    let created = Arc::clone(&applied_batches_table_created);
    let table_name = APPLIED_BATCHES_TABLE.to_owned();

    run_duckdb_blocking(pool, blocking_slots, move |conn| -> EtlResult<()> {
        match conn.execute_batch(&ddl) {
            Ok(()) => {}
            Err(error) if is_create_table_conflict(&error, &table_name) => {}
            Err(error) => {
                return Err(etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake CREATE TABLE failed",
                    format_query_error_detail(&ddl),
                    source: error
                ));
            }
        }
        ensure_helper_table_replay_epoch_column(conn, APPLIED_BATCHES_TABLE)?;

        let set_option_sql = format!(
            "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {}, table_name => {});",
            HELPER_TABLE_DATA_INLINING_ROW_LIMIT,
            quote_literal(APPLIED_BATCHES_TABLE),
        );
        conn.execute_batch(&set_option_sql).map_err(|err| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake set_option failed",
                format_query_error_detail(&set_option_sql),
                source: err
            )
        })?;

        created.store(true, Ordering::Relaxed);
        Ok(())
    })
    .await
}

/// Ensures the ETL-managed streaming progress table exists.
pub(super) async fn ensure_streaming_progress_table_exists(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    streaming_progress_table_created: Arc<AtomicBool>,
) -> EtlResult<()> {
    if streaming_progress_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let _table_creation_permit = table_creation_slots.acquire_owned().await.map_err(|_| {
        etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
    })?;

    if streaming_progress_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}" (
             table_name VARCHAR NOT NULL,
             replay_epoch VARCHAR,
             last_commit_lsn UBIGINT NOT NULL,
             last_tx_ordinal UBIGINT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL
             );"#
    );
    let created = Arc::clone(&streaming_progress_table_created);
    let table_name = STREAMING_PROGRESS_TABLE.to_owned();

    run_duckdb_blocking(pool, blocking_slots, move |conn| -> EtlResult<()> {
        match conn.execute_batch(&ddl) {
            Ok(()) => {}
            Err(err) if is_create_table_conflict(&err, &table_name) => {}
            Err(err) => {
                return Err(etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake CREATE TABLE failed",
                    format_query_error_detail(&ddl),
                    source: err
                ));
            }
        }
        ensure_helper_table_replay_epoch_column(conn, STREAMING_PROGRESS_TABLE)?;

        let set_option_sql = format!(
            "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {}, table_name => {});",
            HELPER_TABLE_DATA_INLINING_ROW_LIMIT,
            quote_literal(STREAMING_PROGRESS_TABLE),
        );
        conn.execute_batch(&set_option_sql).map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake set_option failed",
                format_query_error_detail(&set_option_sql),
                source: error
            )
        })?;

        created.store(true, Ordering::Relaxed);
        Ok(())
    })
    .await
}

/// Adds the replay epoch column to helper tables created by older versions.
fn ensure_helper_table_replay_epoch_column(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<()> {
    if helper_table_has_column(conn, table_name, REPLAY_EPOCH_COLUMN)? {
        return Ok(());
    }

    let table_name = format!(r#"{LAKE_CATALOG}."{table_name}""#);
    let column_name = quote_identifier(REPLAY_EPOCH_COLUMN);
    let sql = format!("ALTER TABLE {table_name} ADD COLUMN {column_name} VARCHAR;");
    conn.execute_batch(&sql).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake helper table migration failed",
            format_query_error_detail(&sql),
            source: source
        )
    })?;

    Ok(())
}

fn helper_table_has_column(
    conn: &duckdb::Connection,
    table_name: &str,
    column_name: &str,
) -> EtlResult<bool> {
    let sql = format!(
        "SELECT 1 FROM information_schema.columns WHERE table_catalog = {} AND table_name = {} \
         AND column_name = {} LIMIT 1;",
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
        quote_literal(column_name)
    );
    let mut statement = conn.prepare(&sql).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake helper table schema lookup failed",
            format_query_error_detail(&sql),
            source: source
        )
    })?;
    let mut rows = statement.query([]).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake helper table schema lookup failed",
            format_query_error_detail(&sql),
            source: source
        )
    })?;

    rows.next().map(|row| row.is_some()).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake helper table schema row fetch failed",
            format_query_error_detail(&sql),
            source: source
        )
    })
}

/// Prepares and applies one table's CDC mutations, one atomic batch at a time.
///
/// Each batch keeps a separate retry and timeout budget, so a committed batch
/// never consumes the next batch's execution budget and the replay watermark
/// stays atomic with its data, including ambiguous commits.
///
/// Preparation is interleaved with application because completing partial
/// updates reads stored values. A batch must observe the rows every earlier
/// batch of the same call already wrote, so its recovery read runs only once
/// the previous batch has committed.
pub(super) async fn prepare_and_apply_mutation_table_batches(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    config: DuckLakeStreamingBatchConfig,
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: DuckLakeTableName,
    replay_epoch: String,
    tracked_mutations: Vec<TrackedTableMutation>,
) -> EtlResult<()> {
    let mut pending_chunks: VecDeque<Vec<TrackedTableMutation>> =
        split_tracked_mutations(config, tracked_mutations).into();
    info!(table = %table_name, batch_count = pending_chunks.len(),
        "ducklake table batches prepared");

    let mut known_progress = None;
    while let Some(mut chunk) = pending_chunks.pop_front() {
        let mut recovered = RecoveredPartialRows::empty();
        if has_canonical_identity(replicated_table_schema) {
            let request = plan_partial_update_recovery(
                replicated_table_schema,
                chunk.iter().map(|tracked| &tracked.mutation),
                recovery_byte_budget(config, &chunk),
            )?;
            if !request.is_empty() {
                let request = Arc::new(request);
                let outcome = recover_partial_update_rows_with_retry(
                    Arc::clone(&pool),
                    Arc::clone(&blocking_slots),
                    replicated_table_schema.clone(),
                    table_name.clone(),
                    Arc::clone(&request),
                )
                .await?;
                // A read that stopped at its byte budget leaves the events
                // from the first uncovered identity to the next batch, which
                // reads again once this batch has committed.
                let covered_mutations =
                    covered_mutation_count(chunk.len(), &request, outcome.covered_keys);
                if covered_mutations < chunk.len() {
                    let remainder = chunk.split_off(covered_mutations);
                    pending_chunks.push_front(remainder);
                }
                recovered = outcome.recovered;
            }
        }

        let mut prepared_batches = Vec::with_capacity(1);
        push_prepared_mutation_batch(
            &mut prepared_batches,
            replicated_table_schema,
            &table_name,
            &replay_epoch,
            chunk,
            recovered,
        )?;

        let Some(batch) = prepared_batches.pop() else {
            continue;
        };
        known_progress = apply_table_batch_with_progress_retry(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            batch,
            known_progress,
        )
        .await?;
    }

    Ok(())
}

/// Returns the bytes one batch may add by completing its partial updates.
///
/// The chunk is already bounded by the streaming byte cap, but it is bounded
/// by the bytes the events carry, and a partial update carries none of the
/// values it leaves out. Budgeting the recovery against the same cap keeps a
/// batch of partial updates for large stored rows from growing without bound.
fn recovery_byte_budget(
    config: DuckLakeStreamingBatchConfig,
    chunk: &[TrackedTableMutation],
) -> usize {
    let chunk_bytes = chunk
        .iter()
        .map(|tracked| mutation_size_hint(&tracked.mutation))
        .fold(0usize, usize::saturating_add);

    config.max_bytes.saturating_sub(chunk_bytes).max(1)
}

/// Returns how many leading mutations of a chunk one recovery covers.
///
/// Identities are requested in first-seen order and a read stops only at its
/// byte budget, so the uncovered identities are a suffix and the batch keeps
/// every event before the first of them.
fn covered_mutation_count(
    chunk_len: usize,
    request: &PartialUpdateRecoveryRequest,
    covered_keys: usize,
) -> usize {
    request.keys().get(covered_keys).map_or(chunk_len, |key| key.mutation_index.min(chunk_len))
}

/// Reads one batch's missing partial-update columns, retrying read failures.
///
/// A recovery read fails for the same transient reasons a commit does, so it
/// shares the commit retry budget instead of surfacing one catalog or storage
/// blip as a table-level failure.
async fn recover_partial_update_rows_with_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    replicated_table_schema: ReplicatedTableSchema,
    table_name: DuckLakeTableName,
    request: Arc<PartialUpdateRecoveryRequest>,
) -> EtlResult<PartialUpdateRecoveryOutcome> {
    let requested_keys = request.keys().len();
    let retry_table_name = table_name.clone();

    retry_recovery_read(retry_table_name, requested_keys, move || {
        let pool = Arc::clone(&pool);
        let blocking_slots = Arc::clone(&blocking_slots);
        let attempt_schema = replicated_table_schema.clone();
        let attempt_table_name = table_name.clone();
        let attempt_request = Arc::clone(&request);
        async move {
            run_duckdb_blocking(pool, blocking_slots, move |conn| {
                StoredRowRecovery::new(conn, &attempt_table_name, &attempt_schema)
                    .recover(&attempt_request)
            })
            .await
        }
    })
    .await
}

/// Retries one recovery read with the batch commit retry budget.
async fn retry_recovery_read<AttemptFn, AttemptFut>(
    table_name: DuckLakeTableName,
    requested_keys: usize,
    attempt: AttemptFn,
) -> EtlResult<PartialUpdateRecoveryOutcome>
where
    AttemptFn: FnMut() -> AttemptFut,
    AttemptFut: std::future::Future<Output = EtlResult<PartialUpdateRecoveryOutcome>>,
{
    retry_with_backoff(
        RetryPolicy {
            max_retries: MAX_COMMIT_RETRIES,
            initial_delay: Duration::from_millis(INITIAL_RETRY_DELAY_MS),
            max_delay: Duration::from_millis(
                MAX_RETRY_DELAY_MS.max(TRANSIENT_DELETE_FILE_RETRY_DELAY_MS),
            ),
        },
        ducklake_retry_decision,
        jitter_ducklake_retry_delay,
        |attempt: RetryAttempt<'_, etl::error::EtlError>| {
            counter!(
                ETL_DUCKLAKE_RETRIES_TOTAL,
                BATCH_KIND_LABEL => DuckLakeTableBatchKind::Mutation.as_str(),
                RETRY_SCOPE_LABEL => "partial_update_recovery",
            )
            .increment(1);
            warn!(
                attempt = attempt.retry_index,
                max = attempt.max_retries,
                table = %table_name,
                keys = requested_keys,
                error = %query_log_detail(attempt.error),
                "ducklake partial update recovery attempt failed, retrying"
            );
        },
        attempt,
    )
    .await
    .map_err(|failure| failure.last_error)
}

/// Applies one atomic per-table batch and retries on failure.
pub(super) async fn apply_table_batch_with_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batch: PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    apply_table_batch_with_progress_retry(pool, blocking_slots, batch, None).await.map(|_| ())
}

/// Reuse a cursor only within the caller's held table slot, after a confirmed
/// successful attempt. Every retry rereads durable progress (ambiguous COMMIT).
async fn apply_table_batch_with_progress_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batch: PreparedDuckLakeTableBatch,
    known_progress: Option<TableStreamingProgress>,
) -> EtlResult<Option<TableStreamingProgress>> {
    let first_attempt = AtomicBool::new(true);
    let table_name = batch.table_name.clone();
    let batch_id = batch.batch_id.clone();
    let batch_kind = batch.batch_kind;
    let batch = Arc::new(batch);

    retry_with_backoff(
        RetryPolicy {
            max_retries: MAX_COMMIT_RETRIES,
            initial_delay: Duration::from_millis(INITIAL_RETRY_DELAY_MS),
            max_delay: Duration::from_millis(
                MAX_RETRY_DELAY_MS.max(TRANSIENT_DELETE_FILE_RETRY_DELAY_MS),
            ),
        },
        ducklake_retry_decision,
        jitter_ducklake_retry_delay,
        |attempt: RetryAttempt<'_, etl::error::EtlError>| {
            counter!(
                ETL_DUCKLAKE_RETRIES_TOTAL,
                BATCH_KIND_LABEL => batch_kind.as_str(),
                RETRY_SCOPE_LABEL => "single_batch",
            )
            .increment(1);
            warn!(
                attempt = attempt.retry_index,
                max = attempt.max_retries,
                table = %table_name,
                batch_id = %batch_id,
                error = %query_log_detail(attempt.error),
                "ducklake table mutation attempt failed, retrying"
            );
        },
        move || {
            let known_progress =
                first_attempt.swap(false, Ordering::Relaxed).then_some(known_progress).flatten();
            let attempt_batch = Arc::clone(&batch);
            let pool = Arc::clone(&pool);
            let blocking_slots = Arc::clone(&blocking_slots);
            async move {
                run_duckdb_blocking_with_context(pool, blocking_slots, move |conn, context| {
                    if batch_kind == DuckLakeTableBatchKind::Copy {
                        if applied_batch_marker_exists(conn, attempt_batch.as_ref())? {
                            record_replayed_batch_skip(attempt_batch.as_ref());
                            return Ok(None);
                        }

                        apply_table_batch(conn, attempt_batch.as_ref(), context)?;
                        return Ok(None);
                    }

                    let batches = std::slice::from_ref(attempt_batch.as_ref());
                    match known_progress {
                        Some(progress) => apply_table_batches_with_progress(
                            conn,
                            batches,
                            context,
                            Some(progress),
                        ),
                        None => apply_table_batches(conn, batches, context),
                    }
                })
                .await
            }
        },
    )
    .await
    .map_err(|failure| {
        if is_ducklake_shutdown_requested_error(&failure.last_error) {
            return failure.last_error;
        }

        counter!(
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
            BATCH_KIND_LABEL => batch_kind.as_str(),
            RETRY_SCOPE_LABEL => "single_batch",
        )
        .increment(1);
        etl_error!(
            ErrorKind::DestinationAtomicBatchRetryable,
            "DuckLake atomic table batch failed after retries",
            format!(
                "table={table_name}, batch_id={batch_id}, batch_kind={}",
                batch_kind.as_str()
            ),
            source: failure.last_error
        )
    })
}

/// Returns the approximate decoded size of the values one mutation carries.
fn mutation_size_hint(mutation: &TableMutation) -> usize {
    match mutation {
        TableMutation::Insert(row) | TableMutation::Replace(row) => row.size_hint(),
        TableMutation::Delete(row) => row.size_hint(),
        TableMutation::Update { delete_row, new_row } => {
            delete_row.size_hint().saturating_add(new_row.size_hint())
        }
    }
}

/// Splits one table's CDC mutations into ordered atomic batch inputs.
///
/// Mutations stay in source order and use the upstream CDC mutation cap.
fn split_tracked_mutations(
    config: DuckLakeStreamingBatchConfig,
    tracked_mutations: Vec<TrackedTableMutation>,
) -> Vec<Vec<TrackedTableMutation>> {
    let mut chunks = Vec::new();
    let mut pending_mutations = Vec::new();
    let mut pending_bytes = 0usize;

    for tracked_mutation in tracked_mutations {
        let mutation_bytes = mutation_size_hint(&tracked_mutation.mutation);
        if !pending_mutations.is_empty()
            && pending_bytes.saturating_add(mutation_bytes) > config.max_bytes
        {
            chunks.push(std::mem::take(&mut pending_mutations));
            pending_bytes = 0;
        }
        pending_bytes = pending_bytes.saturating_add(mutation_bytes);
        pending_mutations.push(tracked_mutation);
        if pending_mutations.len() >= config.max_rows {
            chunks.push(std::mem::take(&mut pending_mutations));
            pending_bytes = 0;
        }
    }

    if !pending_mutations.is_empty() {
        chunks.push(pending_mutations);
    }

    chunks
}

/// Prepares every atomic batch of one table's CDC mutations up front.
///
/// Production interleaves preparation with application through
/// [`prepare_and_apply_mutation_table_batches`]; this helper keeps the batch
/// shapes reachable for tests that drive the prepared operations themselves.
#[cfg(test)]
fn prepare_mutation_table_batches(
    config: DuckLakeStreamingBatchConfig,
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: DuckLakeTableName,
    replay_epoch: String,
    tracked_mutations: Vec<TrackedTableMutation>,
    recovery: &dyn PartialUpdateRecovery,
) -> EtlResult<Vec<PreparedDuckLakeTableBatch>> {
    let mut prepared_batches = Vec::new();
    for chunk in split_tracked_mutations(config, tracked_mutations) {
        let recovered =
            recover_chunk_partial_updates(replicated_table_schema, &chunk, usize::MAX, recovery)?.1;
        push_prepared_mutation_batch(
            &mut prepared_batches,
            replicated_table_schema,
            &table_name,
            &replay_epoch,
            chunk,
            recovered,
        )?;
    }

    Ok(prepared_batches)
}

/// Prepares one retry-safe atomic batch for a table-copy row chunk.
pub(super) fn prepare_copy_table_batch(
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: DuckLakeTableName,
    replay_epoch: String,
    batch_id: TableCopyBatchId,
    table_rows: Vec<TableRow>,
) -> EtlResult<PreparedDuckLakeCopyBatch> {
    let estimated_bytes =
        table_rows.iter().map(SizeHint::size_hint).fold(0usize, usize::saturating_add);
    Ok(PreparedDuckLakeCopyBatch {
        batch: PreparedDuckLakeTableBatch {
            table_name,
            replay_epoch,
            batch_id: batch_id.to_string(),
            batch_kind: DuckLakeTableBatchKind::Copy,
            first_start_lsn: None,
            last_commit_lsn: None,
            first_sequence_key: None,
            last_sequence_key: None,
            insert_column_names: replicated_column_names(replicated_table_schema),
            action: PreparedDuckLakeTableBatchAction::Mutation(vec![
                PreparedTableMutation::Upsert(prepare_copy_rows(
                    replicated_table_schema,
                    table_rows,
                )?),
            ]),
        },
        estimated_bytes: u64::try_from(estimated_bytes).unwrap_or(u64::MAX),
    })
}

/// Prepares the durable marker written after every copy worker finishes.
pub(super) fn prepare_copy_complete_table_batch(
    table_name: DuckLakeTableName,
    replay_epoch: String,
) -> PreparedDuckLakeTableBatch {
    let identity = build_copy_complete_batch_identity(&table_name);
    PreparedDuckLakeTableBatch {
        table_name,
        replay_epoch,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::CopyComplete,
        first_start_lsn: None,
        last_commit_lsn: None,
        first_sequence_key: None,
        last_sequence_key: None,
        insert_column_names: Vec::new(),
        action: PreparedDuckLakeTableBatchAction::Mutation(Vec::new()),
    }
}

/// Prepares the ordered atomic batch for one table's truncate events.
pub(super) fn prepare_truncate_table_batch(
    table_name: DuckLakeTableName,
    replay_epoch: String,
    tracked_truncates: Vec<TrackedTruncateEvent>,
) -> PreparedDuckLakeTableBatch {
    let identity = build_truncate_batch_identity(&table_name, &tracked_truncates);
    PreparedDuckLakeTableBatch {
        table_name,
        replay_epoch,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Truncate,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_sequence_key: tracked_truncates.first().map(TrackedTruncateEvent::sequence_key),
        last_sequence_key: tracked_truncates.last().map(TrackedTruncateEvent::sequence_key),
        insert_column_names: Vec::new(),
        action: PreparedDuckLakeTableBatchAction::Truncate,
    }
}

/// Applies jitter to one DuckLake retry delay.
fn jitter_ducklake_retry_delay(base_delay: Duration) -> Duration {
    let jitter_ratio = rand::rng().random_range(0.5..=1.5_f64);
    base_delay.mul_f64(jitter_ratio)
}

/// Replay decision for one streaming batch after reading the table watermark.
enum StreamingReplayDecision {
    Skip,
    Apply,
}

/// Records that one replay-safe batch was skipped because it was already
/// committed.
fn record_replayed_batch_skip(batch: &PreparedDuckLakeTableBatch) {
    counter!(
        ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
        BATCH_KIND_LABEL => batch.batch_kind.as_str(),
    )
    .increment(1);
    debug!(
        table = %batch.table_name,
        batch_id = %batch.batch_id,
        batch_kind = batch.batch_kind.as_str(),
        "ducklake table batch already committed, skipping replay"
    );
}

/// Reads the steady-state streaming replay watermark for one table.
fn read_table_streaming_progress(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
) -> EtlResult<Option<TableStreamingProgress>> {
    let lookup_started = Instant::now();
    let table_id = table_name.id();
    let sql = format!(
        r#"SELECT last_commit_lsn, last_tx_ordinal
         FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         WHERE table_name = {} AND COALESCE({REPLAY_EPOCH_COLUMN}, {}) = {}
         ORDER BY last_commit_lsn DESC, last_tx_ordinal DESC
         LIMIT 1;"#,
        quote_literal(&table_id),
        quote_literal(LEGACY_REPLAY_EPOCH),
        quote_literal(replay_epoch),
    );
    let mut statement = conn.prepare(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress query prepare failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    let mut rows = statement.query([]).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress query failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;

    let row = rows.next().map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress row fetch failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    info!(table = %table_name, progress_lookup_elapsed_ms = lookup_started.elapsed().as_millis() as u64,
        "ducklake streaming progress read");
    let Some(row) = row else {
        return Ok(None);
    };

    let last_commit_lsn: u64 = row.get(0).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress commit lsn read failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    let last_tx_ordinal: u64 = row.get(1).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress tx ordinal read failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;

    Ok(Some(TableStreamingProgress {
        last_sequence_key: EventSequenceKey::new(PgLsn::from(last_commit_lsn), last_tx_ordinal),
    }))
}

/// Reads the last applied streaming sequence key for one table.
pub(super) fn read_table_streaming_progress_sequence_key(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
) -> EtlResult<Option<EventSequenceKey>> {
    Ok(read_table_streaming_progress(conn, table_name, replay_epoch)?
        .map(|progress| progress.last_sequence_key))
}

/// Drops already-applied tracked mutations using the persisted sequence key.
pub(super) fn retain_mutations_after_sequence_key(
    tracked_mutations: Vec<TrackedTableMutation>,
    last_sequence_key: Option<EventSequenceKey>,
) -> Vec<TrackedTableMutation> {
    match last_sequence_key {
        Some(last_sequence_key) => tracked_mutations
            .into_iter()
            .filter(|tracked_mutation| {
                compare_sequence_keys(tracked_mutation.sequence_key(), last_sequence_key)
                    == std::cmp::Ordering::Greater
            })
            .collect(),
        None => tracked_mutations,
    }
}

/// Drops already-applied tracked truncates using the persisted sequence key.
pub(super) fn retain_truncates_after_sequence_key(
    tracked_truncates: Vec<TrackedTruncateEvent>,
    last_sequence_key: Option<EventSequenceKey>,
) -> Vec<TrackedTruncateEvent> {
    match last_sequence_key {
        Some(last_sequence_key) => tracked_truncates
            .into_iter()
            .filter(|tracked_truncate| {
                compare_sequence_keys(tracked_truncate.sequence_key(), last_sequence_key)
                    == std::cmp::Ordering::Greater
            })
            .collect(),
        None => tracked_truncates,
    }
}

/// Decides whether a streaming batch must be replayed or skipped.
fn streaming_replay_decision(
    progress: TableStreamingProgress,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<StreamingReplayDecision> {
    let first_sequence_key = batch.first_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its first sequence key",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;
    let last_sequence_key = batch.last_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its last sequence key",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;

    if compare_sequence_keys(progress.last_sequence_key, first_sequence_key)
        != std::cmp::Ordering::Less
    {
        if compare_sequence_keys(progress.last_sequence_key, last_sequence_key)
            == std::cmp::Ordering::Less
        {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake streaming progress landed inside an atomic batch",
                format!(
                    "table={}, progress={}, first={}, last={}",
                    batch.table_name,
                    format_sequence_key(progress.last_sequence_key),
                    format_sequence_key(first_sequence_key),
                    format_sequence_key(last_sequence_key)
                )
            ));
        }

        return Ok(StreamingReplayDecision::Skip);
    }

    Ok(StreamingReplayDecision::Apply)
}

/// Compares two ETL event sequence keys using commit LSN then transaction
/// ordinal.
fn compare_sequence_keys(left: EventSequenceKey, right: EventSequenceKey) -> std::cmp::Ordering {
    (u64::from(left.commit_lsn), left.tx_ordinal)
        .cmp(&(u64::from(right.commit_lsn), right.tx_ordinal))
}

/// Applies all prepared atomic batches for one table on the same connection.
fn apply_table_batches(
    conn: &duckdb::Connection,
    batches: &[PreparedDuckLakeTableBatch],
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<Option<TableStreamingProgress>> {
    apply_table_batches_with_progress(conn, batches, operation_context, None)
}

fn apply_table_batches_with_progress(
    conn: &duckdb::Connection,
    batches: &[PreparedDuckLakeTableBatch],
    operation_context: &DuckLakeBlockingOperationContext,
    known_progress: Option<TableStreamingProgress>,
) -> EtlResult<Option<TableStreamingProgress>> {
    if batches.is_empty() {
        return Ok(known_progress);
    }

    let mut streaming_progress = if batches[0].uses_streaming_progress() {
        match known_progress {
            Some(progress) => Some(progress),
            None => read_table_streaming_progress(
                conn,
                batches[0].table_name(),
                &batches[0].replay_epoch,
            )?,
        }
    } else {
        None
    };

    for batch in batches {
        if !batch.uses_streaming_progress() {
            // Copy batches keep the marker path because initial-copy retries
            // still depend on per-batch idempotency.
            if applied_batch_marker_exists(conn, batch)? {
                record_replayed_batch_skip(batch);
                continue;
            }

            apply_table_batch(conn, batch, operation_context).map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake atomic table batch failed",
                    format!(
                        "table={}, batch_id={}, batch_kind={}",
                        batch.table_name,
                        batch.batch_id,
                        batch.batch_kind.as_str()
                    ),
                    source: error
                )
            })?;
            continue;
        }

        if let Some(progress) = streaming_progress {
            match streaming_replay_decision(progress, batch)? {
                StreamingReplayDecision::Skip => {
                    record_replayed_batch_skip(batch);
                    continue;
                }
                StreamingReplayDecision::Apply => {}
            }
        }

        apply_table_batch(conn, batch, operation_context).map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake atomic table batch failed",
                format!(
                    "table={}, batch_id={}, batch_kind={}",
                    batch.table_name,
                    batch.batch_id,
                    batch.batch_kind.as_str()
                ),
                source: error
            )
        })?;

        streaming_progress = batch
            .last_sequence_key
            .map(|last_sequence_key| TableStreamingProgress { last_sequence_key });
    }

    Ok(streaming_progress)
}

/// Builds one prepared atomic batch from an ordered slice of tracked mutations.
fn push_prepared_mutation_batch(
    prepared_batches: &mut Vec<PreparedDuckLakeTableBatch>,
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
    tracked_mutations: Vec<TrackedTableMutation>,
    recovered: RecoveredPartialRows,
) -> EtlResult<()> {
    if tracked_mutations.is_empty() {
        return Ok(());
    }

    let identity =
        build_mutation_batch_identity(table_name, replicated_table_schema, &tracked_mutations)?;
    let first_sequence_key = tracked_mutations.first().map(TrackedTableMutation::sequence_key);
    let last_sequence_key = tracked_mutations.last().map(TrackedTableMutation::sequence_key);
    let mutations = tracked_mutations.into_iter().map(|tracked| tracked.mutation).collect();

    prepared_batches.push(PreparedDuckLakeTableBatch {
        table_name: table_name.clone(),
        replay_epoch: replay_epoch.to_owned(),
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Mutation,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_sequence_key,
        last_sequence_key,
        insert_column_names: replicated_column_names(replicated_table_schema),
        action: PreparedDuckLakeTableBatchAction::Mutation(prepare_table_mutations(
            replicated_table_schema,
            mutations,
            recovered,
        )?),
    });

    Ok(())
}

/// Flushes a normalized full-row run before an operation that needs stored
/// values. Deletes precede surviving rows, and duplicate inserts are retained.
fn flush_full_row_writes(
    prepared: &mut Vec<PreparedTableMutation>,
    predicates: &mut Vec<String>,
    rows: &mut Vec<Option<TableRow>>,
    key_set: &mut DeleteKeySet,
    origin: &mut Option<&'static str>,
) {
    if !predicates.is_empty() {
        prepared.push(PreparedTableMutation::Delete {
            key_set: key_set.take_clause(),
            predicates: std::mem::take(predicates),
            origin: origin.take().unwrap_or("mixed"),
        });
    }
    if !rows.is_empty() {
        prepared.push(PreparedTableMutation::Upsert(prepare_rows(
            std::mem::take(rows).into_iter().flatten().collect(),
        )));
    }
}

/// Reads the stored columns one batch's partial updates leave out, then
/// normalizes the batch.
///
/// Production reads through [`prepare_and_apply_mutation_table_batches`],
/// which retries the read; this keeps the same steps reachable for tests that
/// drive one batch against a local table.
#[cfg(test)]
fn recover_and_prepare_table_mutations(
    replicated_table_schema: &ReplicatedTableSchema,
    mutations: Vec<TableMutation>,
    recovery: &dyn PartialUpdateRecovery,
) -> EtlResult<Vec<PreparedTableMutation>> {
    if !has_canonical_identity(replicated_table_schema) {
        return prepare_ordered_table_mutations(replicated_table_schema, mutations);
    }

    let request = plan_partial_update_recovery(replicated_table_schema, &mutations, usize::MAX)?;
    let recovered = if request.is_empty() {
        RecoveredPartialRows::empty()
    } else {
        recovery.recover(&request)?.recovered
    };

    prepare_table_mutations(replicated_table_schema, mutations, recovered)
}

/// Plans and performs one chunk's recovery read.
///
/// Returns how many leading mutations of the chunk the recovery covers, which
/// is fewer than the whole chunk only when the read stopped at its byte
/// budget, and the rows it read back. This is the synchronous counterpart of
/// the steps [`prepare_and_apply_mutation_table_batches`] performs around its
/// retried read.
#[cfg(test)]
fn recover_chunk_partial_updates(
    replicated_table_schema: &ReplicatedTableSchema,
    chunk: &[TrackedTableMutation],
    max_recovered_bytes: usize,
    recovery: &dyn PartialUpdateRecovery,
) -> EtlResult<(usize, RecoveredPartialRows)> {
    if !has_canonical_identity(replicated_table_schema) {
        return Ok((chunk.len(), RecoveredPartialRows::empty()));
    }

    let request = plan_partial_update_recovery(
        replicated_table_schema,
        chunk.iter().map(|tracked| &tracked.mutation),
        max_recovered_bytes,
    )?;
    if request.is_empty() {
        return Ok((chunk.len(), RecoveredPartialRows::empty()));
    }

    let outcome = recovery.recover(&request)?;

    Ok((covered_mutation_count(chunk.len(), &request, outcome.covered_keys), outcome.recovered))
}

/// Plans the single read that completes the batch's coalescable partial
/// updates.
///
/// The plan tracks identities the way [`prepare_table_mutations`] does, minus
/// the flush that the ordered partial-update path performs. It therefore
/// requests a superset of the identities the normalizer can use: a requested
/// identity the normalizer ends up not needing costs one extra key in the
/// read, while an identity the normalizer needs is always requested.
fn plan_partial_update_recovery<'a>(
    replicated_table_schema: &ReplicatedTableSchema,
    mutations: impl IntoIterator<Item = &'a TableMutation>,
    max_recovered_bytes: usize,
) -> EtlResult<PartialUpdateRecoveryRequest> {
    let mut keys = Vec::new();
    let mut columns = BTreeSet::new();
    let mut deleted = HashSet::new();
    for (mutation_index, mutation) in mutations.into_iter().enumerate() {
        match mutation {
            TableMutation::Insert(_) => {}
            TableMutation::Delete(row) => {
                deleted.insert(delete_predicate_from_row(replicated_table_schema, row)?);
            }
            TableMutation::Replace(row) => {
                deleted.insert(delete_predicate_from_row(replicated_table_schema, row)?);
            }
            TableMutation::Update { delete_row, new_row } => {
                let predicate = delete_predicate_from_row(replicated_table_schema, delete_row)?;
                let is_new_identity = deleted.insert(predicate.clone());
                let UpdatedTableRow::Partial(partial_row) = new_row else {
                    continue;
                };
                // A partial row with nothing missing needs no stored value,
                // and a repeated identity is completed in memory from the
                // first read.
                if !is_new_identity || partial_row.missing_column_indexes().is_empty() {
                    continue;
                }

                columns.extend(partial_row.missing_column_indexes().iter().copied());
                keys.push(PartialUpdateRecoveryKey {
                    predicate,
                    components: delete_key_components(replicated_table_schema, delete_row)?,
                    mutation_index,
                });
            }
        }
    }

    Ok(PartialUpdateRecoveryRequest::new(keys, columns, max_recovered_bytes))
}

/// Returns whether predicate equality is a complete identity comparison.
///
/// Predicate equality is complete only for canonical integer and UUID
/// columns. Other types retain SQL's equality semantics.
fn has_canonical_identity(replicated_table_schema: &ReplicatedTableSchema) -> bool {
    replicated_table_schema.identity_column_schemas().next().is_some()
        && replicated_table_schema.identity_column_schemas().all(|column| {
            matches!(
                column.typ,
                etl::schema::Type::INT2
                    | etl::schema::Type::INT4
                    | etl::schema::Type::INT8
                    | etl::schema::Type::UUID
            )
        })
}

/// Applies a partial row to every pending row at one identity.
///
/// Patched rows keep their position, so a later delete of the same identity
/// still removes them and a later partial update patches them again.
fn patch_pending_rows(
    replicated_table_schema: &ReplicatedTableSchema,
    rows: &mut [Option<TableRow>],
    positions: &mut HashMap<String, Vec<usize>>,
    predicate: &str,
    partial_row: &PartialTableRow,
) -> EtlResult<()> {
    for index in positions.remove(predicate).unwrap_or_default() {
        let Some(previous) = rows[index].take() else {
            continue;
        };
        let mut present = partial_row.values().iter();
        let values = previous
            .into_values()
            .into_iter()
            .enumerate()
            .map(|(column, old)| {
                if partial_row.missing_column_indexes().contains(&column) {
                    old
                } else {
                    present.next().expect("validated partial row has each present column").clone()
                }
            })
            .collect();
        let updated = TableRow::new(values);
        positions
            .entry(delete_predicate_from_row(replicated_table_schema, &updated)?)
            .or_default()
            .push(index);
        rows[index] = Some(updated);
    }

    Ok(())
}

/// Completes a partial row from the stored values of one recovered row.
fn complete_partial_row(
    replicated_table_schema: &ReplicatedTableSchema,
    partial_row: &PartialTableRow,
    recovered_columns: &[usize],
    recovered_values: &[Cell],
) -> EtlResult<TableRow> {
    let mut present = partial_row.values().iter();
    let mut values = Vec::with_capacity(partial_row.total_columns());
    for column in 0..partial_row.total_columns() {
        if !partial_row.missing_column_indexes().contains(&column) {
            let Some(value) = present.next() else {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake partial update row ended early",
                    format!(
                        "Table '{}' did not provide enough values for its partial update row",
                        replicated_table_schema.name()
                    )
                ));
            };
            values.push(value.clone());
            continue;
        }

        let Some(value) = recovered_columns
            .binary_search(&column)
            .ok()
            .and_then(|index| recovered_values.get(index))
        else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update recovery is missing a column",
                format!(
                    "Table '{}' did not read back replicated column {column}",
                    replicated_table_schema.name()
                )
            ));
        };
        values.push(value.clone());
    }

    Ok(TableRow::new(values))
}

/// Normalizes full-row CDC operations inside an existing atomic batch.
///
/// The invariant is: applying the pending deletes to the original table and
/// then appending pending rows equals executing the consumed events in order.
/// Deleting an identity removes both original rows and earlier pending rows;
/// inserting only appends. A key-changing full update deletes its OLD identity
/// and appends the NEW row without deleting existing rows at the new identity.
/// A partial update whose identity is already deleted folds the pending rows
/// it patches. Otherwise it is completed from the values `recovered` read back
/// for its identity and joins the same key set and insert run as a full row.
/// An identity with no recovered row keeps the pre-coalescing ordered
/// `UPDATE`, which is the only form that can still patch rows staged earlier
/// in this batch; that statement flushes the run, so every remaining recovered
/// value is dropped as stale and later partial updates keep the ordered form
/// too.
fn prepare_table_mutations(
    schema: &ReplicatedTableSchema,
    mutations: Vec<TableMutation>,
    mut recovered: RecoveredPartialRows,
) -> EtlResult<Vec<PreparedTableMutation>> {
    if !has_canonical_identity(schema) {
        return prepare_ordered_table_mutations(schema, mutations);
    }

    let mut prepared = Vec::new();
    let mut predicates = Vec::new();
    let mut rows: Vec<Option<TableRow>> = Vec::new();
    let mut origin = None;
    let mut key_set = DeleteKeySet::new(schema);
    let mut deleted = HashSet::new();
    let mut positions: HashMap<String, Vec<usize>> = HashMap::new();
    for mutation in mutations {
        let (delete, insert) = match mutation {
            TableMutation::Insert(row) => (None, Some(row)),
            TableMutation::Delete(old) => (
                Some((
                    delete_predicate_from_row(schema, &old)?,
                    delete_key_components(schema, &old)?,
                    "delete",
                )),
                None,
            ),
            TableMutation::Replace(row) => (
                Some((
                    delete_predicate_from_row(schema, &row)?,
                    delete_key_components(schema, &row)?,
                    "replace",
                )),
                Some(row),
            ),
            TableMutation::Update { delete_row, new_row: UpdatedTableRow::Full(row) } => (
                Some((
                    delete_predicate_from_row(schema, &delete_row)?,
                    delete_key_components(schema, &delete_row)?,
                    "update",
                )),
                Some(row),
            ),
            TableMutation::Update { delete_row, new_row: UpdatedTableRow::Partial(row) } => {
                let predicate = delete_predicate_from_row(schema, &delete_row)?;
                // Once the original identity is deleted, every surviving row
                // at that identity is known in memory. Patch those rows only;
                // an absent row must not become an insert.
                if deleted.contains(&predicate) {
                    patch_pending_rows(schema, &mut rows, &mut positions, &predicate, &row)?;
                    continue;
                }
                let has_pending_row = positions
                    .get(&predicate)
                    .is_some_and(|indexes| indexes.iter().any(|index| rows[*index].is_some()));
                let stored_row_count = recovered.rows_for(&predicate).map(<[TableRow]>::len);
                if has_pending_row && stored_row_count == Some(0) {
                    // The read covered this identity and storage holds no row
                    // for it, so every row the statement would touch was
                    // staged earlier in this batch: patch those in memory
                    // rather than order a statement for them. This is the
                    // insert-then-update shape, which would otherwise pay one
                    // ordered statement per event.
                    patch_pending_rows(schema, &mut rows, &mut positions, &predicate, &row)?;
                    continue;
                }
                if let Some(stored_rows) =
                    recovered.rows_for(&predicate).filter(|stored| !stored.is_empty())
                {
                    // Rows staged earlier in this batch are patched in place,
                    // and each stored row is re-inserted completed, so the key
                    // set removes only what storage still holds.
                    let completed = stored_rows
                        .iter()
                        .map(|stored| {
                            complete_partial_row(schema, &row, recovered.columns(), stored.values())
                        })
                        .collect::<EtlResult<Vec<_>>>()?;
                    patch_pending_rows(schema, &mut rows, &mut positions, &predicate, &row)?;
                    deleted.insert(predicate.clone());
                    predicates.push(predicate);
                    key_set.push(&delete_key_components(schema, &delete_row)?);
                    origin = Some(match origin {
                        None => "update",
                        Some("update") => "update",
                        Some(_) => "mixed",
                    });
                    for completed_row in completed {
                        positions
                            .entry(delete_predicate_from_row(schema, &completed_row)?)
                            .or_default()
                            .push(rows.len());
                        rows.push(Some(completed_row));
                    }
                    continue;
                }

                let assignments = update_assignments_from_partial_row(schema, &row)?;
                flush_full_row_writes(
                    &mut prepared,
                    &mut predicates,
                    &mut rows,
                    &mut key_set,
                    &mut origin,
                );
                positions.clear();
                deleted.clear();
                // The flushed statements change the stored rows this batch
                // read back, so no recovered value survives the barrier.
                recovered.clear();
                prepared.push(PreparedTableMutation::Update { assignments, predicate });
                continue;
            }
        };
        if let Some((predicate, key, delete_origin)) = delete {
            if let Some(indexes) = positions.remove(&predicate) {
                for index in indexes {
                    rows[index] = None;
                }
            }
            if deleted.insert(predicate.clone()) {
                predicates.push(predicate);
                key_set.push(&key);
            }
            origin = Some(match origin {
                None => delete_origin,
                Some(previous) if previous == delete_origin => previous,
                Some(_) => "mixed",
            });
        }
        if let Some(row) = insert {
            positions.entry(delete_predicate_from_row(schema, &row)?).or_default().push(rows.len());
            rows.push(Some(row));
        }
    }
    flush_full_row_writes(&mut prepared, &mut predicates, &mut rows, &mut key_set, &mut origin);
    Ok(prepared)
}

/// Preserves SQL operation order when identities cannot be compared in Rust.
fn prepare_ordered_table_mutations(
    replicated_table_schema: &ReplicatedTableSchema,
    mutations: Vec<TableMutation>,
) -> EtlResult<Vec<PreparedTableMutation>> {
    let mut prepared_mutations = Vec::new();
    let mut upsert_rows = Vec::new();
    let mut delete_predicates = Vec::new();

    for mutation in mutations {
        match mutation {
            TableMutation::Insert(row) => {
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        key_set: None,
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }
                upsert_rows.push(row);
            }
            TableMutation::Delete(row) => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                delete_predicates.push(delete_predicate_from_row(replicated_table_schema, &row)?);
            }
            TableMutation::Update { delete_row, new_row } => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        key_set: None,
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }
                match new_row {
                    UpdatedTableRow::Full(upsert_row) => {
                        prepared_mutations.push(PreparedTableMutation::Delete {
                            key_set: None,
                            predicates: vec![delete_predicate_from_row(
                                replicated_table_schema,
                                &delete_row,
                            )?],
                            origin: "update",
                        });
                        prepared_mutations
                            .push(PreparedTableMutation::Upsert(prepare_rows(vec![upsert_row])));
                    }
                    UpdatedTableRow::Partial(partial_row) => {
                        prepared_mutations.push(PreparedTableMutation::Update {
                            assignments: update_assignments_from_partial_row(
                                replicated_table_schema,
                                &partial_row,
                            )?,
                            predicate: delete_predicate_from_row(
                                replicated_table_schema,
                                &delete_row,
                            )?,
                        });
                    }
                }
            }
            TableMutation::Replace(row) => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        key_set: None,
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }

                prepared_mutations.push(PreparedTableMutation::Delete {
                    key_set: None,
                    predicates: vec![delete_predicate_from_row(replicated_table_schema, &row)?],
                    origin: "replace",
                });
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(vec![row])));
            }
        }
    }

    if !upsert_rows.is_empty() {
        prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(upsert_rows)));
    }
    if !delete_predicates.is_empty() {
        prepared_mutations.push(PreparedTableMutation::Delete {
            key_set: None,
            predicates: delete_predicates,
            origin: "delete",
        });
    }

    Ok(prepared_mutations)
}

/// Builds a `WHERE` clause from the replica-identity values stored in `row`.
fn delete_key_values<'a>(
    replicated_table_schema: &'a ReplicatedTableSchema,
    row: impl Into<DeletePredicateRowRef<'a>>,
) -> EtlResult<Vec<(&'a etl::schema::ColumnSchema, &'a Cell)>> {
    let row = row.into();
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    let identity_column_schemas: Vec<_> =
        replicated_table_schema.identity_column_schemas().collect();
    if identity_column_schemas.is_empty() {
        return Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "DuckLake delete requires a replica identity",
            format!(
                "Table '{}' has no replicated replica-identity columns",
                replicated_table_schema.name()
            )
        ));
    }

    let key_values: Vec<_> = match row {
        DeletePredicateRowRef::Full(row) => {
            if row.values().len() != replicated_column_schemas.len() {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake row shape does not match schema",
                    format!(
                        "Expected {} values for table '{}', got {}",
                        replicated_column_schemas.len(),
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                ));
            }

            let mut identity_columns = identity_column_schemas.iter().copied().peekable();
            let mut key_values = Vec::with_capacity(identity_column_schemas.len());

            for (column_schema, value) in replicated_column_schemas.iter().zip(row.values()) {
                if identity_columns.peek().is_some_and(|identity_column| {
                    identity_column.ordinal_position == column_schema.ordinal_position
                }) {
                    let Some(identity_column) = identity_columns.next() else {
                        return Err(etl_error!(
                            ErrorKind::InvalidState,
                            "DuckLake replica identity schema is inconsistent",
                            format!(
                                "Table '{}' identity columns ended unexpectedly",
                                replicated_table_schema.name()
                            )
                        ));
                    };

                    key_values.push((identity_column, value));
                }
            }

            key_values
        }
        DeletePredicateRowRef::Key(row) => {
            if row.values().len() != identity_column_schemas.len() {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake key image does not match replica identity",
                    format!(
                        "Expected {} key values for table '{}', got {}",
                        identity_column_schemas.len(),
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                ));
            }

            identity_column_schemas.iter().copied().zip(row.values()).collect()
        }
    };

    Ok(key_values)
}

/// Builds the key-set components of one identity from a row.
///
/// The SQL literal is generated once and reused for both the `VALUES` key list
/// and any range bound derived from it, so a bound never re-parses a rendered
/// literal.
fn delete_key_components<'a>(
    replicated_table_schema: &'a ReplicatedTableSchema,
    row: impl Into<DeletePredicateRowRef<'a>>,
) -> EtlResult<Vec<KeyComponent>> {
    Ok(delete_key_values(replicated_table_schema, row)?
        .into_iter()
        .map(|(_, cell)| KeyComponent::from_cell(cell, cell_to_sql_literal_ref(cell)))
        .collect())
}

/// Builds a predicate with PostgreSQL identity NULL semantics.
fn delete_predicate_from_row<'a>(
    replicated_table_schema: &'a ReplicatedTableSchema,
    row: impl Into<DeletePredicateRowRef<'a>>,
) -> EtlResult<String> {
    Ok(identity_predicate(delete_key_values(replicated_table_schema, row)?))
}

/// Builds SQL `SET` assignments from a partial update row.
fn update_assignments_from_partial_row(
    replicated_table_schema: &ReplicatedTableSchema,
    partial_row: &PartialTableRow,
) -> EtlResult<Vec<String>> {
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    if partial_row.total_columns() != replicated_column_schemas.len() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row does not match schema",
            format!(
                "Expected {} replicated columns for table '{}', got {}",
                replicated_column_schemas.len(),
                replicated_table_schema.name(),
                partial_row.total_columns()
            )
        ));
    }

    if partial_row.values().is_empty() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row has no assignments",
            format!(
                "Table '{}' emitted an empty partial update row",
                replicated_table_schema.name()
            )
        ));
    }

    if partial_row.values().len() + partial_row.missing_column_indexes().len()
        != partial_row.total_columns()
    {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row shape is inconsistent",
            format!(
                "Table '{}' partial row reports {} total columns but has {} present and {} missing",
                replicated_table_schema.name(),
                partial_row.total_columns(),
                partial_row.values().len(),
                partial_row.missing_column_indexes().len()
            )
        ));
    }

    let mut assignments = Vec::with_capacity(partial_row.values().len());
    let mut missing_indexes = partial_row.missing_column_indexes().iter().copied().peekable();
    let mut present_values = partial_row.values().iter();
    for (column_index, column_schema) in replicated_column_schemas.iter().enumerate() {
        if missing_indexes.peek().copied() == Some(column_index) {
            missing_indexes.next();
            continue;
        }

        let Some(value) = present_values.next() else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update row ended early",
                format!(
                    "Table '{}' did not provide enough values for its partial update row",
                    replicated_table_schema.name()
                )
            ));
        };
        let quoted_column =
            quote_identifier(&DUCKLAKE_COLUMN_NAME_MAPPING.map_name(&column_schema.name));
        assignments.push(format!("{quoted_column} = {}", cell_to_sql_literal_ref(value)));
    }

    if missing_indexes.next().is_some() || present_values.next().is_some() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row shape is inconsistent",
            format!(
                "Table '{}' partial row has leftover values or missing indexes after decoding",
                replicated_table_schema.name()
            )
        ));
    }

    Ok(assignments)
}

/// Builds a deterministic identity for one ordered mutation batch.
fn build_mutation_batch_identity(
    table_name: &DuckLakeTableName,
    replicated_table_schema: &ReplicatedTableSchema,
    tracked_mutations: &[TrackedTableMutation],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "mutation".hash(&mut hasher);
    table_name.id().hash(&mut hasher);

    for tracked_mutation in tracked_mutations {
        u64::from(tracked_mutation.sequence_key.commit_lsn).hash(&mut hasher);
        tracked_mutation.sequence_key.tx_ordinal.hash(&mut hasher);

        match &tracked_mutation.mutation {
            TableMutation::Insert(row) => {
                "insert".hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
            TableMutation::Delete(row) => {
                "delete".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, row)?.hash(&mut hasher);
            }
            TableMutation::Update { delete_row, new_row } => {
                "update".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, delete_row)?.hash(&mut hasher);
                match new_row {
                    UpdatedTableRow::Full(row) => hash_table_row_ref(&mut hasher, row),
                    UpdatedTableRow::Partial(row) => hash_partial_table_row_ref(&mut hasher, row)?,
                }
            }
            TableMutation::Replace(row) => {
                "replace".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, row)?.hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
        }
    }

    Ok(build_batch_identity(
        DuckLakeTableBatchKind::Mutation,
        None,
        tracked_mutations.last().map(|tracked_mutation| tracked_mutation.sequence_key.commit_lsn),
        hasher.finish(),
    ))
}

/// Builds the deterministic identity shared by retries of a copy barrier.
fn build_copy_complete_batch_identity(table_name: &DuckLakeTableName) -> DuckLakeBatchIdentity {
    let mut hasher = BatchIdHasher::new();
    "copy_complete".hash(&mut hasher);
    table_name.id().hash(&mut hasher);

    build_batch_identity(DuckLakeTableBatchKind::CopyComplete, None, None, hasher.finish())
}

/// Builds a deterministic identity for one ordered truncate batch.
fn build_truncate_batch_identity(
    table_name: &DuckLakeTableName,
    tracked_truncates: &[TrackedTruncateEvent],
) -> DuckLakeBatchIdentity {
    let mut hasher = BatchIdHasher::new();
    "truncate".hash(&mut hasher);
    table_name.id().hash(&mut hasher);

    for tracked_truncate in tracked_truncates {
        u64::from(tracked_truncate.sequence_key.commit_lsn).hash(&mut hasher);
        tracked_truncate.sequence_key.tx_ordinal.hash(&mut hasher);
        tracked_truncate.options.hash(&mut hasher);
    }

    build_batch_identity(
        DuckLakeTableBatchKind::Truncate,
        None,
        tracked_truncates.last().map(|tracked_truncate| tracked_truncate.sequence_key.commit_lsn),
        hasher.finish(),
    )
}

/// Builds the final persisted batch identity string.
fn build_batch_identity(
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    fingerprint: u64,
) -> DuckLakeBatchIdentity {
    let first_start_lsn_u64 = first_start_lsn.map(u64::from).unwrap_or_default();
    let last_commit_lsn_u64 = last_commit_lsn.map(u64::from).unwrap_or_default();

    DuckLakeBatchIdentity {
        batch_id: format!(
            "{}:{first_start_lsn_u64:016x}:{last_commit_lsn_u64:016x}:{fingerprint:016x}",
            batch_kind.as_str()
        ),
        first_start_lsn,
        last_commit_lsn,
    }
}

/// Hashes a row using its SQL literal form so retries are independent of
/// appender encoding.
fn hash_table_row_ref(hasher: &mut BatchIdHasher, row: &TableRow) {
    table_row_to_sql_literal_ref(row).hash(hasher);
}

/// Hashes a partial row using column indexes and SQL literal forms.
fn hash_partial_table_row_ref(hasher: &mut BatchIdHasher, row: &PartialTableRow) -> EtlResult<()> {
    row.total_columns().hash(hasher);
    let mut missing_indexes = row.missing_column_indexes().iter().copied().peekable();
    let mut present_values = row.values().iter();

    for column_index in 0..row.total_columns() {
        if missing_indexes.peek().copied() == Some(column_index) {
            missing_indexes.next();
            continue;
        }

        let Some(value) = present_values.next() else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial row shape is inconsistent",
                format!("Partial row ended before replicated column index {}", column_index)
            ));
        };

        column_index.hash(hasher);
        cell_to_sql_literal_ref(value).hash(hasher);
    }

    if present_values.next().is_some() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial row shape is inconsistent",
            "Partial row contained more present values than its missing indexes allow"
        ));
    }

    Ok(())
}

/// Returns whether the atomic batch marker already exists.
fn applied_batch_marker_exists(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<bool> {
    applied_batch_marker_id_exists(conn, &batch.table_name, &batch.replay_epoch, &batch.batch_id)
}

/// Returns whether one marker identity already exists.
fn applied_batch_marker_id_exists(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
    batch_id: &str,
) -> EtlResult<bool> {
    let table_id = table_name.id();
    let sql = format!(
        r#"SELECT 1 FROM {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         WHERE table_name = {}
           AND COALESCE({REPLAY_EPOCH_COLUMN}, {}) = {}
           AND batch_id = {}
         LIMIT 1;"#,
        quote_literal(&table_id),
        quote_literal(LEGACY_REPLAY_EPOCH),
        quote_literal(replay_epoch),
        quote_literal(batch_id)
    );
    let mut statement = conn.prepare(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query prepare failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    let mut rows = statement.query([]).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;

    rows.next().map(|row| row.is_some()).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query row fetch failed",
            format_query_error_detail(&sql),
            source: err
        )
    })
}

/// Inserts the atomic batch marker inside the open DuckLake transaction.
fn insert_applied_batch_marker(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let marker = PreparedDuckLakeBatchMarker {
        batch_kind: batch.batch_kind,
        first_start_lsn: batch.first_start_lsn,
        last_commit_lsn: batch.last_commit_lsn,
    };
    insert_applied_batch_marker_fields(
        conn,
        &batch.table_name,
        &batch.replay_epoch,
        &batch.batch_id,
        &marker,
    )
}

/// Inserts one compact marker inside the open DuckLake transaction.
fn insert_applied_batch_marker_fields(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
    batch_id: &str,
    marker: &PreparedDuckLakeBatchMarker,
) -> EtlResult<()> {
    let table_id = table_name.id();
    let sql = format!(
        r#"INSERT INTO {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         (table_name, replay_epoch, batch_id, batch_kind, first_start_lsn, last_commit_lsn, applied_at)
         VALUES ({}, {}, {}, {}, {}, {}, current_timestamp);"#,
        quote_literal(&table_id),
        quote_literal(replay_epoch),
        quote_literal(batch_id),
        quote_literal(marker.batch_kind.as_str()),
        optional_lsn_to_sql_literal(marker.first_start_lsn),
        optional_lsn_to_sql_literal(marker.last_commit_lsn),
    );
    conn.execute_batch(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake batch marker insert failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    Ok(())
}

/// Appends the steady-state streaming replay watermark inside the open
/// transaction.
fn update_table_streaming_progress(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let last_sequence_key = batch.last_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its last sequence key",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;
    let table_id = batch.table_name.id();
    let sql = format!(
        r#"INSERT INTO {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         (table_name, replay_epoch, last_commit_lsn, last_tx_ordinal, updated_at)
         VALUES ({}, {}, {}, {}, current_timestamp);"#,
        quote_literal(&table_id),
        quote_literal(&batch.replay_epoch),
        u64::from(last_sequence_key.commit_lsn),
        last_sequence_key.tx_ordinal,
    );
    conn.execute_batch(&sql).map_err(|err| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress update failed",
            format_query_error_detail(&sql),
            source: err
        )
    })?;
    Ok(())
}

/// Joins quoted column identifiers for insert/select lists.
fn quoted_column_list(column_names: &[String]) -> String {
    column_names
        .iter()
        .map(|column_name| quote_identifier(column_name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Reusable per-batch temp staging table for DuckLake upserts.
struct ReusableStagingTable {
    table_name: DuckLakeTableName,
    staging_name: String,
    created: bool,
    insert_column_names: Vec<String>,
}

impl ReusableStagingTable {
    /// Creates a fresh staging-table manager for one destination table.
    fn new(table_name: &DuckLakeTableName, insert_column_names: Vec<String>) -> Self {
        Self {
            table_name: table_name.clone(),
            staging_name: format!("__staging_{}", table_name.id()),
            created: false,
            insert_column_names,
        }
    }

    /// Loads one prepared row set into staging and applies it to the target
    /// table.
    fn stage_and_insert(
        &mut self,
        conn: &duckdb::Connection,
        prepared_rows: &PreparedRows,
    ) -> EtlResult<()> {
        let started = Instant::now();
        self.prepare(conn)?;
        let staging_prepare_elapsed_ms = started.elapsed().as_millis() as u64;
        let started = Instant::now();
        self.load_rows(conn, prepared_rows)?;
        info!(
            staging_prepare_elapsed_ms,
            staging_load_elapsed_ms = started.elapsed().as_millis() as u64,
            "ducklake staging loaded"
        );
        self.insert_staged_rows(conn)
    }

    /// Drops the temp staging table after the batch finishes.
    fn cleanup(&self, conn: &duckdb::Connection) {
        if !self.created {
            return;
        }

        let staging_table = quote_identifier(&self.staging_name);
        if let Err(error) = conn.execute_batch(&format!("drop table if exists {staging_table}")) {
            tracing::error!(error = %query_log_detail(&error), "error drop table staging");
        }
    }

    /// Creates the temp table once, then clears it before each reuse.
    fn prepare(&mut self, conn: &duckdb::Connection) -> EtlResult<()> {
        if self.created {
            return self.clear(conn);
        }

        self.ensure_created(conn)
    }

    /// Creates the temporary staging table without clearing existing rows.
    fn ensure_created(&mut self, conn: &duckdb::Connection) -> EtlResult<()> {
        if self.created {
            return Ok(());
        }

        #[cfg(feature = "test-utils")]
        {
            let mut counts = STAGING_TABLE_CREATIONS_BY_TABLE.lock();
            *counts.entry(self.table_name.id()).or_default() += 1;
        }

        let staging_table = quote_identifier(&self.staging_name);
        let column_list = quoted_column_list(&self.insert_column_names);
        let target_table = qualified_lake_table_name(&self.table_name);
        conn.execute_batch(&format!(
            "create or replace temp table {staging_table} as
             select {column_list} from {target_table} limit 0;"
        ))
        .map_err(|error| {
            tracing::error!(error = %query_log_detail(&error), "error creating temporary table");

            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging table creation failed",
                source: error
            )
        })?;
        self.created = true;
        Ok(())
    }

    /// Inserts every currently staged row into the destination table.
    fn insert_staged_rows(&self, conn: &duckdb::Connection) -> EtlResult<()> {
        let column_list = quoted_column_list(&self.insert_column_names);
        let target_table = qualified_lake_table_name(&self.table_name);
        let staging_table = quote_identifier(&self.staging_name);
        let sql = format!(
            "insert into {target_table} ({column_list}) select {column_list} from {staging_table};"
        );
        conn.execute_batch(&sql).map_err(|error| {
            tracing::error!(error = %query_log_detail(&error), "error inserting rows");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake INSERT SELECT failed",
                format_query_error_detail(&sql),
                source: error
            )
        })?;
        Ok(())
    }

    /// Clears all rows from an existing temporary staging table.
    fn clear(&self, conn: &duckdb::Connection) -> EtlResult<()> {
        if !self.created {
            return Ok(());
        }

        let staging_table = quote_identifier(&self.staging_name);
        let sql = format!("truncate table {staging_table};");
        conn.execute_batch(&sql).map_err(|error| {
            tracing::error!(error = %query_log_detail(&error), "error clear staging");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging table clear failed",
                source: error
            )
        })?;
        Ok(())
    }

    /// Loads one prepared row payload into the temp staging table.
    fn load_rows(&self, conn: &duckdb::Connection, prepared_rows: &PreparedRows) -> EtlResult<()> {
        match prepared_rows {
            PreparedRows::Appender(all_values) => {
                let mut appender = conn.appender(&self.staging_name).map_err(|error| {
                    tracing::error!(error = %query_log_detail(&error), "error appender");
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging appender creation failed",
                        source: error
                    )
                })?;
                for values in all_values {
                    appender.append_row(duckdb::appender_params_from_iter(values)).map_err(
                        |err| {
                            tracing::error!(error = %query_log_detail(&err), "error append row");
                            etl_error!(
                                ErrorKind::DestinationQueryFailed,
                                "DuckLake staging append_row failed",
                                source: err
                            )
                        },
                    )?;
                }
                appender.flush().map_err(|err| {
                    tracing::error!(error = %query_log_detail(&err), "error flush");
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging appender flush failed",
                        source: err
                    )
                })?;
            }
            PreparedRows::ArrowRecordBatch(record_batch) => {
                let mut appender = conn.appender(&self.staging_name).map_err(|error| {
                    tracing::error!(error = %query_log_detail(&error), "error appender");
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging appender creation failed",
                        source: error
                    )
                })?;
                appender.append_record_batch(record_batch.clone()).map_err(|err| {
                    tracing::error!(error = %query_log_detail(&err), "error append record batch");
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging append_record_batch failed",
                        source: err
                    )
                })?;
                appender.flush().map_err(|err| {
                    tracing::error!(error = %query_log_detail(&err), "error flush");
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging appender flush failed",
                        source: err
                    )
                })?;
            }
            PreparedRows::SqlLiterals(row_literals) => {
                insert_rows_into_staging_with_sql(
                    conn,
                    &self.staging_name,
                    row_literals.as_slice(),
                )?;
            }
        }
        Ok(())
    }
}

/// Connection-local accumulator for one table-copy replay epoch.
pub(super) struct DuckLakeCopyAccumulator {
    table_name: DuckLakeTableName,
    replay_epoch: String,
    staging: ReusableStagingTable,
    pending_markers: HashMap<String, PreparedDuckLakeBatchMarker>,
    staged_bytes: u64,
}

impl DuckLakeCopyAccumulator {
    /// Creates an empty accumulator shaped from its first prepared batch.
    pub(super) fn new(batch: &PreparedDuckLakeCopyBatch) -> Self {
        Self {
            table_name: batch.batch.table_name.clone(),
            replay_epoch: batch.batch.replay_epoch.clone(),
            staging: ReusableStagingTable::new(
                &batch.batch.table_name,
                batch.batch.insert_column_names.clone(),
            ),
            pending_markers: HashMap::new(),
            staged_bytes: 0,
        }
    }

    /// Returns the approximate decoded bytes awaiting a durable flush.
    pub(super) fn staged_bytes(&self) -> u64 {
        self.staged_bytes
    }

    /// Appends one prepared copy batch to the connection-local staging table.
    ///
    /// Returns `false` when the deterministic batch marker proves that this
    /// batch was already committed during the same replay epoch.
    pub(super) fn append(
        &mut self,
        conn: &duckdb::Connection,
        batch: PreparedDuckLakeCopyBatch,
    ) -> EtlResult<bool> {
        if batch.batch.table_name != self.table_name
            || batch.batch.replay_epoch != self.replay_epoch
            || batch.batch.insert_column_names != self.staging.insert_column_names
        {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake buffered copy batch does not match its staging session",
                format!("table={}", self.table_name)
            ));
        }

        if self.pending_markers.contains_key(&batch.batch.batch_id)
            || applied_batch_marker_exists(conn, &batch.batch)?
        {
            record_replayed_batch_skip(&batch.batch);
            return Ok(false);
        }

        let estimated_bytes = batch.estimated_bytes;
        let (batch_id, marker, prepared_rows) = batch.into_buffer_parts()?;
        self.staging.ensure_created(conn)?;
        self.staging.load_rows(conn, &prepared_rows)?;
        self.pending_markers.insert(batch_id, marker);
        self.staged_bytes = self.staged_bytes.saturating_add(estimated_bytes);

        Ok(true)
    }

    /// Commits all staged rows and their markers as one DuckLake change.
    ///
    /// When supplied, `copy_complete` is committed in the same transaction as
    /// the final staged window. A failure before `COMMIT` is rolled back. A
    /// `COMMIT` failure has an ambiguous outcome, so the dedicated connection
    /// and complete table-copy attempt are invalidated instead of retried.
    /// Staging is cleared only after `COMMIT` succeeds.
    pub(super) fn flush(
        &mut self,
        conn: &duckdb::Connection,
        copy_complete: Option<PreparedDuckLakeTableBatch>,
    ) -> EtlResult<()> {
        if self.pending_markers.is_empty() && copy_complete.is_none() {
            return Ok(());
        }

        let started = Instant::now();
        conn.execute_batch("begin transaction").map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake buffered copy BEGIN TRANSACTION failed",
                source: error
            )
        })?;

        let result = (|| -> EtlResult<()> {
            if !self.pending_markers.is_empty() {
                self.staging.insert_staged_rows(conn)?;
                for (batch_id, marker) in &self.pending_markers {
                    insert_applied_batch_marker_fields(
                        conn,
                        &self.table_name,
                        &self.replay_epoch,
                        batch_id,
                        marker,
                    )?;
                }
            }

            if let Some(copy_complete) = &copy_complete {
                if copy_complete.table_name != self.table_name
                    || copy_complete.replay_epoch != self.replay_epoch
                {
                    return Err(etl_error!(
                        ErrorKind::InvalidState,
                        "DuckLake copy-complete marker does not match its buffered session",
                        format!("table={}", self.table_name)
                    ));
                }
                if !applied_batch_marker_exists(conn, copy_complete)? {
                    insert_applied_batch_marker(conn, copy_complete)?;
                }
            }

            Ok(())
        })();

        if let Err(error) = result {
            if let Err(rollback_error) = conn.execute_batch("rollback") {
                tracing::error!(error = %rollback_error, "error rollback buffered copy");
            }
            return Err(error);
        }

        // A COMMIT error does not prove whether DuckLake committed. Returning
        // the error makes the shared runner mark this connection broken; the
        // owner then fences the buffer until recovery drops and recopies the
        // complete table instead of retrying an ambiguous transaction.
        conn.execute_batch("commit").map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake buffered copy COMMIT failed",
                source: error
            )
        })?;

        #[cfg(feature = "test-utils")]
        maybe_fail_after_committed_batch_for_tests(DuckLakeTableBatchKind::Copy, &self.table_name)?;

        self.staging.clear(conn)?;
        histogram!(
            ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
            BATCH_KIND_LABEL => DuckLakeTableBatchKind::Copy.as_str(),
            SUB_BATCH_KIND_LABEL => "buffered_insert",
        )
        .record(started.elapsed().as_secs_f64());
        histogram!(
            ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
            BATCH_KIND_LABEL => DuckLakeTableBatchKind::Copy.as_str(),
            SUB_BATCH_KIND_LABEL => "buffered_insert",
        )
        .record(self.pending_markers.len() as f64);
        trace!(
            table = %self.table_name,
            copy_batch_count = self.pending_markers.len(),
            staged_bytes = self.staged_bytes,
            copy_complete = copy_complete.is_some(),
            "ducklake buffered copy committed"
        );
        self.pending_markers.clear();
        self.staged_bytes = 0;

        Ok(())
    }
}

/// Applies one atomic per-table batch in a single DuckLake transaction.
fn apply_table_batch(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    let batch_started = Instant::now();
    let batch_span = tracing::info_span!(
        "ducklake_atomic_batch",
        table = %batch.table_name,
        batch_id = %batch.batch_id,
        batch_kind = batch.batch_kind.as_str(),
        first_sequence_key = %format_optional_sequence_key(batch.first_sequence_key),
        last_sequence_key = %format_optional_sequence_key(batch.last_sequence_key),
        operation_id = operation_context.operation_id(),
        timeout_ms = operation_context.timeout_ms(),
        prepared_operations = prepared_mutation_count(batch),
    );
    let _entered = batch_span.enter();
    info!("ducklake atomic batch started");

    conn.execute_batch("BEGIN TRANSACTION").map_err(|error| {
        tracing::error!(error = %query_log_detail(&error), "error transaction");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: error
        )
    })?;

    let mut reusable_staging_table =
        ReusableStagingTable::new(&batch.table_name, batch.insert_column_names.clone());
    let result = (|| -> EtlResult<()> {
        match &batch.action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
                for (operation_index, prepared_mutation) in prepared_mutations.iter().enumerate() {
                    let operation_kind = match prepared_mutation {
                        PreparedTableMutation::Upsert(_) => "insert",
                        PreparedTableMutation::Delete { .. } => "delete",
                        PreparedTableMutation::Update { .. } => "update",
                    };
                    let operation_rows = match prepared_mutation {
                        PreparedTableMutation::Upsert(rows) => prepared_rows_count(rows),
                        PreparedTableMutation::Delete { predicates, .. } => predicates.len(),
                        PreparedTableMutation::Update { .. } => 1,
                    };
                    let operation_span = tracing::info_span!(
                        "ducklake_batch_operation",
                        operation_index,
                        operation_kind,
                        operation_rows,
                    );
                    let _entered = operation_span.enter();
                    let started = Instant::now();
                    info!("ducklake batch operation started");
                    let result = apply_table_mutation(
                        conn,
                        batch,
                        prepared_mutation,
                        &mut reusable_staging_table,
                        operation_context,
                    );
                    let elapsed_ms = started.elapsed().as_millis() as u64;
                    let batch_elapsed_ms = batch_started.elapsed().as_millis() as u64;
                    if let Err(error) = &result {
                        #[cfg(feature = "ducklake-query-error-details")]
                        tracing::error!(mutation = ?prepared_mutation,
                            insert_column_names = ?batch.insert_column_names,
                            "ducklake failed operation original input");
                        warn!(elapsed_ms, batch_elapsed_ms,
                            error = %query_log_detail(error),
                            error_kind = ?error.kind(),
                            interrupt_reason = operation_context.interrupt_reason_label(),
                            "ducklake batch operation failed");
                    } else {
                        info!(elapsed_ms, batch_elapsed_ms, "ducklake batch operation completed");
                    }
                    result?;
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => {
                apply_truncate_batch_action(conn, &batch.table_name)?;
            }
        }

        info!(
            elapsed_ms = batch_started.elapsed().as_millis() as u64,
            "ducklake batch checkpoint starting"
        );
        if batch.uses_streaming_progress() {
            update_table_streaming_progress(conn, batch)?;
        } else {
            insert_applied_batch_marker(conn, batch)?;
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            info!(
                elapsed_ms = batch_started.elapsed().as_millis() as u64,
                "ducklake batch committing"
            );
            let commit_started = Instant::now();
            conn.execute_batch("COMMIT").map_err(|error| {
                tracing::error!(error = %query_log_detail(&error), commit_elapsed_ms = commit_started.elapsed().as_millis() as u64, "error commit");
                reusable_staging_table.cleanup(conn);
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake COMMIT failed",
                    source: error
                )
            })?;
            let commit_elapsed_ms = commit_started.elapsed().as_millis() as u64;
            let cleanup_started = Instant::now();
            reusable_staging_table.cleanup(conn);
            let staging_cleanup_elapsed_ms = cleanup_started.elapsed().as_millis() as u64;
            histogram!(
                ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                SUB_BATCH_KIND_LABEL => batch_log_kind(batch),
            )
            .record(batch_started.elapsed().as_secs_f64());
            histogram!(
                ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                SUB_BATCH_KIND_LABEL => batch_log_kind(batch),
            )
            .record(prepared_mutation_count(batch) as f64);
            info!(
                elapsed_ms = batch_started.elapsed().as_millis() as u64,
                table = %batch.table_name,
                batch_id = %batch.batch_id,
                batch_kind = batch.batch_kind.as_str(),
                first_start_lsn = %format_optional_lsn(batch.first_start_lsn),
                last_commit_lsn = %format_optional_lsn(batch.last_commit_lsn),
                sub_batch_kind = batch_log_kind(batch),
                insert_sub_batch_rows = apply_sub_batch_rows(batch),
                commit_elapsed_ms,
                staging_cleanup_elapsed_ms,
                "ducklake batch committed"
            );

            #[cfg(feature = "test-utils")]
            maybe_fail_after_committed_batch_for_tests(batch.batch_kind, &batch.table_name)?;

            Ok(())
        }
        Err(err) => {
            warn!(elapsed_ms = batch_started.elapsed().as_millis() as u64,
                error = %query_log_detail(&err),
                error_kind = ?err.kind(), "ducklake atomic batch rolling back");
            let rollback = conn.execute_batch("ROLLBACK");
            reusable_staging_table.cleanup(conn);
            if let Err(err) = rollback {
                tracing::error!(error = %query_log_detail(&err), "error rollback");
            }

            Err(err)
        }
    }
}

/// Applies the truncate action inside an open transaction.
fn apply_truncate_batch_action(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
) -> EtlResult<()> {
    let target_table = qualified_lake_table_name(table_name);
    let sql = format!("TRUNCATE TABLE {target_table};");
    conn.execute_batch(&sql).map_err(|error| {
        tracing::error!(error = %query_log_detail(&error), "error truncating table");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake TRUNCATE TABLE failed",
            format_query_error_detail(&sql),
            source: error
        )
    })?;
    Ok(())
}

/// Formats an optional LSN for marker-table inserts.
fn optional_lsn_to_sql_literal(lsn: Option<PgLsn>) -> String {
    lsn.map_or_else(|| "NULL".to_owned(), |value| u64::from(value).to_string())
}

/// Applies one prepared table mutation inside an open transaction.
fn apply_table_mutation(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    prepared_mutation: &PreparedTableMutation,
    reusable_staging_table: &mut ReusableStagingTable,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    match prepared_mutation {
        PreparedTableMutation::Upsert(prepared_rows) => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                PREPARED_ROWS_KIND_LABEL => prepared_rows_kind(prepared_rows),
            )
            .record(prepared_rows_count(prepared_rows) as f64);
            apply_upsert_mutation(conn, prepared_rows, reusable_staging_table)
        }
        PreparedTableMutation::Delete { predicates, origin, key_set } => {
            histogram!(
                ETL_DUCKLAKE_DELETE_PREDICATES,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                DELETE_ORIGIN_LABEL => *origin,
            )
            .record(predicates.len() as f64);
            if let Some(key_set) = key_set {
                let sql = format!(
                    "DELETE FROM {} AS cdc_target {key_set}",
                    qualified_lake_table_name(&batch.table_name)
                );
                return conn.execute_batch(&sql).map_err(|source| etl_error!(ErrorKind::DestinationQueryFailed, "DuckLake key-set delete failed", source: DuckDbSensitiveQueryError { error: source, sql }));
            }
            apply_delete_mutation(conn, batch, predicates.as_slice(), origin, operation_context)
        }
        PreparedTableMutation::Update { assignments, predicate } => apply_update_mutation(
            conn,
            &reusable_staging_table.table_name,
            assignments.as_slice(),
            predicate,
        ),
    }
}

/// Applies one upsert batch inside an open DuckLake transaction.
fn apply_upsert_mutation(
    conn: &duckdb::Connection,
    prepared_rows: &PreparedRows,
    reusable_staging_table: &mut ReusableStagingTable,
) -> EtlResult<()> {
    let row_count = prepared_rows_count(prepared_rows);

    if row_count == 0 {
        return Ok(());
    }

    reusable_staging_table.stage_and_insert(conn, prepared_rows)
}

/// Applies one delete batch inside an open DuckLake transaction.
fn apply_delete_mutation(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    predicates: &[String],
    origin: &'static str,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    if predicates.is_empty() {
        return Ok(());
    }

    let target_table = qualified_lake_table_name(&batch.table_name);
    let chunk_count = predicates.len().div_ceil(SQL_DELETE_BATCH_SIZE);
    for (chunk_index, chunk) in predicates.chunks(SQL_DELETE_BATCH_SIZE).enumerate() {
        let where_clause =
            chunk.iter().map(|predicate| format!("({predicate})")).collect::<Vec<_>>().join(" OR ");

        let sql_query = format!("DELETE FROM {target_table} WHERE {where_clause};");
        conn.execute_batch(&sql_query).map_err(|error| {
            let duckdb_interrupted = is_duckdb_interrupt_error(&error);
            let error = DuckDbSensitiveQueryError { error, sql: sql_query.clone() };
            tracing::error!(
                error = %query_log_detail(&error),
                table = %batch.table_name,
                batch_id = %batch.batch_id,
                batch_kind = batch.batch_kind.as_str(),
                first_start_lsn = %format_optional_lsn(batch.first_start_lsn),
                last_commit_lsn = %format_optional_lsn(batch.last_commit_lsn),
                first_sequence_key = %format_optional_sequence_key(batch.first_sequence_key),
                last_sequence_key = %format_optional_sequence_key(batch.last_sequence_key),
                delete_origin = origin,
                delete_predicate_count = predicates.len(),
                delete_chunk_index = chunk_index,
                delete_chunk_count = chunk_count,
                delete_chunk_predicate_count = chunk.len(),
                duckdb_interrupted,
                ducklake_interrupt_reason = operation_context.interrupt_reason_label(),
                ducklake_operation_id = operation_context.operation_id(),
                ducklake_operation_kind = operation_context.operation_kind(),
                ducklake_operation_timeout_ms = operation_context.timeout_ms(),
                "error deleting rows"
            );
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake DELETE failed",
                format_delete_mutation_error_detail(
                    &target_table,
                    predicates.len(),
                    chunk_index,
                    chunk_count,
                    chunk.len(),
                ),
                source: error
            )
        })?;
    }

    Ok(())
}

/// Applies one update statement inside an open DuckLake transaction.
fn apply_update_mutation(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    assignments: &[String],
    predicate: &str,
) -> EtlResult<()> {
    if assignments.is_empty() {
        return Ok(());
    }

    let set_clause = assignments.join(", ");
    let target_table = qualified_lake_table_name(table_name);
    let sql_query = format!("UPDATE {target_table} SET {set_clause} WHERE {predicate};");
    conn.execute_batch(&sql_query).map_err(|error| {
        let error = DuckDbSensitiveQueryError { error, sql: sql_query.clone() };
        tracing::error!(error = %query_log_detail(&error), table = %table_name, "error updating rows");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake UPDATE failed",
            format_update_mutation_error_detail(&target_table, assignments.len(), !predicate.is_empty()),
            source: error
        )
    })?;

    Ok(())
}

/// Returns the number of values carried by a prepared row payload.
fn prepared_rows_count(prepared_rows: &PreparedRows) -> usize {
    match prepared_rows {
        PreparedRows::Appender(values) => values.len(),
        PreparedRows::ArrowRecordBatch(record_batch) => record_batch.num_rows(),
        PreparedRows::SqlLiterals(values) => values.len(),
    }
}

/// Returns the encoding strategy used by a prepared row payload.
fn prepared_rows_kind(prepared_rows: &PreparedRows) -> &'static str {
    match prepared_rows {
        PreparedRows::Appender(_) => "appender",
        PreparedRows::ArrowRecordBatch(_) => "arrow_record_batch",
        PreparedRows::SqlLiterals(_) => "sql_literals",
    }
}

/// Returns the number of prepared mutation statements in one atomic batch.
fn prepared_mutation_count(batch: &PreparedDuckLakeTableBatch) -> usize {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => 1,
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => prepared_mutations.len(),
    }
}

/// Returns the insert row count when the batch is a pure insert sub-batch.
fn apply_sub_batch_rows(batch: &PreparedDuckLakeTableBatch) -> Option<usize> {
    let PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) = &batch.action else {
        return None;
    };

    if prepared_mutations.len() != 1 {
        return None;
    }

    match &prepared_mutations[0] {
        PreparedTableMutation::Upsert(prepared_rows) => Some(prepared_rows_count(prepared_rows)),
        PreparedTableMutation::Delete { .. } | PreparedTableMutation::Update { .. } => None,
    }
}

/// Classifies a prepared batch for concise INFO logging.
fn batch_log_kind(batch: &PreparedDuckLakeTableBatch) -> &'static str {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => "truncate",
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
            match prepared_mutations.as_slice() {
                [PreparedTableMutation::Upsert(_)] => "insert",
                [PreparedTableMutation::Delete { origin, .. }]
                | [
                    PreparedTableMutation::Delete { origin, .. },
                    PreparedTableMutation::Upsert(_),
                ] => origin,
                [PreparedTableMutation::Update { .. }] => "update",
                _ => "mutation",
            }
        }
    }
}

/// Inserts rows into the local staging table using SQL literals.
fn insert_rows_into_staging_with_sql(
    conn: &duckdb::Connection,
    staging: &str,
    row_literals: &[String],
) -> EtlResult<()> {
    let staging_table = quote_identifier(staging);
    for chunk in row_literals.chunks(SQL_INSERT_BATCH_SIZE) {
        let sql = format!("INSERT INTO {staging_table} VALUES {};", chunk.join(", "));
        conn.execute_batch(&sql)
            .map_err(|err| {
                let err = DuckDbSensitiveQueryError { error: err, sql: sql.clone() };
                tracing::error!(error = %query_log_detail(&err), "error insert_rows_into_staging_with_sql");
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake staging row insert failed",
                    source: err
                )
            })?;
    }

    Ok(())
}

#[cfg(feature = "test-utils")]
static FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static FAIL_AFTER_COPY_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static STAGING_TABLE_CREATIONS_BY_TABLE: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Arms a test hook that injects one post-commit failure for the next atomic
/// batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_atomic_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_owned());
}

/// Arms a test hook that injects one post-commit failure for the next copy
/// batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_copy_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_owned());
}

/// Clears DuckLake destination test hooks.
#[cfg(feature = "test-utils")]
pub fn reset_ducklake_test_hooks() {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = None;
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = None;
    STAGING_TABLE_CREATIONS_BY_TABLE.lock().clear();
}

/// Returns the number of staging-table creations performed for one table since
/// the last reset.
#[cfg(feature = "test-utils")]
pub fn ducklake_staging_table_creations_for_tests(table_name: &str) -> usize {
    STAGING_TABLE_CREATIONS_BY_TABLE.lock().get(table_name).copied().unwrap_or_default()
}

/// Injects a synthetic failure after commit so retries must rely on the correct
/// marker path.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_committed_batch_for_tests(
    batch_kind: DuckLakeTableBatchKind,
    table_name: &DuckLakeTableName,
) -> EtlResult<()> {
    match batch_kind {
        DuckLakeTableBatchKind::Copy | DuckLakeTableBatchKind::CopyComplete => {
            maybe_fail_after_copy_batch_commit_for_tests(table_name)
        }
        DuckLakeTableBatchKind::Mutation | DuckLakeTableBatchKind::Truncate => {
            maybe_fail_after_atomic_batch_commit_for_tests(table_name)
        }
    }
}

/// Injects a synthetic failure after commit so retries must rely on the
/// progress row.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_atomic_batch_commit_for_tests(table_name: &DuckLakeTableName) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name.id().as_str()) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake test hook injected post-commit failure"
        ));
    }

    Ok(())
}

/// Injects a synthetic failure after commit so copy retries must rely on the
/// marker table.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_copy_batch_commit_for_tests(table_name: &DuckLakeTableName) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name.id().as_str()) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake test hook injected copy post-commit failure"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use etl::{
        data::{OldTableRow, PartialTableRow, UpdatedTableRow},
        destination::TableCopyAttemptId,
        schema::{
            ColumnSchema, IdentityMask, ReplicatedTableSchema, ReplicationMask, TableId, TableName,
            TableSchema, Type as PgType,
        },
    };

    use super::*;
    use crate::ducklake::partial_update::AbsentStoredRows;

    #[test]
    fn sequence_key_format_preserves_fixed_width_hex_encoding() {
        let sequence_key = EventSequenceKey::new(PgLsn::from(1), 2);

        assert_eq!(format_sequence_key(sequence_key), "0000000000000001/0000000000000002");
    }

    fn make_schema() -> TableSchema {
        TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ],
        )
    }

    fn make_replicated_schema() -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(make_schema()))
    }

    fn make_replicated_schema_with_columns(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            columns,
        )))
    }

    #[test]
    fn replicated_column_names_use_ducklake_mapping() {
        let schema = make_replicated_schema_with_columns(vec![
            ColumnSchema::new("ID".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("Display_Name".to_owned(), PgType::TEXT, -1, 2, true),
        ]);

        assert_eq!(replicated_column_names(&schema), ["id", "display_name"]);
    }

    fn ducklake_table_name() -> DuckLakeTableName {
        DuckLakeTableName::new("public", "users")
    }

    fn attach_lake_catalog(conn: &duckdb::Connection) {
        conn.execute_batch("attach ':memory:' as lake;").unwrap();
    }

    fn make_prepared_batch(table_name: DuckLakeTableName) -> PreparedDuckLakeTableBatch {
        PreparedDuckLakeTableBatch {
            table_name,
            replay_epoch: LEGACY_REPLAY_EPOCH.to_owned(),
            batch_id: "test-batch".to_owned(),
            batch_kind: DuckLakeTableBatchKind::Mutation,
            first_start_lsn: None,
            last_commit_lsn: None,
            first_sequence_key: None,
            last_sequence_key: None,
            insert_column_names: vec![],
            action: PreparedDuckLakeTableBatchAction::Mutation(vec![]),
        }
    }

    fn assert_query_failure_omits_sensitive_value(
        error: &etl::error::EtlError,
        description: &'static str,
        sensitive_value: &str,
    ) {
        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(error.description(), Some(description));
        if cfg!(feature = "ducklake-query-error-details") {
            assert!(error.to_string().contains(sensitive_value));
            assert!(error.to_string().contains("SQL:"));
            return;
        }
        assert!(!error.to_string().contains(sensitive_value));
        assert!(!error.detail().is_some_and(|detail| detail.contains(sensitive_value)));
        let source = error.source().expect("expected sanitized source");
        assert!(!source.to_string().contains(sensitive_value));
        assert!(source.to_string().contains("omitted because it may contain row values"));
    }

    #[test]
    fn applied_batch_marker_exists_filters_by_replay_epoch() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            r#"create table lake."__etl_applied_table_batches" (
                 table_name varchar not null,
                 replay_epoch varchar,
                 batch_id varchar not null,
                 batch_kind varchar not null,
                 first_start_lsn ubigint,
                 last_commit_lsn ubigint,
                 applied_at timestamptz not null
               );"#,
        )
        .unwrap();
        let mut batch = make_prepared_batch(ducklake_table_name());
        batch.replay_epoch = "current".to_owned();
        let table_id = batch.table_name.id();

        conn.execute_batch(&format!(
            r#"insert into lake."__etl_applied_table_batches"
               (table_name, replay_epoch, batch_id, batch_kind, applied_at)
               values ({}, 'other', 'test-batch', 'mutation', current_timestamp);"#,
            quote_literal(&table_id)
        ))
        .unwrap();
        assert!(!applied_batch_marker_exists(&conn, &batch).unwrap());

        conn.execute_batch(&format!(
            r#"insert into lake."__etl_applied_table_batches"
               (table_name, replay_epoch, batch_id, batch_kind, applied_at)
               values ({}, 'current', 'test-batch', 'mutation', current_timestamp);"#,
            quote_literal(&table_id)
        ))
        .unwrap();
        assert!(applied_batch_marker_exists(&conn, &batch).unwrap());
    }

    #[test]
    fn applied_batch_marker_exists_treats_null_replay_epoch_as_legacy() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            r#"create table lake."__etl_applied_table_batches" (
                 table_name varchar not null,
                 replay_epoch varchar,
                 batch_id varchar not null,
                 batch_kind varchar not null,
                 first_start_lsn ubigint,
                 last_commit_lsn ubigint,
                 applied_at timestamptz not null
               );"#,
        )
        .unwrap();
        let legacy_batch = make_prepared_batch(ducklake_table_name());
        let mut current_batch = make_prepared_batch(ducklake_table_name());
        current_batch.replay_epoch = "current".to_owned();
        let table_id = legacy_batch.table_name.id();

        conn.execute_batch(&format!(
            r#"insert into lake."__etl_applied_table_batches"
               (table_name, replay_epoch, batch_id, batch_kind, applied_at)
               values ({}, null, 'test-batch', 'mutation', current_timestamp);"#,
            quote_literal(&table_id)
        ))
        .unwrap();

        assert!(applied_batch_marker_exists(&conn, &legacy_batch).unwrap());
        assert!(!applied_batch_marker_exists(&conn, &current_batch).unwrap());
    }

    #[test]
    fn ensure_helper_table_replay_epoch_column_adds_missing_column() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            r#"create table lake."__etl_applied_table_batches" (
                 table_name varchar not null,
                 batch_id varchar not null
               );"#,
        )
        .unwrap();

        assert!(
            !helper_table_has_column(&conn, APPLIED_BATCHES_TABLE, REPLAY_EPOCH_COLUMN).unwrap()
        );
        ensure_helper_table_replay_epoch_column(&conn, APPLIED_BATCHES_TABLE).unwrap();
        assert!(
            helper_table_has_column(&conn, APPLIED_BATCHES_TABLE, REPLAY_EPOCH_COLUMN).unwrap()
        );
        ensure_helper_table_replay_epoch_column(&conn, APPLIED_BATCHES_TABLE).unwrap();
    }

    #[test]
    fn read_table_streaming_progress_sequence_key_filters_by_replay_epoch() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            r#"create table lake."__etl_streaming_progress" (
                 table_name varchar not null,
                 replay_epoch varchar,
                 last_commit_lsn ubigint not null,
                 last_tx_ordinal ubigint not null,
                 updated_at timestamptz not null
               );"#,
        )
        .unwrap();
        let table_name = ducklake_table_name();
        let table_id = table_name.id();

        conn.execute_batch(&format!(
            r#"insert into lake."__etl_streaming_progress"
               (table_name, replay_epoch, last_commit_lsn, last_tx_ordinal, updated_at)
               values
                 ({0}, 'current', 20, 1, current_timestamp),
                 ({0}, 'other', 999, 0, current_timestamp),
                 ({0}, 'current', 20, 2, current_timestamp),
                 ({0}, null, 30, 0, current_timestamp);"#,
            quote_literal(&table_id)
        ))
        .unwrap();

        let current_key =
            read_table_streaming_progress_sequence_key(&conn, &table_name, "current").unwrap();
        assert_eq!(current_key, Some(EventSequenceKey::new(PgLsn::from(20), 2)));

        let legacy_key =
            read_table_streaming_progress_sequence_key(&conn, &table_name, LEGACY_REPLAY_EPOCH)
                .unwrap();
        assert_eq!(legacy_key, Some(EventSequenceKey::new(PgLsn::from(30), 0)));
    }

    #[test]
    fn staging_load_rows_appends_arrow_record_batch() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "create table staging_arrow_copy (id integer, name varchar, created_at timestamp);",
        )
        .unwrap();
        let replicated_table_schema = make_replicated_schema_with_columns(vec![
            ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false),
            ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ColumnSchema::new("created_at".to_owned(), PgType::TIMESTAMP, -1, 3, true),
        ]);
        let prepared_rows = prepare_copy_rows(
            &replicated_table_schema,
            vec![
                TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                    Cell::Timestamp(
                        chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
                            .unwrap()
                            .and_hms_opt(3, 4, 5)
                            .unwrap(),
                    ),
                ]),
                TableRow::new(vec![Cell::I32(2), Cell::Null, Cell::Null]),
            ],
        )
        .unwrap();
        assert!(matches!(prepared_rows, PreparedRows::ArrowRecordBatch(_)));

        let staging_table = ReusableStagingTable {
            table_name: ducklake_table_name(),
            staging_name: "staging_arrow_copy".to_owned(),
            created: true,
            insert_column_names: vec!["id".to_owned(), "name".to_owned(), "created_at".to_owned()],
        };

        staging_table.load_rows(&conn, &prepared_rows).unwrap();

        let count: i64 = conn
            .query_row("select count(*) from staging_arrow_copy", [], |row| row.get(0))
            .unwrap();
        let id_sum: i64 =
            conn.query_row("select sum(id) from staging_arrow_copy", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 2);
        assert_eq!(id_sum, 3);
    }

    #[test]
    fn apply_delete_mutation_failure_omits_row_values_from_detail() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let batch = make_prepared_batch(ducklake_table_name());
        let operation_context = DuckLakeBlockingOperationContext::for_tests();
        let sensitive_value = "alice@example.com";
        let predicates = vec![format!("\"email\" = '{sensitive_value}'")];

        let error = apply_delete_mutation(
            &conn,
            &batch,
            predicates.as_slice(),
            "delete",
            &operation_context,
        )
        .unwrap_err();

        assert_query_failure_omits_sensitive_value(
            &error,
            "DuckLake DELETE failed",
            sensitive_value,
        );
    }

    #[test]
    fn apply_update_mutation_failure_omits_row_values_from_detail() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let sensitive_value = "secret-token";
        let assignments = vec![format!("\"token\" = '{sensitive_value}'")];
        let predicate = format!("\"token\" = '{sensitive_value}'");

        let error = apply_update_mutation(
            &conn,
            &ducklake_table_name(),
            assignments.as_slice(),
            &predicate,
        )
        .unwrap_err();

        assert_query_failure_omits_sensitive_value(
            &error,
            "DuckLake UPDATE failed",
            sensitive_value,
        );
    }

    #[test]
    fn delete_predicate_from_row_uses_only_replica_identity_columns() {
        let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), PgType::INT4, -1, 1, false)
                    .with_primary_key(1),
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 2, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
            ],
        )));
        let row =
            TableRow::new(vec![Cell::I32(7), Cell::I32(42), Cell::String("alice".to_owned())]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"tenant_id\" = 7 AND \"id\" = 42"
        );
    }

    #[test]
    fn delete_predicate_from_row_supports_alternative_identity_without_primary_key() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 1, 0]),
        );
        let row = TableRow::new(vec![
            Cell::I32(7),
            Cell::String("alice@example.com".to_owned()),
            Cell::String("alice".to_owned()),
        ]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"email\" = 'alice@example.com'"
        );
    }

    #[test]
    fn delete_predicate_from_row_uses_full_replica_identity_columns() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 1, 1]),
        );
        let row = TableRow::new(vec![
            Cell::I32(7),
            Cell::String("alice@example.com".to_owned()),
            Cell::String("alice".to_owned()),
        ]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"id\" = 7 AND \"email\" = 'alice@example.com' AND \"name\" = 'alice'"
        );
    }

    #[test]
    fn delete_predicate_from_row_rejects_missing_replica_identity() {
        let table_schema = Arc::new(make_schema());
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 0]),
        );
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let error = delete_predicate_from_row(&replicated_table_schema, &row).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
        assert_eq!(error.description(), Some("DuckLake delete requires a replica identity"));
    }

    /// Builds a backlog with repeated keys inside one source transaction.
    fn replacement_backlog(count: u64) -> Vec<TrackedTableMutation> {
        (0..count)
            .map(|index| {
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(100), index),
                    TableMutation::Replace(TableRow::new(vec![
                        Cell::I32(i32::try_from(index % 7).unwrap()),
                        Cell::String(format!("version-{index}")),
                    ])),
                )
            })
            .collect()
    }

    #[test]
    fn default_batches_preserve_replay_and_roll_back_only_failed_batch() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            "create schema lake.public;
             create table lake.public.users (id integer, name varchar);
             create table lake.__etl_streaming_progress (
                 table_name varchar, replay_epoch varchar, last_commit_lsn ubigint,
                 last_tx_ordinal ubigint, updated_at timestamptz);",
        )
        .unwrap();
        let schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::default(),
            &schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            replacement_backlog(20),
            &AbsentStoredRows,
        )
        .unwrap();
        let context = DuckLakeBlockingOperationContext::for_tests();
        let cached = apply_table_batches(&conn, &batches[..1], &context).unwrap();
        let progress =
            read_table_streaming_progress(&conn, &ducklake_table_name(), LEGACY_REPLAY_EPOCH)
                .unwrap()
                .unwrap();
        assert_eq!(progress.last_sequence_key.tx_ordinal, 15);
        // The second batch deletes first, then fails inserting a malformed row.
        let mut failed = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::default(),
            &schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            replacement_backlog(20),
            &AbsentStoredRows,
        )
        .unwrap()
        .remove(1);
        let PreparedDuckLakeTableBatchAction::Mutation(operations) = &mut failed.action else {
            unreachable!()
        };
        operations[1] = PreparedTableMutation::Upsert(prepare_rows(vec![TableRow::new(vec![
            Cell::String("invalid-integer".to_owned()),
            Cell::Null,
        ])]));
        assert!(apply_table_batches_with_progress(&conn, &[failed], &context, cached).is_err());
        assert_eq!(
            conn.query_row("select count(*) from lake.public.users", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            7
        );
        assert_eq!(
            read_table_streaming_progress(&conn, &ducklake_table_name(), LEGACY_REPLAY_EPOCH)
                .unwrap()
                .unwrap()
                .last_sequence_key
                .tx_ordinal,
            15
        );
        // Replaying the entire source transaction skips committed work and
        // continues.
        let reloaded = apply_table_batches(&conn, &batches, &context).unwrap();
        apply_table_batches_with_progress(&conn, &batches, &context, reloaded).unwrap();
        let rows = conn
            .prepare("select id, name from lake.public.users order by id")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (0, "version-14".to_owned()),
                (1, "version-15".to_owned()),
                (2, "version-16".to_owned()),
                (3, "version-17".to_owned()),
                (4, "version-18".to_owned()),
                (5, "version-19".to_owned()),
                (6, "version-13".to_owned())
            ]
        );
        assert_eq!(
            read_table_streaming_progress(&conn, &ducklake_table_name(), LEGACY_REPLAY_EPOCH)
                .unwrap()
                .unwrap()
                .last_sequence_key
                .tx_ordinal,
            19
        );
    }

    #[test]
    fn independent_row_rewrites_use_one_delete_and_one_insert() {
        let schema = make_replicated_schema();
        for full_update in [false, true] {
            let mutations = (0..16)
                .map(|id| {
                    let row = TableRow::new(vec![Cell::I32(id), Cell::String("new".to_owned())]);
                    if full_update {
                        TableMutation::Update {
                            delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(id)])),
                            new_row: UpdatedTableRow::Full(row),
                        }
                    } else {
                        TableMutation::Replace(row)
                    }
                })
                .collect();
            let prepared =
                prepare_table_mutations(&schema, mutations, RecoveredPartialRows::empty()).unwrap();
            assert_eq!(prepared.len(), 2);
            let PreparedTableMutation::Delete { predicates, .. } = &prepared[0] else {
                panic!("expected delete");
            };
            assert_eq!(predicates.len(), 16);
            let PreparedTableMutation::Upsert(rows) = &prepared[1] else {
                panic!("expected insert");
            };
            assert_eq!(prepared_rows_count(rows), 16);
        }
    }

    #[test]
    fn interleaved_inserts_and_rewrites_share_one_delete_scan() {
        let schema = make_replicated_schema();
        let mut mutations = Vec::new();
        for id in 0..8 {
            let row =
                |value: &str| TableRow::new(vec![Cell::I32(id), Cell::String(value.to_owned())]);
            mutations.push(TableMutation::Insert(row("started")));
            mutations.push(TableMutation::Replace(row("completed")));
        }
        let prepared =
            prepare_table_mutations(&schema, mutations, RecoveredPartialRows::empty()).unwrap();
        let deletes = prepared
            .iter()
            .filter(|operation| matches!(operation, PreparedTableMutation::Delete { .. }))
            .count();
        assert_eq!(deletes, 1);
    }

    #[test]
    fn full_row_event_classes_share_one_delete_scan() {
        let schema = make_replicated_schema();
        let row = |id| TableRow::new(vec![Cell::I32(id), Cell::String("value".to_owned())]);
        let mutations = vec![
            TableMutation::Insert(row(1)),
            TableMutation::Replace(row(1)),
            TableMutation::Delete(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
            TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(2)])),
                new_row: UpdatedTableRow::Full(row(3)),
            },
            TableMutation::Replace(row(3)),
            TableMutation::Insert(row(4)),
        ];
        let prepared =
            prepare_table_mutations(&schema, mutations, RecoveredPartialRows::empty()).unwrap();
        assert_eq!(prepared.len(), 2);
        let PreparedTableMutation::Delete { predicates, .. } = &prepared[0] else {
            panic!("expected delete");
        };
        assert_eq!(predicates.len(), 3);
    }

    /// Builds mixed CDC operations, including dependencies on earlier keys.
    fn rewrite_case(kind: usize, version: usize) -> TableMutation {
        let row = |id| TableRow::new(vec![Cell::I32(id), Cell::String(format!("v{version}"))]);
        match kind {
            0 | 1 => TableMutation::Replace(row(i32::try_from(kind + 1).unwrap())),
            2 | 3 => TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(1)])),
                new_row: UpdatedTableRow::Full(row(if kind == 2 { 1 } else { 2 })),
            },
            4 => TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(1)])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(2, row(1), vec![])),
            },
            5 => TableMutation::Delete(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
            6 | 7 => TableMutation::Insert(row(i32::try_from(kind - 5).unwrap())),
            8 | 9 => TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(1)])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    2,
                    TableRow::new(vec![Cell::I32(if kind == 8 { 1 } else { 2 })]),
                    vec![1],
                )),
            },
            10 => TableMutation::Delete(OldTableRow::Full(row(2))),
            11 => TableMutation::Update {
                delete_row: OldTableRow::Full(row(2)),
                new_row: UpdatedTableRow::Full(row(1)),
            },
            12 => TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(3)])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    2,
                    TableRow::new(vec![Cell::String(format!("v{version}"))]),
                    vec![0],
                )),
            },
            13 => TableMutation::Replace(TableRow::new(vec![Cell::I32(1), Cell::Null])),
            _ => unreachable!("test case kind is in 0..14"),
        }
    }

    /// Executes the prepared operations against an in-memory table, with no
    /// external database, to compare grouped SQL with ordered single events.
    fn execute_rewrite_case(
        conn: &duckdb::Connection,
        schema: &ReplicatedTableSchema,
        kinds: &[usize],
        grouped: bool,
    ) -> Vec<(i32, Option<String>)> {
        conn.execute_batch(
            "delete from lake.public.users;
             insert into lake.public.users values (1, 'before-1'), (2, 'before-2');",
        )
        .unwrap();
        let mutations = kinds.iter().enumerate().map(|(i, kind)| rewrite_case(*kind, i));
        // Grouped preparation is interleaved with application, exactly like
        // `prepare_and_apply_mutation_table_batches` does, so every batch
        // completes its partial updates from the rows the previous batch left.
        let mutation_groups: Vec<Vec<TableMutation>> = if grouped {
            split_tracked_mutations(
                DuckLakeStreamingBatchConfig::default(),
                mutations
                    .enumerate()
                    .map(|(index, mutation)| {
                        TrackedTableMutation::new(
                            EventSequenceKey::new(PgLsn::from(100), u64::try_from(index).unwrap()),
                            mutation,
                        )
                    })
                    .collect(),
            )
            .into_iter()
            .map(|chunk| chunk.into_iter().map(|tracked| tracked.mutation).collect::<Vec<_>>())
            .collect()
        } else {
            mutations.map(|mutation| vec![mutation]).collect()
        };
        let table_name = ducklake_table_name();
        let batch = make_prepared_batch(table_name.clone());
        let mut staging =
            ReusableStagingTable::new(&batch.table_name, replicated_column_names(schema));
        for group in mutation_groups {
            let operations = if grouped {
                recover_and_prepare_table_mutations(
                    schema,
                    group,
                    &StoredRowRecovery::new(conn, &table_name, schema),
                )
                .unwrap()
            } else {
                prepare_ordered_table_mutations(schema, group).unwrap()
            };
            for operation in &operations {
                apply_table_mutation(
                    conn,
                    &batch,
                    operation,
                    &mut staging,
                    &DuckLakeBlockingOperationContext::for_tests(),
                )
                .unwrap();
            }
        }
        staging.cleanup(conn);
        conn.prepare("select id, name from lake.public.users order by id, name")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn grouped_rewrites_match_ordered_sql_for_mixed_dependencies() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            "create schema lake.public; create table lake.public.users (id integer, name varchar)",
        )
        .unwrap();
        let schema = make_replicated_schema();
        for kinds in [
            vec![6, 0, 7, 1],
            vec![0, 7, 1, 7],
            vec![0, 7, 7, 1],
            vec![0, 7, 1, 3],
            vec![0, 7, 1, 4],
            vec![0, 7, 1, 5],
        ] {
            assert_eq!(
                execute_rewrite_case(&conn, &schema, &kinds, true),
                execute_rewrite_case(&conn, &schema, &kinds, false),
                "case {kinds:?}",
            );
        }
        // Reproducible longer streams cross the 16-event atomic batch cap.
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(20260910);
        for length in [15, 16, 17, 31, 32, 33, 64] {
            for _ in 0..8 {
                let kinds: Vec<_> = (0..length).map(|_| rng.random_range(0..14)).collect();
                assert_eq!(
                    execute_rewrite_case(&conn, &schema, &kinds, true),
                    execute_rewrite_case(&conn, &schema, &kinds, false),
                    "case {kinds:?}",
                );
            }
        }
        // Enumerate repeat-key, key-change, partial-update, insert and delete
        // interactions both before and after a candidate coalesced run.
        for a in 0..14 {
            for b in 0..14 {
                for c in 0..14 {
                    let kinds = [a, b, c];
                    assert_eq!(
                        execute_rewrite_case(&conn, &schema, &kinds, true),
                        execute_rewrite_case(&conn, &schema, &kinds, false),
                        "case {kinds:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn uuid_rewrites_batch_and_repeated_identity_keeps_the_last_row() {
        let schema = make_replicated_schema_with_columns(vec![
            ColumnSchema::new("id".to_owned(), PgType::UUID, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
        ]);
        let ids = ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"];
        let mutations = [0, 1, 0].map(|i| {
            TableMutation::Replace(TableRow::new(vec![
                Cell::Uuid(ids[i].parse().unwrap()),
                Cell::String("new".to_owned()),
            ]))
        });
        let prepared =
            prepare_table_mutations(&schema, mutations.into(), RecoveredPartialRows::empty())
                .unwrap();
        assert_eq!(prepared.len(), 2);
        let PreparedTableMutation::Delete { predicates, .. } = &prepared[0] else {
            panic!("expected delete")
        };
        assert_eq!(predicates.len(), 2);
        let PreparedTableMutation::Upsert(rows) = &prepared[1] else { panic!("expected insert") };
        assert_eq!(prepared_rows_count(rows), 2);
    }

    #[test]
    fn composite_identity_types_match_ordered_sql() {
        let ids = ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"];
        let cases = [
            (PgType::INT2, "smallint", vec![Cell::I16(1), Cell::I16(2), Cell::Null]),
            (PgType::INT8, "bigint", vec![Cell::I64(i64::MIN), Cell::I64(i64::MAX), Cell::Null]),
            (
                PgType::UUID,
                "uuid",
                vec![
                    Cell::Uuid(ids[0].parse().unwrap()),
                    Cell::Uuid(ids[1].parse().unwrap()),
                    Cell::Null,
                ],
            ),
            (PgType::UUID, "uuid", vec![Cell::Null, Cell::Null, Cell::Null]),
            (PgType::FLOAT8, "double", vec![Cell::F64(0.0), Cell::F64(-0.0), Cell::F64(f64::NAN)]),
            (
                PgType::TEXT,
                "varchar collate nocase",
                vec![
                    Cell::String("a".to_owned()),
                    Cell::String("A".to_owned()),
                    Cell::String("a' ".to_owned()),
                ],
            ),
        ];
        for (typ, sql_type, keys) in cases {
            let schema = make_replicated_schema_with_columns(vec![
                ColumnSchema::new("id".to_owned(), typ, -1, 1, true).with_primary_key(1),
                ColumnSchema::new("bucket".to_owned(), PgType::INT4, -1, 2, false)
                    .with_primary_key(2),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
            ]);
            let conn = duckdb::Connection::open_in_memory().unwrap();
            attach_lake_catalog(&conn);
            conn.execute_batch(&format!(
                "create schema lake.public; create table lake.public.users (id {sql_type}, bucket \
                 integer, name varchar)",
            ))
            .unwrap();
            let mut results = Vec::new();
            for grouped in [false, true] {
                conn.execute_batch("delete from lake.public.users").unwrap();
                let row = |index: usize, bucket: i32, value: &str| {
                    TableRow::new(vec![
                        keys[index].clone(),
                        Cell::I32(bucket),
                        Cell::String(value.to_owned()),
                    ])
                };
                let key = |index: usize, bucket: i32| {
                    OldTableRow::Key(TableRow::new(vec![keys[index].clone(), Cell::I32(bucket)]))
                };
                let mutations = vec![
                    TableMutation::Insert(row(0, 0, "start")),
                    TableMutation::Replace(row(1, 0, "replace")),
                    TableMutation::Update {
                        delete_row: key(0, 0),
                        new_row: UpdatedTableRow::Full(row(2, 1, "move")),
                    },
                    TableMutation::Insert(row(2, 0, "other bucket")),
                    TableMutation::Update {
                        delete_row: key(2, 1),
                        new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                            3,
                            TableRow::new(vec![Cell::String("partial".to_owned())]),
                            vec![0, 1],
                        )),
                    },
                    TableMutation::Delete(key(1, 0)),
                    TableMutation::Replace(row(2, 1, "final")),
                ];
                let operations = if grouped {
                    prepare_table_mutations(&schema, mutations, RecoveredPartialRows::empty())
                        .unwrap()
                } else {
                    prepare_ordered_table_mutations(&schema, mutations).unwrap()
                };
                let batch = make_prepared_batch(ducklake_table_name());
                let mut staging =
                    ReusableStagingTable::new(&batch.table_name, replicated_column_names(&schema));
                for operation in &operations {
                    apply_table_mutation(
                        &conn,
                        &batch,
                        operation,
                        &mut staging,
                        &DuckLakeBlockingOperationContext::for_tests(),
                    )
                    .unwrap();
                }
                staging.cleanup(&conn);
                let result = conn
                    .prepare("select to_json(t) from lake.public.users t order by to_json(t)")
                    .unwrap()
                    .query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                results.push(result);
            }
            assert_eq!(results[0], results[1], "type {sql_type}");
        }
    }

    #[test]
    fn truncate_between_normalized_batches_preserves_replay_order() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            "create schema lake.public;
             create table lake.public.users (id integer, name varchar);
             create table lake.__etl_streaming_progress (
                 table_name varchar, replay_epoch varchar, last_commit_lsn ubigint,
                 last_tx_ordinal ubigint, updated_at timestamptz);",
        )
        .unwrap();
        let schema = make_replicated_schema();
        let make_batch = |lsn: u64| {
            prepare_mutation_table_batches(
                DuckLakeStreamingBatchConfig::default(),
                &schema,
                ducklake_table_name(),
                LEGACY_REPLAY_EPOCH.to_owned(),
                vec![TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(lsn), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(i32::try_from(lsn).unwrap()),
                        Cell::String("row".to_owned()),
                    ])),
                )],
                &AbsentStoredRows,
            )
            .unwrap()
            .remove(0)
        };
        let batches = vec![
            make_batch(100),
            prepare_truncate_table_batch(
                ducklake_table_name(),
                LEGACY_REPLAY_EPOCH.to_owned(),
                vec![TrackedTruncateEvent::new(EventSequenceKey::new(PgLsn::from(200), 0), 0)],
            ),
            make_batch(300),
        ];
        let context = DuckLakeBlockingOperationContext::for_tests();
        apply_table_batches(&conn, &batches[..2], &context).unwrap();
        assert_eq!(
            conn.query_row("select count(*) from lake.public.users", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        for _ in 0..2 {
            apply_table_batches(&conn, &batches, &context).unwrap();
            assert_eq!(
                conn.query_row("select id from lake.public.users", [], |r| r.get::<_, i32>(0))
                    .unwrap(),
                300
            );
        }
        assert_eq!(
            read_table_streaming_progress(&conn, &ducklake_table_name(), LEGACY_REPLAY_EPOCH)
                .unwrap()
                .unwrap()
                .last_sequence_key
                .commit_lsn,
            PgLsn::from(300)
        );
    }

    #[test]
    fn noncanonical_identity_rewrites_preserve_operation_order() {
        let schema = make_replicated_schema_with_columns(vec![
            ColumnSchema::new("id".to_owned(), PgType::FLOAT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
        ]);
        let mutations = [0.0, -0.0].map(|id| {
            TableMutation::Replace(TableRow::new(vec![
                Cell::F64(id),
                Cell::String("new".to_owned()),
            ]))
        });
        assert_eq!(
            prepare_table_mutations(&schema, mutations.into(), RecoveredPartialRows::empty())
                .unwrap()
                .len(),
            4
        );
    }

    #[test]
    fn prepare_table_mutations_replace_emits_delete_then_upsert() {
        let replicated_table_schema = make_replicated_schema();
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Replace(row)],
            RecoveredPartialRows::empty(),
        )
        .unwrap();

        assert_eq!(prepared.len(), 2);
        match &prepared[0] {
            PreparedTableMutation::Delete { predicates, origin, .. } => {
                assert_eq!(predicates, &vec!["\"id\" = 1".to_owned()]);
                assert_eq!(origin, &"replace");
            }
            PreparedTableMutation::Upsert(_) | PreparedTableMutation::Update { .. } => {
                panic!("expected delete first")
            }
        }
        match &prepared[1] {
            PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                assert_eq!(rows.len(), 1);
            }
            PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                panic!("expected appender payload")
            }
            PreparedTableMutation::Upsert(PreparedRows::ArrowRecordBatch(_)) => {
                panic!("expected appender payload")
            }
            PreparedTableMutation::Delete { .. } | PreparedTableMutation::Update { .. } => {
                panic!("expected upsert second")
            }
        }
    }

    #[test]
    fn prepare_table_mutations_update_emits_update_statement() {
        let replicated_table_schema = make_replicated_schema();
        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(1)])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    2,
                    TableRow::new(vec![Cell::I32(1), Cell::String("after".to_owned())]),
                    vec![],
                )),
            }],
            RecoveredPartialRows::empty(),
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec!["\"id\" = 1".to_owned(), "\"name\" = 'after'".to_owned()]
                );
                assert_eq!(predicate, "\"id\" = 1");
            }
            PreparedTableMutation::Upsert(_) | PreparedTableMutation::Delete { .. } => {
                panic!("expected update")
            }
        }
    }

    #[test]
    fn prepare_table_mutations_update_uses_alternative_identity_key_for_changed_key_update() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
                ColumnSchema::new("payload".to_owned(), PgType::TEXT, -1, 4, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 1, 0, 0]),
        );

        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::String(
                    "alice@example.com".to_owned(),
                )])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    4,
                    TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice@new.example.com".to_owned()),
                        Cell::String("ripe".to_owned()),
                    ]),
                    vec![3],
                )),
            }],
            RecoveredPartialRows::empty(),
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec![
                        "\"id\" = 1".to_owned(),
                        "\"email\" = 'alice@new.example.com'".to_owned(),
                        "\"name\" = 'ripe'".to_owned(),
                    ]
                );
                assert_eq!(predicate, "\"email\" = 'alice@example.com'");
            }
            PreparedTableMutation::Upsert(_) | PreparedTableMutation::Delete { .. } => {
                panic!("expected update")
            }
        }
    }

    #[test]
    fn prepare_table_mutations_update_uses_full_replica_identity_predicate() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
                ColumnSchema::new("payload".to_owned(), PgType::TEXT, -1, 4, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 1, 1, 1]),
        );

        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Full(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice@example.com".to_owned()),
                    Cell::String("seed".to_owned()),
                    Cell::String("toast".to_owned()),
                ])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    4,
                    TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice@example.com".to_owned()),
                        Cell::String("grown".to_owned()),
                    ]),
                    vec![3],
                )),
            }],
            RecoveredPartialRows::empty(),
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec![
                        "\"id\" = 1".to_owned(),
                        "\"email\" = 'alice@example.com'".to_owned(),
                        "\"name\" = 'grown'".to_owned(),
                    ]
                );
                assert_eq!(
                    predicate,
                    "\"id\" = 1 AND \"email\" = 'alice@example.com' AND \"name\" = 'seed' AND \
                     \"payload\" = 'toast'"
                );
            }
            PreparedTableMutation::Upsert(_) | PreparedTableMutation::Delete { .. } => {
                panic!("expected update")
            }
        }
    }

    #[test]
    fn prepare_mutation_table_batches_insert_only_uses_single_upsert_operation() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::default(),
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(20), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(20), 1),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ])),
                ),
            ],
            &AbsentStoredRows,
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch_kind, DuckLakeTableBatchKind::Mutation);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                        assert_eq!(rows.len(), 2);
                    }
                    PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                        panic!("expected appender payload")
                    }
                    PreparedTableMutation::Upsert(PreparedRows::ArrowRecordBatch(_)) => {
                        panic!("expected appender payload")
                    }
                    PreparedTableMutation::Delete { .. } | PreparedTableMutation::Update { .. } => {
                        panic!("expected upsert")
                    }
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_coalesce_insert_delete_insert() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::default(),
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(110), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(110), 1),
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ]))),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(110), 2),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_owned()),
                    ])),
                ),
            ],
            &AbsentStoredRows,
        )
        .unwrap();

        assert_eq!(batches.len(), 1);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 2);
                assert!(matches!(prepared[0], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[1],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_group_contiguous_deletes() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::default(),
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(110), 0),
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ]))),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(120), 0),
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ]))),
                ),
            ],
            &AbsentStoredRows,
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Delete { predicates, origin, .. } => {
                        assert_eq!(origin, &"delete");
                        assert_eq!(
                            predicates,
                            &vec!["\"id\" = 1".to_owned(), "\"id\" = 2".to_owned()]
                        );
                    }
                    PreparedTableMutation::Upsert(_) | PreparedTableMutation::Update { .. } => {
                        panic!("expected delete batch")
                    }
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_group_contiguous_updates() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::default(),
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(110), 0),
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("before-a".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("after-a".to_owned()),
                        ])),
                    },
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(120), 0),
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("before-b".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("after-b".to_owned()),
                        ])),
                    },
                ),
            ],
            &AbsentStoredRows,
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 2);
                assert!(matches!(prepared[0], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[1],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_split_non_inserts_at_cap() {
        let replicated_table_schema = make_replicated_schema();
        let batch_size = DuckLakeStreamingBatchConfig::default().max_rows;
        let tracked = (0..=batch_size)
            .map(|idx| {
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(200 + idx as u64), 0),
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(idx as i32),
                        Cell::String(format!("name-{idx}")),
                    ]))),
                )
            })
            .collect();
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::default(),
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            tracked,
            &AbsentStoredRows,
        )
        .unwrap();

        assert_eq!(batches.len(), 2);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), batch_size);
                }
                PreparedTableMutation::Upsert(_) | PreparedTableMutation::Update { .. } => {
                    panic!("expected delete batch")
                }
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }

        match &batches[1].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), 1);
                }
                PreparedTableMutation::Upsert(_) | PreparedTableMutation::Update { .. } => {
                    panic!("expected delete batch")
                }
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_coalesce_update_with_trailing_insert() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::default(),
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(110), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(120), 1),
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("seed".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("grown".to_owned()),
                        ])),
                    },
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(130), 2),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_owned()),
                    ])),
                ),
            ],
            &AbsentStoredRows,
        )
        .unwrap();

        assert_eq!(batches.len(), 1);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 2);
                assert!(matches!(prepared[0], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[1],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                let PreparedTableMutation::Upsert(rows) = &prepared[1] else {
                    panic!("expected insert")
                };
                assert_eq!(prepared_rows_count(rows), 2);
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn retain_mutations_after_sequence_key_drops_applied_prefix() {
        let retained = retain_mutations_after_sequence_key(
            vec![
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(110), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("one".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(120), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("two".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(130), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(3),
                        Cell::String("three".to_owned()),
                    ])),
                ),
            ],
            Some(EventSequenceKey::new(PgLsn::from(120), 0)),
        );

        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].sequence_key(), EventSequenceKey::new(PgLsn::from(130), 0));
    }

    #[test]
    fn retain_truncates_after_sequence_key_drops_applied_prefix() {
        let retained = retain_truncates_after_sequence_key(
            vec![
                TrackedTruncateEvent::new(EventSequenceKey::new(PgLsn::from(200), 0), 0),
                TrackedTruncateEvent::new(EventSequenceKey::new(PgLsn::from(200), 1), 0),
                TrackedTruncateEvent::new(EventSequenceKey::new(PgLsn::from(210), 0), 0),
            ],
            Some(EventSequenceKey::new(PgLsn::from(200), 0)),
        );

        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].sequence_key(), EventSequenceKey::new(PgLsn::from(200), 1));
        assert_eq!(retained[1].sequence_key(), EventSequenceKey::new(PgLsn::from(210), 0));
    }

    #[test]
    fn prepare_copy_table_batch_uses_propagated_id() {
        let replicated_table_schema = make_replicated_schema();
        let batch_id = TableCopyBatchId::new(TableCopyAttemptId::from_u128(1), 2);
        let expected_batch_id = batch_id.to_string();
        let prepared = prepare_copy_table_batch(
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            batch_id,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("identical".to_owned())])],
        )
        .unwrap();

        assert_eq!(prepared.batch.batch_id, expected_batch_id);
    }

    #[test]
    fn build_mutation_batch_identity_is_deterministic() {
        let replicated_table_schema = make_replicated_schema();
        let tracked = vec![
            TrackedTableMutation::new(
                EventSequenceKey::new(PgLsn::from(200), 0),
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ])),
            ),
            TrackedTableMutation::new(
                EventSequenceKey::new(PgLsn::from(200), 1),
                TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ]))),
            ),
        ];

        let table_name = ducklake_table_name();
        let first =
            build_mutation_batch_identity(&table_name, &replicated_table_schema, &tracked).unwrap();
        let second =
            build_mutation_batch_identity(&table_name, &replicated_table_schema, &tracked).unwrap();

        assert_eq!(first.batch_id, second.batch_id);
        assert_eq!(first.first_start_lsn, None);
        assert_eq!(first.last_commit_lsn, Some(PgLsn::from(200)));
    }

    #[test]
    fn build_mutation_batch_identity_changes_with_order_and_sequence_key() {
        let replicated_table_schema = make_replicated_schema();
        let table_name = ducklake_table_name();
        let original = build_mutation_batch_identity(
            &table_name,
            &replicated_table_schema,
            &[
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(200), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(200), 1),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ])),
                ),
            ],
        )
        .unwrap();
        let reordered = build_mutation_batch_identity(
            &table_name,
            &replicated_table_schema,
            &[
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(200), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventSequenceKey::new(PgLsn::from(200), 1),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
            ],
        )
        .unwrap();
        let changed_lsn = build_mutation_batch_identity(
            &table_name,
            &replicated_table_schema,
            &[TrackedTableMutation::new(
                EventSequenceKey::new(PgLsn::from(201), 0),
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ])),
            )],
        )
        .unwrap();

        assert_ne!(original.batch_id, reordered.batch_id);
        assert_ne!(original.batch_id, changed_lsn.batch_id);
    }

    #[test]
    fn build_truncate_batch_identity_changes_with_sequence_key() {
        let table_name = ducklake_table_name();
        let first = build_truncate_batch_identity(
            &table_name,
            &[TrackedTruncateEvent::new(EventSequenceKey::new(PgLsn::from(400), 0), 0)],
        );
        let second = build_truncate_batch_identity(
            &table_name,
            &[TrackedTruncateEvent::new(EventSequenceKey::new(PgLsn::from(401), 0), 0)],
        );

        assert_ne!(first.batch_id, second.batch_id);
    }
    /// Real DuckLake/Parquet benchmark; opt in explicitly and keep artifacts on
    /// the caller-selected filesystem. No PostgreSQL or object store is used.
    #[test]
    #[ignore = "local DuckLake performance experiment"]
    fn benchmark_mixed_cdc_ducklake() {
        let root = std::env::var("CDC_BENCH_DIR").unwrap();
        let schema = make_replicated_schema();
        for round in 0..3 {
            for (mode, cap) in [
                ("ordered", 16),
                ("normalized", 16),
                ("normalized", 256),
                ("normalized", 1024),
                ("normalized", 4096),
            ] {
                let run = tempfile::Builder::new()
                    .prefix(&format!("{mode}-{cap}-{round}-"))
                    .tempdir_in(&root)
                    .unwrap();
                let path = run.path().to_string_lossy().into_owned();
                std::fs::create_dir_all(&path).unwrap();
                let conn = duckdb::Connection::open_in_memory().unwrap();
                conn.execute_batch("INSTALL ducklake; LOAD ducklake; SET threads=1;").unwrap();
                conn.execute_batch(&format!(
                    "ATTACH 'ducklake:{path}/catalog.duckdb' AS lake (DATA_PATH '{path}/data', \
                     DATA_INLINING_ROW_LIMIT 0); CREATE SCHEMA lake.public; CREATE TABLE \
                     lake.public.users(id INTEGER, name VARCHAR); CREATE TABLE \
                     lake.__etl_streaming_progress(table_name VARCHAR, replay_epoch VARCHAR, \
                     last_commit_lsn UBIGINT, last_tx_ordinal UBIGINT, updated_at TIMESTAMPTZ);"
                ))
                .unwrap();
                for file in 0..10 {
                    conn.execute_batch(&format!(
                        "INSERT INTO lake.public.users SELECT i::INTEGER, 'before' FROM range({}, \
                         {}) r(i)",
                        file * 10000,
                        (file + 1) * 10000
                    ))
                    .unwrap();
                }
                conn.execute_batch("CALL lake.set_option('data_inlining_row_limit', 1000000)")
                    .unwrap();
                let version: String = conn.query_row("SELECT version()", [], |r| r.get(0)).unwrap();
                let started = std::time::Instant::now();
                let mutations = (0..2048)
                    .flat_map(|id| {
                        let row = |name: &str| {
                            TableRow::new(vec![
                                Cell::I32(100000 + id),
                                Cell::String(name.to_owned()),
                            ])
                        };
                        [
                            TableMutation::Insert(row("started")),
                            TableMutation::Replace(row("completed")),
                        ]
                    })
                    .collect::<Vec<_>>();
                let mut batches = Vec::new();
                let mut iter = mutations.into_iter();
                loop {
                    let chunk = iter.by_ref().take(cap).collect::<Vec<_>>();
                    if chunk.is_empty() {
                        break;
                    }
                    let mut batch = make_prepared_batch(ducklake_table_name());
                    batch.insert_column_names = replicated_column_names(&schema);
                    batch.last_sequence_key = Some(EventSequenceKey::new(
                        PgLsn::from(100),
                        u64::try_from(batches.len()).unwrap(),
                    ));
                    batch.action =
                        PreparedDuckLakeTableBatchAction::Mutation(if mode == "ordered" {
                            prepare_ordered_table_mutations(&schema, chunk).unwrap()
                        } else {
                            prepare_table_mutations(&schema, chunk, RecoveredPartialRows::empty())
                                .unwrap()
                        });
                    batches.push(batch);
                }
                let prepared_ms = started.elapsed().as_millis();
                let operations: usize = batches.iter().map(prepared_mutation_count).sum();
                for batch in &batches {
                    apply_table_batch(&conn, batch, &DuckLakeBlockingOperationContext::for_tests())
                        .unwrap();
                }
                let elapsed_ms = started.elapsed().as_millis();
                let counts: (i64, i64, i64) = conn
                    .query_row(
                        "SELECT count(*), count(*) FILTER (WHERE name='completed'), count(*) \
                         FILTER (WHERE name='started') FROM lake.public.users",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .unwrap();
                assert_eq!(counts, (102048, 2048, 0));
                let mismatches: i64 = conn
                    .query_row(
                        "SELECT count(*) FROM (SELECT id,name FROM lake.public.users EXCEPT ALL \
                         SELECT i::INTEGER, CASE WHEN i < 100000 THEN 'before' ELSE 'completed' \
                         END FROM range(102048) r(i))",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(mismatches, 0);
                println!(
                    "BENCH mode={mode} cap={cap} round={round} engine={version} events=4096 \
                     batches={} operations={operations} prepare_ms={prepared_ms} \
                     elapsed_ms={elapsed_ms}",
                    batches.len()
                );
            }
        }
    }

    #[test]
    fn streaming_batches_obey_byte_limit_and_regroup_after_replay() {
        let schema = make_replicated_schema();
        let make = || {
            (0..40)
                .map(|id| {
                    TrackedTableMutation::new(
                        EventSequenceKey::new(PgLsn::from(100), id),
                        TableMutation::Replace(TableRow::new(vec![
                            Cell::I32(i32::try_from(id).unwrap()),
                            Cell::String("x".repeat(1024)),
                        ])),
                    )
                })
                .collect::<Vec<_>>()
        };
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::new(1024, 1).unwrap(),
            &schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            make(),
            &AbsentStoredRows,
        )
        .unwrap();
        assert_eq!(batches.len(), 40);
        let pending = retain_mutations_after_sequence_key(
            make(),
            Some(EventSequenceKey::new(PgLsn::from(100), 15)),
        );
        let batches = prepare_mutation_table_batches(
            DuckLakeStreamingBatchConfig::new(1024, 1024 * 1024).unwrap(),
            &schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            pending,
            &AbsentStoredRows,
        )
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].first_sequence_key.unwrap().tx_ordinal, 16);
        assert_eq!(batches[0].last_sequence_key.unwrap().tx_ordinal, 39);
    }

    /// Schema shaped like a row whose large column is left out of an update:
    /// the identity, a small column, and the column Postgres omits.
    fn toast_replicated_schema() -> ReplicatedTableSchema {
        make_replicated_schema_with_columns(vec![
            ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ColumnSchema::new("payload".to_owned(), PgType::TEXT, -1, 3, true),
        ])
    }

    /// Builds the partial update emitted when the omitted column is unchanged.
    fn toast_partial_update(id: i32, name: &str) -> TableMutation {
        TableMutation::Update {
            delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(id)])),
            new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                3,
                TableRow::new(vec![Cell::I32(id), Cell::String(name.to_owned())]),
                vec![2],
            )),
        }
    }

    /// Creates a local lake table holding one data file per seeded row.
    fn seeded_toast_lake(rows: &[(i32, &str, &str)]) -> (tempfile::TempDir, duckdb::Connection) {
        let lake_dir = tempfile::tempdir().unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("install ducklake; load ducklake;").unwrap();
        conn.execute_batch(&format!(
            "attach 'ducklake:{catalog}' as lake (data_path '{data}', data_inlining_row_limit 0); \
             create schema lake.public; create table lake.public.users (id integer, name varchar, \
             payload varchar);",
            catalog = lake_dir.path().join("meta.ducklake").display(),
            data = lake_dir.path().join("data").display(),
        ))
        .unwrap();
        // Each row is committed on its own, so the recovery read has to span
        // several data files just like a long-lived CDC table does.
        for (id, name, payload) in rows {
            conn.execute_batch(&format!(
                "insert into lake.public.users values ({id}, '{name}', '{payload}');"
            ))
            .unwrap();
        }

        (lake_dir, conn)
    }

    /// Applies prepared operations to the lake table and reads every row back.
    fn apply_and_read_toast_lake(
        conn: &duckdb::Connection,
        schema: &ReplicatedTableSchema,
        operations: &[PreparedTableMutation],
    ) -> Vec<(i32, String, String)> {
        let batch = make_prepared_batch(ducklake_table_name());
        let mut staging =
            ReusableStagingTable::new(&batch.table_name, replicated_column_names(schema));
        for operation in operations {
            apply_table_mutation(
                conn,
                &batch,
                operation,
                &mut staging,
                &DuckLakeBlockingOperationContext::for_tests(),
            )
            .unwrap();
        }
        staging.cleanup(conn);

        conn.prepare("select id, name, payload from lake.public.users order by id, name")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    /// Fails the test if the normalizer reads stored rows.
    struct UnexpectedRecovery;

    impl PartialUpdateRecovery for UnexpectedRecovery {
        fn recover(
            &self,
            _request: &PartialUpdateRecoveryRequest,
        ) -> EtlResult<PartialUpdateRecoveryOutcome> {
            panic!("the batch must not read stored rows")
        }
    }

    #[test]
    fn partial_updates_of_distinct_keys_use_one_delete_and_one_insert() {
        let schema = toast_replicated_schema();
        let seed: Vec<(i32, &str, &str)> = (0..8).map(|id| (id, "before", "toasted")).collect();
        let (_lake_dir, conn) = seeded_toast_lake(&seed);
        let table_name = ducklake_table_name();

        let prepared = recover_and_prepare_table_mutations(
            &schema,
            (0..8).map(|id| toast_partial_update(id, "after")).collect(),
            &StoredRowRecovery::new(&conn, &table_name, &schema),
        )
        .unwrap();

        // One key-set delete plus one staged insert, whatever the event count.
        assert_eq!(prepared.len(), 2);
        let PreparedTableMutation::Delete { predicates, key_set, origin } = &prepared[0] else {
            panic!("expected delete");
        };
        assert_eq!(predicates.len(), 8);
        assert_eq!(origin, &"update");
        assert!(key_set.is_some());
        let PreparedTableMutation::Upsert(rows) = &prepared[1] else { panic!("expected insert") };
        assert_eq!(prepared_rows_count(rows), 8);

        // The omitted column keeps its stored value in every completed row.
        assert_eq!(
            apply_and_read_toast_lake(&conn, &schema, &prepared),
            (0..8).map(|id| (id, "after".to_owned(), "toasted".to_owned())).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repeated_partial_updates_of_one_key_apply_in_event_order() {
        let schema = toast_replicated_schema();
        let (_lake_dir, conn) = seeded_toast_lake(&[(1, "before", "toasted")]);
        let table_name = ducklake_table_name();

        let prepared = recover_and_prepare_table_mutations(
            &schema,
            vec![
                toast_partial_update(1, "first"),
                toast_partial_update(1, "second"),
                toast_partial_update(1, "third"),
            ],
            &StoredRowRecovery::new(&conn, &table_name, &schema),
        )
        .unwrap();

        assert_eq!(prepared.len(), 2);
        let PreparedTableMutation::Delete { predicates, .. } = &prepared[0] else {
            panic!("expected delete");
        };
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            apply_and_read_toast_lake(&conn, &schema, &prepared),
            vec![(1, "third".to_owned(), "toasted".to_owned())]
        );
    }

    #[test]
    fn partial_update_of_a_rewritten_key_folds_in_memory_without_reading() {
        let schema = toast_replicated_schema();
        let row = TableRow::new(vec![
            Cell::I32(1),
            Cell::String("inserted".to_owned()),
            Cell::String("fresh".to_owned()),
        ]);

        let prepared = recover_and_prepare_table_mutations(
            &schema,
            vec![
                TableMutation::Delete(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
                TableMutation::Insert(row),
                toast_partial_update(1, "patched"),
            ],
            &UnexpectedRecovery,
        )
        .unwrap();

        assert_eq!(prepared.len(), 2);
        let PreparedTableMutation::Upsert(rows) = &prepared[1] else { panic!("expected insert") };
        assert_eq!(prepared_rows_count(rows), 1);

        let (_lake_dir, conn) = seeded_toast_lake(&[(1, "before", "toasted")]);
        assert_eq!(
            apply_and_read_toast_lake(&conn, &schema, &prepared),
            vec![(1, "patched".to_owned(), "fresh".to_owned())]
        );
    }

    #[test]
    fn partial_update_of_an_absent_key_keeps_the_ordered_update() {
        let schema = toast_replicated_schema();
        let (_lake_dir, conn) = seeded_toast_lake(&[(1, "before", "toasted")]);
        let table_name = ducklake_table_name();

        let prepared = recover_and_prepare_table_mutations(
            &schema,
            vec![toast_partial_update(2, "after")],
            &StoredRowRecovery::new(&conn, &table_name, &schema),
        )
        .unwrap();

        // A key with no stored row must not become an insert, so it keeps the
        // ordered statement, which matches no row and leaves the table alone.
        assert_eq!(prepared.len(), 1);
        let PreparedTableMutation::Update { predicate, .. } = &prepared[0] else {
            panic!("expected update");
        };
        assert_eq!(predicate, "\"id\" = 2");
        assert_eq!(
            apply_and_read_toast_lake(&conn, &schema, &prepared),
            vec![(1, "before".to_owned(), "toasted".to_owned())]
        );
    }

    #[test]
    fn mixed_batch_with_partial_updates_matches_sequential_execution() {
        let schema = toast_replicated_schema();
        let seed: Vec<(i32, &str, &str)> = vec![
            (1, "before-1", "toast-1"),
            (2, "before-2", "toast-2"),
            (3, "before-3", "toast-3"),
        ];
        let row = |id: i32, name: &str, payload: &str| {
            TableRow::new(vec![
                Cell::I32(id),
                Cell::String(name.to_owned()),
                Cell::String(payload.to_owned()),
            ])
        };
        let mutations = || {
            vec![
                TableMutation::Insert(row(4, "inserted", "fresh")),
                toast_partial_update(1, "patched-1"),
                TableMutation::Delete(OldTableRow::Key(TableRow::new(vec![Cell::I32(2)]))),
                TableMutation::Replace(row(3, "replaced-3", "replaced-toast")),
                toast_partial_update(3, "patched-3"),
                toast_partial_update(4, "patched-4"),
                TableMutation::Update {
                    delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(1)])),
                    new_row: UpdatedTableRow::Full(row(1, "final-1", "final-toast")),
                },
            ]
        };

        let (_grouped_dir, grouped_conn) = seeded_toast_lake(&seed);
        let table_name = ducklake_table_name();
        let grouped = recover_and_prepare_table_mutations(
            &schema,
            mutations(),
            &StoredRowRecovery::new(&grouped_conn, &table_name, &schema),
        )
        .unwrap();
        let grouped_rows = apply_and_read_toast_lake(&grouped_conn, &schema, &grouped);

        let (_ordered_dir, ordered_conn) = seeded_toast_lake(&seed);
        let mut ordered_rows = Vec::new();
        for mutation in mutations() {
            let operations = prepare_ordered_table_mutations(&schema, vec![mutation]).unwrap();
            ordered_rows = apply_and_read_toast_lake(&ordered_conn, &schema, &operations);
        }

        assert_eq!(grouped_rows, ordered_rows);
    }

    #[test]
    fn partial_update_of_a_key_inserted_in_the_same_batch_stays_coalesced() {
        let schema = toast_replicated_schema();
        let (_lake_dir, conn) = seeded_toast_lake(&[(9, "other", "other-toast")]);
        let table_name = ducklake_table_name();
        let row = TableRow::new(vec![
            Cell::I32(1),
            Cell::String("inserted".to_owned()),
            Cell::String("fresh".to_owned()),
        ]);

        let prepared = recover_and_prepare_table_mutations(
            &schema,
            vec![TableMutation::Insert(row), toast_partial_update(1, "patched")],
            &StoredRowRecovery::new(&conn, &table_name, &schema),
        )
        .unwrap();

        // The read proves storage holds no row for the identity, so the only
        // row the statement could touch is the one staged by this batch and
        // the insert-then-update shape stays a single staged insert.
        assert_eq!(prepared.len(), 1);
        let PreparedTableMutation::Upsert(rows) = &prepared[0] else { panic!("expected insert") };
        assert_eq!(prepared_rows_count(rows), 1);
        assert_eq!(
            apply_and_read_toast_lake(&conn, &schema, &prepared),
            vec![
                (1, "patched".to_owned(), "fresh".to_owned()),
                (9, "other".to_owned(), "other-toast".to_owned()),
            ]
        );
    }

    #[test]
    fn repeated_partial_updates_of_an_inserted_key_apply_in_event_order() {
        let schema = toast_replicated_schema();
        let (_lake_dir, conn) = seeded_toast_lake(&[]);
        let table_name = ducklake_table_name();
        let row = TableRow::new(vec![
            Cell::I32(1),
            Cell::String("inserted".to_owned()),
            Cell::String("fresh".to_owned()),
        ]);

        let prepared = recover_and_prepare_table_mutations(
            &schema,
            vec![
                TableMutation::Insert(row),
                toast_partial_update(1, "first"),
                toast_partial_update(1, "second"),
            ],
            &StoredRowRecovery::new(&conn, &table_name, &schema),
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        assert!(
            !prepared
                .iter()
                .any(|operation| matches!(operation, PreparedTableMutation::Update { .. }))
        );
        assert_eq!(
            apply_and_read_toast_lake(&conn, &schema, &prepared),
            vec![(1, "second".to_owned(), "fresh".to_owned())]
        );
    }

    #[test]
    fn recovered_rows_respect_the_streaming_byte_bound() {
        let schema = toast_replicated_schema();
        let payload = "x".repeat(64 * 1024);
        let seed: Vec<(i32, &str, &str)> =
            (0..8).map(|id| (id, "before", payload.as_str())).collect();
        let (_lake_dir, conn) = seeded_toast_lake(&seed);
        let table_name = ducklake_table_name();
        // Room for a couple of recovered rows per batch, so the events have to
        // be spread over several batches even though they are tiny.
        let config = DuckLakeStreamingBatchConfig::new(1024, 3 * payload.len()).unwrap();
        let mut pending: VecDeque<Vec<TrackedTableMutation>> = split_tracked_mutations(
            config,
            (0..8)
                .map(|id| {
                    TrackedTableMutation::new(
                        EventSequenceKey::new(PgLsn::from(100), u64::try_from(id).unwrap()),
                        toast_partial_update(id, "after"),
                    )
                })
                .collect(),
        )
        .into();
        assert_eq!(pending.len(), 1, "the partial events themselves fit one batch");

        let mut batch_count = 0;
        let mut rows = Vec::new();
        while let Some(mut chunk) = pending.pop_front() {
            let (covered, recovered) = recover_chunk_partial_updates(
                &schema,
                &chunk,
                recovery_byte_budget(config, &chunk),
                &StoredRowRecovery::new(&conn, &table_name, &schema),
            )
            .unwrap();
            if covered < chunk.len() {
                pending.push_front(chunk.split_off(covered));
            }
            let mut prepared_batches = Vec::new();
            push_prepared_mutation_batch(
                &mut prepared_batches,
                &schema,
                &table_name,
                LEGACY_REPLAY_EPOCH,
                chunk,
                recovered,
            )
            .unwrap();
            let batch = prepared_batches.pop().expect("every chunk prepares one batch");
            let PreparedDuckLakeTableBatchAction::Mutation(operations) = &batch.action else {
                panic!("expected row mutations");
            };
            let completed_rows = operations
                .iter()
                .filter_map(|operation| match operation {
                    PreparedTableMutation::Upsert(rows) => Some(prepared_rows_count(rows)),
                    PreparedTableMutation::Delete { .. } | PreparedTableMutation::Update { .. } => {
                        None
                    }
                })
                .sum::<usize>();
            // One batch overshoots its budget by at most the row that crossed
            // it, because the read sizes each statement from observed bytes.
            assert!(
                completed_rows * payload.len() <= config.max_bytes + payload.len(),
                "batch completed {completed_rows} rows of {} bytes",
                payload.len()
            );
            rows = apply_and_read_toast_lake(&conn, &schema, operations);
            batch_count += 1;
        }

        assert!(batch_count > 1, "the recovered rows must be split over several batches");
        assert_eq!(
            rows,
            (0..8).map(|id| (id, "after".to_owned(), payload.clone())).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn recovery_read_retries_a_transient_failure() {
        let attempts = std::cell::Cell::new(0usize);
        let outcome = retry_recovery_read(ducklake_table_name(), 3, || {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            async move {
                if attempt == 1 {
                    return Err(etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake partial update recovery failed"
                    ));
                }

                Ok(PartialUpdateRecoveryOutcome {
                    recovered: RecoveredPartialRows::empty(),
                    covered_keys: 3,
                })
            }
        })
        .await
        .unwrap();

        assert_eq!(attempts.get(), 2);
        assert_eq!(outcome.covered_keys, 3);
    }

    #[tokio::test]
    async fn recovery_read_stops_retrying_on_shutdown() {
        let attempts = std::cell::Cell::new(0usize);
        let error = retry_recovery_read(ducklake_table_name(), 1, || {
            attempts.set(attempts.get() + 1);
            async move {
                Err::<PartialUpdateRecoveryOutcome, _>(etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "DuckLake shutdown requested"
                ))
            }
        })
        .await
        .unwrap_err();

        assert_eq!(attempts.get(), 1);
        assert_eq!(error.kind(), ErrorKind::DestinationConnectionFailed);
    }
}
