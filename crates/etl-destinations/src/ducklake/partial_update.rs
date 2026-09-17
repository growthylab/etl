//! Bulk recovery of the columns a partial update leaves out.
//!
//! Postgres omits unchanged TOASTed columns from an `UPDATE` image, so the
//! destination receives a partial row. Applying such a row as its own ordered
//! `UPDATE ... WHERE` statement costs one remote statement per event, which on
//! a table that is updated on every request does not finish inside the
//! foreground query timeout.
//!
//! This module reads the missing values for every partial update of one batch
//! with a single key-set query, so the batch normalizer can complete those rows
//! in memory and fold them into the same batched `DELETE` plus staged `INSERT`
//! the full-row paths already use.

#[cfg(test)]
use std::time::Instant;
use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use duckdb::types::{TimeUnit, Value};
use etl::{
    data::{ArrayCell, Cell, PgNumeric, PgTimeTz, SizeHint, TableRow},
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::{ColumnSchema, ReplicatedTableSchema, Type, is_array_type},
};
#[cfg(test)]
use tracing::info;

use crate::ducklake::{
    DUCKLAKE_COLUMN_NAME_MAPPING, DuckLakeTableName,
    encoding::cell_to_sql_literal_ref,
    key_set::{DeleteKeySet, KeyComponent},
    sql::{qualified_lake_table_name, quote_identifier},
};

/// Maximum identities read back by one recovery statement.
///
/// The batch that owns the request is already bounded upstream by the
/// streaming batch row and byte caps, so this only keeps one statement's
/// `VALUES` list from growing with an unusually large cap. No state outlives
/// the request.
const RECOVERY_KEY_BATCH_SIZE: usize = 1024;
/// Identities read by the first statement of one recovery.
///
/// The first statement only measures: one identity is enough to size every
/// later statement from observed bytes, and a request whose stored rows turn
/// out to be large then overshoots its byte budget by at most one row.
const RECOVERY_PROBE_KEY_BATCH_SIZE: usize = 1;
/// Elapsed time one recovery statement aims to stay under.
///
/// The byte budget alone says nothing about how long a statement runs: the
/// same identities cost milliseconds on a compacted table and minutes on one
/// with tens of thousands of small data files. Sizing later statements from
/// the seconds per identity already observed keeps one statement well inside
/// the caller's timeout, so a slow table degrades into more, smaller reads
/// instead of one read that never returns.
const RECOVERY_CHUNK_TARGET_ELAPSED: Duration = Duration::from_secs(30);

/// Sizes the statements of one recovery from what the earlier ones cost.
///
/// The pacer carries the observed bytes and elapsed time per identity plus an
/// upper bound that a timed-out statement halves. A caller that runs each
/// statement under its own timeout can therefore retry a timeout with a
/// strictly smaller statement instead of repeating the one that timed out.
#[derive(Debug)]
pub(super) struct RecoveryChunkPacer {
    /// Upper bound on the identities of one statement.
    limit: usize,
    /// Identities covered by the statements observed so far.
    covered_keys: usize,
    /// Bytes those statements recovered.
    recovered_bytes: usize,
    /// Time those statements took.
    elapsed: Duration,
}

impl Default for RecoveryChunkPacer {
    fn default() -> Self {
        Self {
            limit: RECOVERY_KEY_BATCH_SIZE,
            covered_keys: 0,
            recovered_bytes: 0,
            elapsed: Duration::ZERO,
        }
    }
}

impl RecoveryChunkPacer {
    /// Returns how many further identities the next statement may cover.
    ///
    /// The first statement probes with a single identity; later ones take the
    /// smaller of what the remaining byte budget and the elapsed-time target
    /// allow, and never more than the current limit.
    pub(super) fn next_chunk_size(&self, remaining_bytes: usize) -> usize {
        if self.covered_keys == 0 {
            return RECOVERY_PROBE_KEY_BATCH_SIZE.min(self.limit);
        }

        let by_bytes = match self.recovered_bytes {
            0 => RECOVERY_KEY_BATCH_SIZE,
            recovered_bytes => remaining_bytes / recovered_bytes.div_ceil(self.covered_keys),
        };
        let by_elapsed = match self.elapsed {
            Duration::ZERO => RECOVERY_KEY_BATCH_SIZE,
            elapsed => {
                let per_key = elapsed.as_nanos().div_ceil(self.covered_keys as u128).max(1);
                usize::try_from(RECOVERY_CHUNK_TARGET_ELAPSED.as_nanos() / per_key)
                    .unwrap_or(RECOVERY_KEY_BATCH_SIZE)
            }
        };

        by_bytes.min(by_elapsed).clamp(1, self.limit)
    }

    /// Records what one completed statement covered and cost.
    pub(super) fn observe(&mut self, keys: usize, recovered_bytes: usize, elapsed: Duration) {
        self.covered_keys = self.covered_keys.saturating_add(keys);
        self.recovered_bytes = self.recovered_bytes.saturating_add(recovered_bytes);
        self.elapsed = self.elapsed.saturating_add(elapsed);
    }

    /// Halves the statement limit after a timeout.
    ///
    /// Returns whether the limit actually shrank: a statement covering a single
    /// identity that still times out cannot be made smaller, and the caller
    /// must surface that failure rather than repeat it.
    pub(super) fn shrink(&mut self) -> bool {
        if self.limit <= 1 {
            return false;
        }
        self.limit /= 2;
        // The observations came from statements that were allowed to be larger,
        // so they would immediately size the next one back up.
        self.covered_keys = 0;
        self.recovered_bytes = 0;
        self.elapsed = Duration::ZERO;

        true
    }

    /// Returns the current upper bound on one statement's identities.
    pub(super) fn limit(&self) -> usize {
        self.limit
    }
}

/// Builds the identity predicate shared by deletes and recovered rows.
///
/// Both the normalizer and the recovery read derive their key from the same
/// renderer, so a recovered row maps back to exactly the predicate string the
/// normalizer tracks.
pub(super) fn identity_predicate<'a>(
    key_values: impl IntoIterator<Item = (&'a ColumnSchema, &'a Cell)>,
) -> String {
    let mut predicates = Vec::new();
    for (column_schema, value) in key_values {
        let quoted_column =
            quote_identifier(&DUCKLAKE_COLUMN_NAME_MAPPING.map_name(&column_schema.name));
        predicates.push(match value {
            Cell::Null => format!("{quoted_column} IS NULL"),
            _ => format!("{quoted_column} = {}", cell_to_sql_literal_ref(value)),
        });
    }

    predicates.join(" AND ")
}

/// One identity whose stored row completes a coalesced partial update.
pub(super) struct PartialUpdateRecoveryKey {
    /// Identity predicate as tracked by the batch normalizer.
    pub(super) predicate: String,
    /// Key-set components of the same identity.
    pub(super) components: Vec<KeyComponent>,
    /// Index of the first mutation in the batch that needs this identity.
    ///
    /// A read that stops at its byte budget leaves the events from this index
    /// on for the next batch, which reads again after this one commits.
    pub(super) mutation_index: usize,
}

/// The single read that completes every coalesced partial update of a batch.
pub(super) struct PartialUpdateRecoveryRequest {
    /// Identities to read, in first-seen order and without duplicates.
    keys: Vec<PartialUpdateRecoveryKey>,
    /// Replicated-column indexes missing from at least one partial row.
    columns: Vec<usize>,
    /// Recovered bytes after which the read stops covering more identities.
    max_recovered_bytes: usize,
}

impl PartialUpdateRecoveryRequest {
    /// Creates a request from identities, the union of their missing columns,
    /// and the byte budget the recovered values may add to one batch.
    pub(super) fn new(
        keys: Vec<PartialUpdateRecoveryKey>,
        columns: BTreeSet<usize>,
        max_recovered_bytes: usize,
    ) -> Self {
        Self { keys, columns: columns.into_iter().collect(), max_recovered_bytes }
    }

    /// Returns the identities to read.
    pub(super) fn keys(&self) -> &[PartialUpdateRecoveryKey] {
        &self.keys
    }

    /// Returns the replicated-column indexes to read, in ascending order.
    pub(super) fn columns(&self) -> &[usize] {
        &self.columns
    }

    /// Returns the bytes the recovered values of this request may add.
    pub(super) fn max_recovered_bytes(&self) -> usize {
        self.max_recovered_bytes
    }

    /// Returns whether the request would read nothing.
    pub(super) fn is_empty(&self) -> bool {
        self.keys.is_empty() || self.columns.is_empty()
    }
}

/// Stored rows read back for one recovery request.
///
/// A covered identity always has an entry, so an empty entry means the row is
/// absent from storage rather than uncovered.
#[derive(Debug, Default)]
pub(super) struct RecoveredPartialRows {
    /// Replicated-column indexes each stored row carries, in ascending order.
    columns: Vec<usize>,
    /// Stored values per covered identity predicate.
    rows: HashMap<String, Vec<TableRow>>,
}

impl RecoveredPartialRows {
    /// Creates a result that carries no recovered identity.
    pub(super) fn empty() -> Self {
        Self::default()
    }

    /// Creates an empty result for the columns of one request.
    fn for_request(request: &PartialUpdateRecoveryRequest) -> Self {
        Self { columns: request.columns().to_vec(), rows: HashMap::new() }
    }

    /// Folds the identities of a later statement of the same recovery in.
    ///
    /// Both sides carry the columns of one request, and each statement covers
    /// a disjoint range of its identities, so no entry is ever overwritten.
    pub(super) fn merge(&mut self, other: Self) {
        if self.columns.is_empty() {
            self.columns = other.columns;
        }
        self.rows.extend(other.rows);
    }

    /// Returns the replicated-column indexes each stored row carries.
    pub(super) fn columns(&self) -> &[usize] {
        &self.columns
    }

    /// Returns the stored rows of one identity, or [`None`] when the identity
    /// is not covered by the read.
    pub(super) fn rows_for(&self, predicate: &str) -> Option<&[TableRow]> {
        self.rows.get(predicate).map(Vec::as_slice)
    }

    /// Drops every recovered identity.
    ///
    /// Callers use this once a statement that changes stored rows is ordered
    /// ahead of the remaining events, which makes the read-back values stale.
    pub(super) fn clear(&mut self) {
        self.columns.clear();
        self.rows.clear();
    }

    /// Marks one identity as covered by the read.
    fn cover(&mut self, predicate: &str) {
        self.rows.entry(predicate.to_owned()).or_default();
    }

    /// Records one stored row for an identity that is covered.
    fn push(&mut self, predicate: &str, row: TableRow) {
        if let Some(rows) = self.rows.get_mut(predicate) {
            rows.push(row);
        }
    }
}

/// Result of one recovery read.
#[derive(Debug)]
pub(super) struct PartialUpdateRecoveryOutcome {
    /// Stored rows for the identities the read covered.
    pub(super) recovered: RecoveredPartialRows,
    /// Number of leading requested identities the read covered.
    ///
    /// A read stops early only when the recovered values reach the request's
    /// byte budget, so the uncovered identities are always a suffix.
    pub(super) covered_keys: usize,
}

/// Reads the stored values a batch of partial updates leaves out.
///
/// Production reads one statement at a time through
/// [`StoredRowRecovery::recover_range`], so that each statement is bounded by
/// its own timeout. This whole-request form drives the same steps
/// synchronously for tests that prepare a batch without a connection pool.
#[cfg(test)]
pub(super) trait PartialUpdateRecovery {
    /// Returns the stored rows for the identities the read covers.
    fn recover(
        &self,
        request: &PartialUpdateRecoveryRequest,
    ) -> EtlResult<PartialUpdateRecoveryOutcome>;
}

/// Reports every requested identity as absent from storage.
///
/// Partial updates then keep the pre-coalescing ordered `UPDATE` path, which
/// is what tests that drive the normalizer without a destination table need.
#[cfg(test)]
pub(super) struct AbsentStoredRows;

#[cfg(test)]
impl PartialUpdateRecovery for AbsentStoredRows {
    fn recover(
        &self,
        request: &PartialUpdateRecoveryRequest,
    ) -> EtlResult<PartialUpdateRecoveryOutcome> {
        let mut recovered = RecoveredPartialRows::for_request(request);
        for key in request.keys() {
            recovered.cover(&key.predicate);
        }

        Ok(PartialUpdateRecoveryOutcome { recovered, covered_keys: request.keys().len() })
    }
}

/// Reads stored rows from the destination table over a DuckDB connection.
pub(super) struct StoredRowRecovery<'a> {
    conn: &'a duckdb::Connection,
    table_name: &'a DuckLakeTableName,
    replicated_table_schema: &'a ReplicatedTableSchema,
}

impl<'a> StoredRowRecovery<'a> {
    /// Creates a recovery bound to one destination table.
    pub(super) fn new(
        conn: &'a duckdb::Connection,
        table_name: &'a DuckLakeTableName,
        replicated_table_schema: &'a ReplicatedTableSchema,
    ) -> Self {
        Self { conn, table_name, replicated_table_schema }
    }

    /// Reads one chunk of identities into `recovered`.
    ///
    /// Returns the recovered bytes this chunk added.
    fn recover_chunk(
        &self,
        keys: &[PartialUpdateRecoveryKey],
        identity_columns: &[&ColumnSchema],
        recovered_columns: &[&ColumnSchema],
        recovered: &mut RecoveredPartialRows,
    ) -> EtlResult<usize> {
        let mut key_set = DeleteKeySet::new(self.replicated_table_schema);
        for key in keys {
            key_set.push(&key.components);
            recovered.cover(&key.predicate);
        }
        let Some(join_clause) = key_set.take_join_clause() else {
            return Ok(0);
        };

        let select_list = identity_columns
            .iter()
            .chain(recovered_columns.iter())
            .map(|column| read_expression(column))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {select_list} FROM {} AS cdc_target{join_clause};",
            qualified_lake_table_name(self.table_name)
        );

        let mut statement = self.conn.prepare(&sql).map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake partial update recovery failed",
                source: source
            )
        })?;
        let column_count = identity_columns.len() + recovered_columns.len();
        let mut rows = statement.query([]).map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake partial update recovery failed",
                source: source
            )
        })?;
        let mut recovered_bytes = 0usize;
        while let Some(row) = rows.next().map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake partial update recovery row fetch failed",
                source: source
            )
        })? {
            let mut cells = Vec::with_capacity(column_count);
            for (index, column) in
                identity_columns.iter().chain(recovered_columns.iter()).enumerate()
            {
                let value: Value = row.get(index).map_err(|source| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake partial update recovery value fetch failed",
                        source: source
                    )
                })?;
                cells.push(value_to_cell(&column.typ, value)?);
            }
            let recovered_row = TableRow::new(cells.split_off(identity_columns.len()));
            recovered_bytes = recovered_bytes.saturating_add(recovered_row.size_hint());
            let predicate = identity_predicate(identity_columns.iter().copied().zip(cells.iter()));
            recovered.push(&predicate, recovered_row);
        }

        Ok(recovered_bytes)
    }
}

impl StoredRowRecovery<'_> {
    /// Resolves the replicated columns one request reads back.
    fn recovered_column_schemas<'b>(
        &'b self,
        request: &PartialUpdateRecoveryRequest,
        replicated_columns: &[&'b ColumnSchema],
    ) -> EtlResult<Vec<&'b ColumnSchema>> {
        let mut recovered_columns = Vec::with_capacity(request.columns().len());
        for column_index in request.columns() {
            let Some(column) = replicated_columns.get(*column_index) else {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake partial update recovery column is out of range",
                    format!(
                        "Table '{}' has {} replicated columns, requested index {column_index}",
                        self.replicated_table_schema.name(),
                        replicated_columns.len()
                    )
                ));
            };
            recovered_columns.push(*column);
        }

        Ok(recovered_columns)
    }

    /// Reads one range of a request's identities with a single statement.
    ///
    /// Each range is its own statement, so a caller that owns the timeout can
    /// bound every statement separately instead of bounding the whole
    /// recovery once.
    pub(super) fn recover_range(
        &self,
        request: &PartialUpdateRecoveryRequest,
        range: std::ops::Range<usize>,
    ) -> EtlResult<PartialUpdateRecoveryChunk> {
        let identity_columns: Vec<&ColumnSchema> =
            self.replicated_table_schema.identity_column_schemas().collect();
        let replicated_columns: Vec<&ColumnSchema> =
            self.replicated_table_schema.column_schemas().collect();
        let recovered_columns = self.recovered_column_schemas(request, &replicated_columns)?;

        let keys = &request.keys()[range];
        let mut recovered = RecoveredPartialRows::for_request(request);
        let recovered_bytes =
            self.recover_chunk(keys, &identity_columns, &recovered_columns, &mut recovered)?;

        Ok(PartialUpdateRecoveryChunk { recovered, recovered_bytes, keys: keys.len() })
    }
}

/// Stored rows read back by one statement of a recovery.
#[derive(Debug)]
pub(super) struct PartialUpdateRecoveryChunk {
    /// Stored rows for the identities this statement covered.
    pub(super) recovered: RecoveredPartialRows,
    /// Bytes those rows carry.
    pub(super) recovered_bytes: usize,
    /// Identities this statement covered.
    pub(super) keys: usize,
}

#[cfg(test)]
impl PartialUpdateRecovery for StoredRowRecovery<'_> {
    fn recover(
        &self,
        request: &PartialUpdateRecoveryRequest,
    ) -> EtlResult<PartialUpdateRecoveryOutcome> {
        let mut recovered = RecoveredPartialRows::for_request(request);
        if request.is_empty() {
            return Ok(PartialUpdateRecoveryOutcome { recovered, covered_keys: 0 });
        }

        let started = Instant::now();
        // Recovered values are far larger than the partial events that need
        // them, so the read stops once it has filled the batch's byte budget
        // and leaves the remaining identities to the next batch. Chunk sizes
        // follow the bytes and elapsed time per identity the read has already
        // observed, so the budget is overshot by at most one chunk.
        let keys = request.keys();
        let mut pacer = RecoveryChunkPacer::default();
        let mut covered_keys = 0usize;
        let mut recovered_bytes = 0usize;
        while covered_keys < keys.len() {
            let chunk_size = pacer
                .next_chunk_size(request.max_recovered_bytes().saturating_sub(recovered_bytes));
            let chunk_end = keys.len().min(covered_keys.saturating_add(chunk_size));
            let chunk_started = Instant::now();
            let chunk = self.recover_range(request, covered_keys..chunk_end)?;
            pacer.observe(chunk.keys, chunk.recovered_bytes, chunk_started.elapsed());
            recovered_bytes = recovered_bytes.saturating_add(chunk.recovered_bytes);
            recovered.merge(chunk.recovered);
            covered_keys = chunk_end;
            if recovered_bytes >= request.max_recovered_bytes() {
                break;
            }
        }

        info!(
            table = %self.table_name,
            keys = covered_keys,
            requested_keys = keys.len(),
            recovered_columns = request.columns().len(),
            recovered_bytes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "ducklake recovered partial update columns"
        );

        Ok(PartialUpdateRecoveryOutcome { recovered, covered_keys })
    }
}

/// Returns the `SELECT` expression that reads one column back.
///
/// Values whose DuckLake column type has no lossless owned DuckDB value are
/// read as text and parsed with the same rules the source text codec uses.
fn read_expression(column: &ColumnSchema) -> String {
    let quoted_column = quote_identifier(&DUCKLAKE_COLUMN_NAME_MAPPING.map_name(&column.name));
    let reference = format!("cdc_target.{quoted_column}");
    if !reads_as_text(&column.typ) {
        return reference;
    }

    if is_array_type(&column.typ) {
        format!("CAST({reference} AS VARCHAR[])")
    } else {
        format!("CAST({reference} AS VARCHAR)")
    }
}

/// Returns whether a column is read back through its text rendering.
fn reads_as_text(typ: &Type) -> bool {
    matches!(
        *typ,
        Type::NUMERIC
            | Type::NUMERIC_ARRAY
            | Type::UUID
            | Type::UUID_ARRAY
            | Type::JSON
            | Type::JSONB
            | Type::JSON_ARRAY
            | Type::JSONB_ARRAY
    ) || !has_dedicated_ducklake_type(typ)
}

/// Returns whether a Postgres type keeps a dedicated DuckLake column type.
///
/// Every other type is stored as `VARCHAR`, which is also how the source text
/// codec preserved it, so it is read back and parsed as text.
fn has_dedicated_ducklake_type(typ: &Type) -> bool {
    matches!(
        *typ,
        Type::BOOL
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::OID
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::DATE
            | Type::TIME
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
            | Type::UUID
            | Type::JSON
            | Type::JSONB
            | Type::BYTEA
            | Type::BOOL_ARRAY
            | Type::INT2_ARRAY
            | Type::INT4_ARRAY
            | Type::INT8_ARRAY
            | Type::OID_ARRAY
            | Type::FLOAT4_ARRAY
            | Type::FLOAT8_ARRAY
            | Type::DATE_ARRAY
            | Type::TIME_ARRAY
            | Type::TIMESTAMP_ARRAY
            | Type::TIMESTAMPTZ_ARRAY
            | Type::UUID_ARRAY
            | Type::JSON_ARRAY
            | Type::JSONB_ARRAY
            | Type::BYTEA_ARRAY
    )
}

/// Converts one DuckDB value back to the [`Cell`] shape of its column.
fn value_to_cell(typ: &Type, value: Value) -> EtlResult<Cell> {
    if matches!(value, Value::Null) {
        return Ok(Cell::Null);
    }
    if is_array_type(typ) {
        let (Value::List(values) | Value::Array(values)) = value else {
            return Err(unexpected_value_error(typ));
        };
        return array_values_to_cell(typ, values);
    }

    match *typ {
        Type::BOOL => Ok(Cell::Bool(value_to_bool(typ, value)?)),
        Type::INT2 => Ok(Cell::I16(value_to_integer(typ, value)?)),
        Type::INT4 => Ok(Cell::I32(value_to_integer(typ, value)?)),
        Type::INT8 => Ok(Cell::I64(value_to_integer(typ, value)?)),
        Type::OID => Ok(Cell::U32(value_to_integer(typ, value)?)),
        Type::FLOAT4 => Ok(Cell::F32(value_to_f32(typ, value)?)),
        Type::FLOAT8 => Ok(Cell::F64(value_to_f64(typ, value)?)),
        Type::NUMERIC => Ok(Cell::Numeric(value_to_numeric(typ, value)?)),
        Type::DATE => Ok(Cell::Date(value_to_date(typ, value)?)),
        Type::TIME => Ok(Cell::Time(value_to_time(typ, value)?)),
        Type::TIMETZ => Ok(Cell::TimeTz(value_to_time_tz(typ, value)?)),
        Type::TIMESTAMP => Ok(Cell::Timestamp(value_to_timestamp(typ, value)?)),
        Type::TIMESTAMPTZ => Ok(Cell::TimestampTz(value_to_timestamptz(typ, value)?)),
        Type::UUID => Ok(Cell::Uuid(value_to_uuid(typ, value)?)),
        Type::JSON | Type::JSONB => Ok(Cell::Json(value_to_json(typ, value)?)),
        Type::BYTEA => Ok(Cell::Bytes(value_to_bytes(typ, value)?)),
        // Every remaining type is stored as text and keeps its Postgres text
        // rendering, exactly like the source codec's fallback.
        _ => Ok(Cell::String(value_to_text(typ, value)?)),
    }
}

/// Converts a DuckDB list back to the [`ArrayCell`] shape of its column.
fn array_values_to_cell(typ: &Type, values: Vec<Value>) -> EtlResult<Cell> {
    /// Maps each element through one converter, preserving `NULL` elements.
    fn map_elements<T>(
        typ: &Type,
        values: Vec<Value>,
        convert: impl Fn(&Type, Value) -> EtlResult<T>,
    ) -> EtlResult<Vec<Option<T>>> {
        values
            .into_iter()
            .map(|value| match value {
                Value::Null => Ok(None),
                value => convert(typ, value).map(Some),
            })
            .collect()
    }

    let array = match *typ {
        Type::BOOL_ARRAY => ArrayCell::Bool(map_elements(typ, values, value_to_bool)?),
        Type::INT2_ARRAY => ArrayCell::I16(map_elements(typ, values, value_to_integer)?),
        Type::INT4_ARRAY => ArrayCell::I32(map_elements(typ, values, value_to_integer)?),
        Type::INT8_ARRAY => ArrayCell::I64(map_elements(typ, values, value_to_integer)?),
        Type::OID_ARRAY => ArrayCell::U32(map_elements(typ, values, value_to_integer)?),
        Type::FLOAT4_ARRAY => ArrayCell::F32(map_elements(typ, values, value_to_f32)?),
        Type::FLOAT8_ARRAY => ArrayCell::F64(map_elements(typ, values, value_to_f64)?),
        Type::NUMERIC_ARRAY => ArrayCell::Numeric(map_elements(typ, values, value_to_numeric)?),
        Type::DATE_ARRAY => ArrayCell::Date(map_elements(typ, values, value_to_date)?),
        Type::TIME_ARRAY => ArrayCell::Time(map_elements(typ, values, value_to_time)?),
        Type::TIMETZ_ARRAY => ArrayCell::TimeTz(map_elements(typ, values, value_to_time_tz)?),
        Type::TIMESTAMP_ARRAY => {
            ArrayCell::Timestamp(map_elements(typ, values, value_to_timestamp)?)
        }
        Type::TIMESTAMPTZ_ARRAY => {
            ArrayCell::TimestampTz(map_elements(typ, values, value_to_timestamptz)?)
        }
        Type::UUID_ARRAY => ArrayCell::Uuid(map_elements(typ, values, value_to_uuid)?),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => {
            ArrayCell::Json(map_elements(typ, values, value_to_json)?)
        }
        Type::BYTEA_ARRAY => ArrayCell::Bytes(map_elements(typ, values, value_to_bytes)?),
        _ => ArrayCell::String(map_elements(typ, values, value_to_text)?),
    };

    Ok(Cell::Array(array))
}

/// Builds the error returned when a column's value has an unusable shape.
fn unexpected_value_error(typ: &Type) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "DuckLake recovered value does not match its column type",
        format!("Column type {typ} cannot be read back from its stored value")
    )
}

fn value_to_bool(typ: &Type, value: Value) -> EtlResult<bool> {
    match value {
        Value::Boolean(value) => Ok(value),
        _ => Err(unexpected_value_error(typ)),
    }
}

/// Converts any DuckDB integer value to the target width.
fn value_to_integer<T: TryFrom<i128>>(typ: &Type, value: Value) -> EtlResult<T> {
    let wide = match value {
        Value::TinyInt(value) => i128::from(value),
        Value::SmallInt(value) => i128::from(value),
        Value::Int(value) => i128::from(value),
        Value::BigInt(value) => i128::from(value),
        Value::HugeInt(value) => value,
        Value::UTinyInt(value) => i128::from(value),
        Value::USmallInt(value) => i128::from(value),
        Value::UInt(value) => i128::from(value),
        Value::UBigInt(value) => i128::from(value),
        _ => return Err(unexpected_value_error(typ)),
    };

    T::try_from(wide).map_err(|_| unexpected_value_error(typ))
}

fn value_to_f32(typ: &Type, value: Value) -> EtlResult<f32> {
    match value {
        Value::Float(value) => Ok(value),
        Value::Double(value) => Ok(value as f32),
        _ => Err(unexpected_value_error(typ)),
    }
}

fn value_to_f64(typ: &Type, value: Value) -> EtlResult<f64> {
    match value {
        Value::Double(value) => Ok(value),
        Value::Float(value) => Ok(f64::from(value)),
        _ => Err(unexpected_value_error(typ)),
    }
}

fn value_to_numeric(typ: &Type, value: Value) -> EtlResult<PgNumeric> {
    value_to_text(typ, value)?.parse().map_err(|_| unexpected_value_error(typ))
}

fn value_to_uuid(typ: &Type, value: Value) -> EtlResult<uuid::Uuid> {
    value_to_text(typ, value)?.parse().map_err(|_| unexpected_value_error(typ))
}

fn value_to_time_tz(typ: &Type, value: Value) -> EtlResult<PgTimeTz> {
    value_to_text(typ, value)?.parse().map_err(|_| unexpected_value_error(typ))
}

fn value_to_json(typ: &Type, value: Value) -> EtlResult<serde_json::Value> {
    serde_json::from_str(&value_to_text(typ, value)?).map_err(|_| unexpected_value_error(typ))
}

fn value_to_text(typ: &Type, value: Value) -> EtlResult<String> {
    match value {
        Value::Text(value) => Ok(value),
        Value::Enum(value) => Ok(value),
        _ => Err(unexpected_value_error(typ)),
    }
}

fn value_to_bytes(typ: &Type, value: Value) -> EtlResult<Vec<u8>> {
    match value {
        Value::Blob(value) => Ok(value),
        _ => Err(unexpected_value_error(typ)),
    }
}

fn value_to_date(typ: &Type, value: Value) -> EtlResult<NaiveDate> {
    let Value::Date32(days) = value else {
        return Err(unexpected_value_error(typ));
    };

    NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(i64::from(days))))
        .ok_or_else(|| unexpected_value_error(typ))
}

fn value_to_time(typ: &Type, value: Value) -> EtlResult<NaiveTime> {
    let Value::Time64(unit, amount) = value else {
        return Err(unexpected_value_error(typ));
    };
    let nanos = unit_to_nanos(unit, amount).ok_or_else(|| unexpected_value_error(typ))?;
    let seconds = nanos.div_euclid(1_000_000_000);
    let sub_nanos = nanos.rem_euclid(1_000_000_000);

    u32::try_from(seconds)
        .ok()
        .zip(u32::try_from(sub_nanos).ok())
        .and_then(|(seconds, sub_nanos)| {
            NaiveTime::from_num_seconds_from_midnight_opt(seconds, sub_nanos)
        })
        .ok_or_else(|| unexpected_value_error(typ))
}

fn value_to_timestamp(typ: &Type, value: Value) -> EtlResult<NaiveDateTime> {
    Ok(value_to_timestamptz(typ, value)?.naive_utc())
}

fn value_to_timestamptz(typ: &Type, value: Value) -> EtlResult<DateTime<Utc>> {
    let Value::Timestamp(unit, amount) = value else {
        return Err(unexpected_value_error(typ));
    };
    let nanos = unit_to_nanos(unit, amount).ok_or_else(|| unexpected_value_error(typ))?;
    let seconds =
        i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| unexpected_value_error(typ))?;
    let sub_nanos =
        u32::try_from(nanos.rem_euclid(1_000_000_000)).map_err(|_| unexpected_value_error(typ))?;

    Utc.timestamp_opt(seconds, sub_nanos).single().ok_or_else(|| unexpected_value_error(typ))
}

/// Converts a DuckDB time unit amount to nanoseconds.
fn unit_to_nanos(unit: TimeUnit, amount: i64) -> Option<i128> {
    let factor = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };

    i128::from(amount).checked_mul(factor)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::schema::{ReplicatedTableSchema, TableId, TableName, TableSchema};

    use super::*;

    /// Builds a replicated schema whose first column is the replica identity.
    fn replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "rows".to_owned()),
            columns,
        )))
    }

    fn key_schema(value_type: Type, value_sql: &str) -> (ReplicatedTableSchema, String) {
        (
            replicated_schema(vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("value".to_owned(), value_type, -1, 2, true),
            ]),
            value_sql.to_owned(),
        )
    }

    /// Round-trips one stored value through the recovery read.
    fn recovered_value(value_type: Type, value_sql: &str, literal: &str) -> Cell {
        let (schema, value_sql_type) = key_schema(value_type, value_sql);
        let lake_dir = tempfile::tempdir().unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("install ducklake; load ducklake;").unwrap();
        conn.execute_batch(&format!(
            "attach 'ducklake:{catalog}' as lake (data_path '{data}'); create schema lake.public; \
             create table lake.public.rows (id bigint, value {value_sql_type});",
            catalog = lake_dir.path().join("meta.ducklake").display(),
            data = lake_dir.path().join("data").display(),
        ))
        .unwrap();
        conn.execute_batch(&format!("insert into lake.public.rows values (1, {literal});"))
            .unwrap();

        let table_name = DuckLakeTableName::new("public", "rows");
        let recovery = StoredRowRecovery::new(&conn, &table_name, &schema);
        let key = Cell::I64(1);
        let request = PartialUpdateRecoveryRequest::new(
            vec![PartialUpdateRecoveryKey {
                predicate: identity_predicate(
                    schema.identity_column_schemas().zip(std::iter::once(&key)),
                ),
                components: vec![KeyComponent::from_cell(&key, cell_to_sql_literal_ref(&key))],
                mutation_index: 0,
            }],
            BTreeSet::from([1]),
            usize::MAX,
        );
        let outcome = recovery.recover(&request).unwrap();
        let rows = outcome.recovered.rows_for("\"id\" = 1").expect("the identity was covered");

        assert_eq!(rows.len(), 1);
        rows[0].values()[0].clone()
    }

    /// The pacer's whole job: never plan a statement whose observed cost would
    /// exceed the caller's timeout, and come back smaller after one does.
    #[test]
    fn the_pacer_probes_then_sizes_statements_from_observed_cost() {
        let mut pacer = RecoveryChunkPacer::default();
        assert_eq!(
            pacer.next_chunk_size(usize::MAX),
            1,
            "the first statement measures one identity"
        );

        // 1 KiB and 10 ms per identity: the elapsed target allows 3000, the
        // limit caps it, and a small byte budget cuts it further.
        pacer.observe(1, 1024, Duration::from_millis(10));
        assert_eq!(pacer.next_chunk_size(usize::MAX), RECOVERY_KEY_BATCH_SIZE);
        assert_eq!(pacer.next_chunk_size(16 * 1024), 16);

        // 6 s per identity on a table whose files are not compacted: five
        // identities already fill the 30 s target.
        let mut slow = RecoveryChunkPacer::default();
        slow.observe(1, 16, Duration::from_secs(6));
        assert_eq!(slow.next_chunk_size(usize::MAX), 5);

        // A statement is never planned with zero identities, however small the
        // remaining budget is.
        let mut large_rows = RecoveryChunkPacer::default();
        large_rows.observe(1, 8 * 1024 * 1024, Duration::from_millis(1));
        assert_eq!(large_rows.next_chunk_size(1), 1);
    }

    #[test]
    fn the_pacer_halves_its_limit_until_one_identity_remains() {
        let mut pacer = RecoveryChunkPacer::default();
        pacer.observe(64, 64 * 1024, Duration::from_millis(64));
        assert!(pacer.shrink());
        assert_eq!(pacer.limit(), RECOVERY_KEY_BATCH_SIZE / 2);
        assert_eq!(
            pacer.next_chunk_size(usize::MAX),
            1,
            "a shrunk pacer re-probes instead of reusing what the larger statement measured"
        );

        let mut shrinks = 1;
        while pacer.shrink() {
            shrinks += 1;
        }
        assert_eq!(pacer.limit(), 1);
        assert_eq!(shrinks, RECOVERY_KEY_BATCH_SIZE.ilog2() as usize);
        assert!(!pacer.shrink(), "a single-identity statement cannot shrink further");
    }

    #[test]
    fn recovery_reads_every_stored_column_shape_back() {
        assert_eq!(recovered_value(Type::TEXT, "varchar", "'toast'"), Cell::String("toast".into()));
        assert_eq!(
            recovered_value(Type::JSONB, "json", "'{\"a\":1}'"),
            Cell::Json(serde_json::json!({"a": 1}))
        );
        assert_eq!(recovered_value(Type::BOOL, "boolean", "true"), Cell::Bool(true));
        assert_eq!(recovered_value(Type::INT2, "smallint", "7"), Cell::I16(7));
        assert_eq!(recovered_value(Type::INT4, "integer", "-7"), Cell::I32(-7));
        assert_eq!(
            recovered_value(Type::INT8, "bigint", "9223372036854775807"),
            Cell::I64(i64::MAX)
        );
        assert_eq!(recovered_value(Type::OID, "ubigint", "4294967295"), Cell::U32(u32::MAX));
        assert_eq!(recovered_value(Type::FLOAT4, "float", "0.5"), Cell::F32(0.5));
        assert_eq!(recovered_value(Type::FLOAT8, "double", "-0.25"), Cell::F64(-0.25));
        assert_eq!(
            recovered_value(Type::NUMERIC, "decimal(10, 2)", "'12.34'"),
            Cell::Numeric("12.34".parse().unwrap())
        );
        assert_eq!(
            recovered_value(Type::NUMERIC, "varchar", "'12.34'"),
            Cell::Numeric("12.34".parse().unwrap())
        );
        assert_eq!(
            recovered_value(Type::DATE, "date", "date '2026-09-14'"),
            Cell::Date(NaiveDate::from_ymd_opt(2026, 9, 14).unwrap())
        );
        assert_eq!(
            recovered_value(Type::TIME, "time", "time '12:34:56.123456'"),
            Cell::Time(NaiveTime::from_hms_micro_opt(12, 34, 56, 123_456).unwrap())
        );
        assert_eq!(
            recovered_value(Type::TIMESTAMP, "timestamp", "timestamp '2026-09-14 12:34:56'"),
            Cell::Timestamp(
                NaiveDate::from_ymd_opt(2026, 9, 14).unwrap().and_hms_opt(12, 34, 56).unwrap()
            )
        );
        assert_eq!(
            recovered_value(
                Type::TIMESTAMPTZ,
                "timestamptz",
                "timestamptz '2026-09-14 12:34:56+00'"
            ),
            Cell::TimestampTz(Utc.with_ymd_and_hms(2026, 9, 14, 12, 34, 56).single().unwrap())
        );
        assert_eq!(
            recovered_value(Type::UUID, "uuid", "'01900000-0000-7000-8000-000000000001'"),
            Cell::Uuid("01900000-0000-7000-8000-000000000001".parse().unwrap())
        );
        assert_eq!(
            recovered_value(Type::BYTEA, "blob", "'\\xAA\\xBB'::blob"),
            Cell::Bytes(vec![0xAA, 0xBB])
        );
        assert_eq!(
            recovered_value(Type::TIMETZ, "varchar", "'12:34:56+02'"),
            Cell::TimeTz("12:34:56+02".parse().unwrap())
        );
        assert_eq!(recovered_value(Type::TEXT, "varchar", "NULL"), Cell::Null);
        assert_eq!(
            recovered_value(Type::INT4_ARRAY, "integer[]", "[1, NULL, 3]"),
            Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)]))
        );
        assert_eq!(
            recovered_value(Type::TEXT_ARRAY, "varchar[]", "['a', NULL]"),
            Cell::Array(ArrayCell::String(vec![Some("a".to_owned()), None]))
        );
        assert_eq!(
            recovered_value(Type::UUID_ARRAY, "uuid[]", "['01900000-0000-7000-8000-000000000001']"),
            Cell::Array(ArrayCell::Uuid(vec![Some(
                "01900000-0000-7000-8000-000000000001".parse().unwrap()
            )]))
        );
    }

    #[test]
    fn absent_identity_keeps_an_empty_entry() {
        let (schema, _) = key_schema(Type::TEXT, "varchar");
        let key = Cell::I64(1);
        let request = PartialUpdateRecoveryRequest::new(
            vec![PartialUpdateRecoveryKey {
                predicate: identity_predicate(
                    schema.identity_column_schemas().zip(std::iter::once(&key)),
                ),
                components: vec![KeyComponent::from_cell(&key, cell_to_sql_literal_ref(&key))],
                mutation_index: 0,
            }],
            BTreeSet::from([1]),
            usize::MAX,
        );

        let outcome = AbsentStoredRows.recover(&request).unwrap();

        assert_eq!(outcome.covered_keys, 1);
        assert_eq!(outcome.recovered.rows_for("\"id\" = 1"), Some([].as_slice()));
        assert_eq!(outcome.recovered.rows_for("\"id\" = 2"), None);
    }
}
