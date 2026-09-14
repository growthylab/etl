use etl::{data::Cell, schema::ReplicatedTableSchema};

use crate::ducklake::{DUCKLAKE_COLUMN_NAME_MAPPING, sql::quote_identifier};

/// Order-preserving numeric image of one canonical identity key value.
///
/// DuckLake prunes data files by comparing a range predicate against the
/// per-file column statistics, so the bound literals emitted for a batch must
/// bracket every key under DuckDB's own ordering. Integers order numerically.
/// DuckDB stores `UUID` as a 128-bit integer whose most significant bit is
/// flipped on both read and write, so ordering the raw big-endian bytes as an
/// unsigned 128-bit value reproduces DuckDB's signed comparison exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyOrdinal {
    /// A canonical `INT2`, `INT4`, or `INT8` identity value.
    Integer(i64),
    /// A canonical `UUID` identity value as its unsigned 128-bit image.
    Uuid(u128),
}

impl KeyOrdinal {
    /// Returns the order-preserving image of a canonical identity cell.
    ///
    /// Returns [`None`] for `NULL` and for any value outside the canonical
    /// integer and UUID identity domain, which disables range pruning for that
    /// column instead of emitting a bound that could exclude a matching row.
    fn from_cell(cell: &Cell) -> Option<Self> {
        match cell {
            Cell::I16(value) => Some(Self::Integer(i64::from(*value))),
            Cell::I32(value) => Some(Self::Integer(i64::from(*value))),
            Cell::U32(value) => Some(Self::Integer(i64::from(*value))),
            Cell::I64(value) => Some(Self::Integer(*value)),
            Cell::Uuid(value) => Some(Self::Uuid(u128::from_be_bytes(*value.as_bytes()))),
            _ => None,
        }
    }

    /// Compares two ordinals of the same kind.
    ///
    /// Returns [`None`] for mismatched kinds, which cannot happen for a single
    /// typed identity column and disables range pruning if it ever does.
    fn compare(self, other: Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => Some(left.cmp(&right)),
            (Self::Uuid(left), Self::Uuid(right)) => Some(left.cmp(&right)),
            _ => None,
        }
    }
}

/// One identity column value of one key, as both SQL literal and sort image.
///
/// The literal is the exact text already emitted into the `VALUES` key list, so
/// a bound reuses it verbatim rather than re-rendering a parsed value.
#[derive(Clone, Debug)]
pub(super) struct KeyComponent {
    /// SQL literal for this component, as emitted into the key set.
    literal: String,
    /// Order-preserving image used for the batch minimum and maximum.
    ordinal: Option<KeyOrdinal>,
}

impl KeyComponent {
    /// Builds a key component from a canonical identity cell.
    pub(super) fn from_cell(cell: &Cell, literal: String) -> Self {
        Self { literal, ordinal: KeyOrdinal::from_cell(cell) }
    }

    /// Returns the SQL literal for this component.
    pub(super) fn literal(&self) -> &str {
        &self.literal
    }
}

/// Per-column minimum and maximum tracked across one key set.
#[derive(Clone, Debug)]
enum ColumnBound {
    /// No key has contributed a value yet.
    Unset,
    /// Every key so far carried a comparable value in this column.
    Range { min: (KeyOrdinal, String), max: (KeyOrdinal, String) },
    /// A `NULL` or incomparable value forbids a range predicate here.
    Disabled,
}

impl ColumnBound {
    /// Folds one key component into this column's running bound.
    fn observe(&mut self, component: &KeyComponent) {
        if matches!(self, Self::Disabled) {
            return;
        }
        let Some(ordinal) = component.ordinal else {
            *self = Self::Disabled;
            return;
        };
        match self {
            Self::Unset => {
                let bound = (ordinal, component.literal.clone());
                *self = Self::Range { min: bound.clone(), max: bound };
            }
            Self::Range { min, max } => {
                let (Some(against_min), Some(against_max)) =
                    (ordinal.compare(min.0), ordinal.compare(max.0))
                else {
                    *self = Self::Disabled;
                    return;
                };
                if against_min.is_lt() {
                    *min = (ordinal, component.literal.clone());
                }
                if against_max.is_gt() {
                    *max = (ordinal, component.literal.clone());
                }
            }
            Self::Disabled => {}
        }
    }
}

/// Accumulates the key set of one batched full-row `DELETE`.
///
/// The key set joins the target table against an inline `VALUES` list so one
/// statement removes every identity in the batch. A join against a `VALUES`
/// list gives DuckLake no per-file predicate, so the whole table is scanned;
/// on a table with tens of thousands of small data files that scan does not
/// finish inside the foreground query timeout. Appending the batch minimum and
/// maximum per identity column restores the file pruning the previous per-key
/// `WHERE col = ...` form had, without changing which rows match.
#[derive(Clone, Debug)]
pub(super) struct DeleteKeySet {
    /// Quoted DuckLake identity column names, in identity order.
    identity_columns: Vec<String>,
    /// Running minimum and maximum per identity column.
    bounds: Vec<ColumnBound>,
    /// Rendered `VALUES` tuples, one per distinct identity.
    keys: Vec<String>,
}

impl DeleteKeySet {
    /// Creates an empty key set for a table's replica identity.
    pub(super) fn new(replicated_table_schema: &ReplicatedTableSchema) -> Self {
        let identity_columns: Vec<String> = replicated_table_schema
            .identity_column_schemas()
            .map(|column| quote_identifier(&DUCKLAKE_COLUMN_NAME_MAPPING.map_name(&column.name)))
            .collect();
        let bounds = vec![ColumnBound::Unset; identity_columns.len()];

        Self { identity_columns, bounds, keys: Vec::new() }
    }

    /// Adds one identity to the key set and folds it into the bounds.
    ///
    /// A component count that does not match the replica identity is ignored
    /// for bound tracking; the caller derives components from the same schema,
    /// so a mismatch only ever disables pruning.
    pub(super) fn push(&mut self, components: &[KeyComponent]) {
        self.keys.push(format!(
            "({})",
            components.iter().map(KeyComponent::literal).collect::<Vec<_>>().join(",")
        ));
        if components.len() != self.bounds.len() {
            self.bounds.iter_mut().for_each(|bound| *bound = ColumnBound::Disabled);
            return;
        }
        for (bound, component) in self.bounds.iter_mut().zip(components) {
            bound.observe(component);
        }
    }

    /// Takes the accumulated key set as a `USING ... WHERE ...` clause.
    ///
    /// Returns [`None`] when no key was pushed. The key set is reset so the
    /// same accumulator serves the next run of full-row writes.
    pub(super) fn take_clause(&mut self) -> Option<String> {
        if self.keys.is_empty() {
            return None;
        }

        let keys = std::mem::take(&mut self.keys);
        let reset = vec![ColumnBound::Unset; self.bounds.len()];
        let bounds = std::mem::replace(&mut self.bounds, reset);

        let mut predicates: Vec<String> = self
            .identity_columns
            .iter()
            .map(|column| format!("cdc_target.{column} IS NOT DISTINCT FROM cdc_keys.{column}"))
            .collect();
        for (column, bound) in self.identity_columns.iter().zip(&bounds) {
            if let ColumnBound::Range { min, max } = bound {
                predicates.push(format!("cdc_target.{column} >= {}", min.1));
                predicates.push(format!("cdc_target.{column} <= {}", max.1));
            }
        }

        Some(format!(
            "USING (VALUES {}) AS cdc_keys({}) WHERE {}",
            keys.join(","),
            self.identity_columns.join(","),
            predicates.join(" AND ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type};
    use uuid::Uuid;

    use super::*;
    use crate::ducklake::encoding::cell_to_sql_literal_ref;

    /// Number of separate DuckLake commits, and therefore data files, used by
    /// the pruning proof.
    const PRUNING_PROOF_FILES: usize = 24;
    /// Rows written per DuckLake commit in the pruning proof.
    const PRUNING_PROOF_ROWS_PER_FILE: usize = 50;

    fn replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "traces".to_owned()),
            columns,
        )))
    }

    fn uuid_key_schema() -> ReplicatedTableSchema {
        replicated_schema(vec![
            ColumnSchema::new("trace_id".to_owned(), Type::UUID, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 2, true),
        ])
    }

    fn int8_key_schema() -> ReplicatedTableSchema {
        replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 2, true),
        ])
    }

    fn composite_key_schema() -> ReplicatedTableSchema {
        replicated_schema(vec![
            ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("trace_id".to_owned(), Type::UUID, -1, 2, false).with_primary_key(2),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 3, true),
        ])
    }

    /// Builds key components the way `delete_key_components` does in batches.
    fn components(cells: &[Cell]) -> Vec<KeyComponent> {
        cells
            .iter()
            .map(|cell| KeyComponent::from_cell(cell, cell_to_sql_literal_ref(cell)))
            .collect()
    }

    fn uuid_cell(value: &str) -> Cell {
        Cell::Uuid(Uuid::parse_str(value).unwrap())
    }

    #[test]
    fn empty_key_set_has_no_clause() {
        let mut key_set = DeleteKeySet::new(&uuid_key_schema());

        assert_eq!(key_set.take_clause(), None);
    }

    #[test]
    fn uuid_key_set_brackets_keys_with_a_range_predicate() {
        let mut key_set = DeleteKeySet::new(&uuid_key_schema());
        for value in [
            "01900000-0000-7000-8000-000000000002",
            "01800000-0000-7000-8000-000000000001",
            "01a00000-0000-7000-8000-000000000003",
        ] {
            key_set.push(&components(&[uuid_cell(value)]));
        }

        assert_eq!(
            key_set.take_clause().unwrap(),
            "USING (VALUES (CAST('01900000-0000-7000-8000-000000000002' AS \
             UUID)),(CAST('01800000-0000-7000-8000-000000000001' AS \
             UUID)),(CAST('01a00000-0000-7000-8000-000000000003' AS UUID))) AS \
             cdc_keys(\"trace_id\") WHERE cdc_target.\"trace_id\" IS NOT DISTINCT FROM \
             cdc_keys.\"trace_id\" AND cdc_target.\"trace_id\" >= \
             CAST('01800000-0000-7000-8000-000000000001' AS UUID) AND cdc_target.\"trace_id\" <= \
             CAST('01a00000-0000-7000-8000-000000000003' AS UUID)"
        );
    }

    #[test]
    fn int8_key_set_brackets_keys_numerically() {
        let mut key_set = DeleteKeySet::new(&int8_key_schema());
        for value in [7_i64, -9, 100, 42] {
            key_set.push(&components(&[Cell::I64(value)]));
        }

        assert_eq!(
            key_set.take_clause().unwrap(),
            "USING (VALUES (7),(-9),(100),(42)) AS cdc_keys(\"id\") WHERE cdc_target.\"id\" IS \
             NOT DISTINCT FROM cdc_keys.\"id\" AND cdc_target.\"id\" >= -9 AND cdc_target.\"id\" \
             <= 100"
        );
    }

    #[test]
    fn composite_key_set_brackets_each_identity_column_independently() {
        let mut key_set = DeleteKeySet::new(&composite_key_schema());
        key_set
            .push(&components(&[Cell::I32(5), uuid_cell("01900000-0000-7000-8000-000000000002")]));
        key_set
            .push(&components(&[Cell::I32(2), uuid_cell("01800000-0000-7000-8000-000000000001")]));

        assert_eq!(
            key_set.take_clause().unwrap(),
            "USING (VALUES (5,CAST('01900000-0000-7000-8000-000000000002' AS \
             UUID)),(2,CAST('01800000-0000-7000-8000-000000000001' AS UUID))) AS \
             cdc_keys(\"tenant_id\",\"trace_id\") WHERE cdc_target.\"tenant_id\" IS NOT DISTINCT \
             FROM cdc_keys.\"tenant_id\" AND cdc_target.\"trace_id\" IS NOT DISTINCT FROM \
             cdc_keys.\"trace_id\" AND cdc_target.\"tenant_id\" >= 2 AND cdc_target.\"tenant_id\" \
             <= 5 AND cdc_target.\"trace_id\" >= CAST('01800000-0000-7000-8000-000000000001' AS \
             UUID) AND cdc_target.\"trace_id\" <= CAST('01900000-0000-7000-8000-000000000002' AS \
             UUID)"
        );
    }

    #[test]
    fn null_key_component_drops_the_range_for_that_column_only() {
        let mut key_set = DeleteKeySet::new(&composite_key_schema());
        key_set
            .push(&components(&[Cell::I32(5), uuid_cell("01900000-0000-7000-8000-000000000002")]));
        key_set.push(&components(&[Cell::I32(2), Cell::Null]));

        let clause = key_set.take_clause().unwrap();

        assert!(clause.contains("cdc_target.\"tenant_id\" >= 2"));
        assert!(clause.contains("cdc_target.\"tenant_id\" <= 5"));
        assert!(!clause.contains("cdc_target.\"trace_id\" >="));
        assert!(!clause.contains("cdc_target.\"trace_id\" <="));
        assert!(
            clause.contains("cdc_target.\"trace_id\" IS NOT DISTINCT FROM cdc_keys.\"trace_id\"")
        );
    }

    #[test]
    fn single_key_set_emits_an_equal_minimum_and_maximum() {
        let mut key_set = DeleteKeySet::new(&uuid_key_schema());
        key_set.push(&components(&[uuid_cell("01900000-0000-7000-8000-000000000002")]));

        let clause = key_set.take_clause().unwrap();

        assert!(clause.contains(
            "cdc_target.\"trace_id\" >= CAST('01900000-0000-7000-8000-000000000002' AS UUID)"
        ));
        assert!(clause.contains(
            "cdc_target.\"trace_id\" <= CAST('01900000-0000-7000-8000-000000000002' AS UUID)"
        ));
    }

    #[test]
    fn take_clause_resets_the_accumulated_bounds() {
        let mut key_set = DeleteKeySet::new(&int8_key_schema());
        key_set.push(&components(&[Cell::I64(1000)]));
        key_set.take_clause().unwrap();

        key_set.push(&components(&[Cell::I64(5)]));

        let clause = key_set.take_clause().unwrap();
        assert!(clause.contains("cdc_target.\"id\" >= 5"));
        assert!(clause.contains("cdc_target.\"id\" <= 5"));
        assert!(!clause.contains("1000"));
    }

    /// Proves the Rust ordering used for UUID bounds is DuckDB's own ordering.
    ///
    /// DuckDB compares `UUID` as a signed 128-bit integer with the most
    /// significant bit flipped, which is equivalent to comparing the raw
    /// big-endian bytes as an unsigned 128-bit value. The sample deliberately
    /// spans that flip so a naive signed comparison would disagree.
    #[test]
    fn uuid_key_ordinal_matches_duckdb_ordering() {
        let samples = [
            "00000000-0000-0000-0000-000000000000",
            "01800000-0000-7000-8000-000000000001",
            "01900000-0000-7000-8000-000000000002",
            "7fffffff-ffff-ffff-ffff-ffffffffffff",
            "80000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ];

        let conn = duckdb::Connection::open_in_memory().unwrap();
        let values = samples
            .iter()
            .map(|value| format!("(CAST('{value}' AS UUID))"))
            .collect::<Vec<_>>()
            .join(",");
        let mut statement = conn
            .prepare(&format!(
                "select CAST(id AS VARCHAR) from (values {values}) t(id) order by id"
            ))
            .unwrap();
        let duckdb_order: Vec<String> = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();

        let mut rust_order = samples.to_vec();
        rust_order.sort_by_key(|value| match KeyOrdinal::from_cell(&uuid_cell(value)).unwrap() {
            KeyOrdinal::Uuid(image) => image,
            KeyOrdinal::Integer(_) => unreachable!("uuid cell yields a uuid ordinal"),
        });

        assert_eq!(duckdb_order, rust_order);
    }

    /// Proves the range predicates let DuckLake skip data files.
    ///
    /// The table is written in [`PRUNING_PROOF_FILES`] separate commits with
    /// disjoint, time-ordered UUIDv7 key ranges, the shape of a heavily
    /// updated CDC table with a UUIDv7 key. `EXPLAIN ANALYZE`
    /// reports `Total Files Read` on the DuckLake table scan; that count is the
    /// deterministic pruning signal asserted here. Without the range predicates
    /// the join against the `VALUES` key list reads every file, which is the
    /// incident.
    #[test]
    fn range_predicates_let_ducklake_prune_data_files() {
        let lake_dir = tempfile::tempdir().unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("install ducklake; load ducklake;").unwrap();
        conn.execute_batch(&format!(
            "attach 'ducklake:{catalog}' as lake (data_path '{data}', data_inlining_row_limit 0); \
             create table lake.traces (trace_id uuid, payload varchar);",
            catalog = lake_dir.path().join("meta.ducklake").display(),
            data = lake_dir.path().join("data").display(),
        ))
        .unwrap();

        // Each insert is its own transaction, so each produces one data file
        // whose UUID statistics cover only that file's key range.
        let mut file_keys: Vec<Vec<String>> = Vec::new();
        for file in 0..PRUNING_PROOF_FILES {
            let keys: Vec<String> = (0..PRUNING_PROOF_ROWS_PER_FILE)
                .map(|row| format!("{file:02x}{row:02x}0000-0000-7000-8000-000000000000"))
                .collect();
            let values = keys
                .iter()
                .enumerate()
                .map(|(row, key)| format!("(CAST('{key}' AS UUID), 'payload-{file}-{row}')"))
                .collect::<Vec<_>>()
                .join(",");
            conn.execute_batch(&format!("insert into lake.traces values {values};")).unwrap();
            file_keys.push(keys);
        }

        let total_files: i64 = conn
            .query_row("select count(*) from ducklake_list_files('lake', 'traces')", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(total_files, PRUNING_PROOF_FILES as i64);

        // Keys taken from one file only, the shape a CDC batch produces on a
        // time-ordered UUIDv7 table.
        let schema = uuid_key_schema();
        let mut key_set = DeleteKeySet::new(&schema);
        for key in file_keys[PRUNING_PROOF_FILES / 2].iter().take(3) {
            key_set.push(&components(&[uuid_cell(key)]));
        }
        let pruning_clause = key_set.take_clause().unwrap();
        let unpruned_clause = pruning_clause
            .split(" AND cdc_target.\"trace_id\" >= ")
            .next()
            .expect("the join predicate precedes the range predicates")
            .to_owned();
        assert!(pruning_clause.len() > unpruned_clause.len());

        assert_eq!(files_read_by_delete(&conn, &unpruned_clause), PRUNING_PROOF_FILES as i64);
        assert_eq!(files_read_by_delete(&conn, &pruning_clause), 1);

        // The pruned DELETE still removes exactly the three targeted rows.
        conn.execute_batch(&format!("delete from lake.traces as cdc_target {pruning_clause};"))
            .unwrap();
        let remaining: i64 =
            conn.query_row("select count(*) from lake.traces", [], |row| row.get(0)).unwrap();
        assert_eq!(remaining, (PRUNING_PROOF_FILES * PRUNING_PROOF_ROWS_PER_FILE) as i64 - 3);
    }

    /// Returns the DuckLake data files read by one key-set DELETE.
    ///
    /// The statement runs inside a transaction that is rolled back, so the
    /// caller can measure several variants against the same table.
    fn files_read_by_delete(conn: &duckdb::Connection, key_set_clause: &str) -> i64 {
        conn.execute_batch("begin;").unwrap();
        let mut statement = conn
            .prepare(&format!(
                "explain analyze delete from lake.traces as cdc_target {key_set_clause}"
            ))
            .unwrap();
        let plan: String = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .join("\n");
        conn.execute_batch("rollback;").unwrap();

        parse_total_files_read(&plan)
            .unwrap_or_else(|| panic!("EXPLAIN ANALYZE reported no file count:\n{plan}"))
    }

    /// Extracts `Total Files Read` from an `EXPLAIN ANALYZE` rendering.
    fn parse_total_files_read(plan: &str) -> Option<i64> {
        plan.lines().find_map(|line| {
            let rest = line.split("Total Files Read:").nth(1)?;
            rest.trim_start()
                .split(|character: char| !character.is_ascii_digit())
                .next()
                .filter(|digits| !digits.is_empty())?
                .parse()
                .ok()
        })
    }
}
