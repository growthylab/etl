use std::collections::HashSet;

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use url::Url;
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::{Validate, ValidationError};

const fn default_connection_pool_size() -> usize {
    DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE
}

const fn default_ducklake_pool_size() -> u32 {
    DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE
}

/// Default DuckLake target data-file size.
pub const DEFAULT_DUCKLAKE_TARGET_FILE_SIZE: &str = "256MiB";
/// Default DuckLake Parquet row-group byte limit.
pub const DEFAULT_DUCKLAKE_PARQUET_ROW_GROUP_SIZE_BYTES: &str = "128MiB";
/// Default DuckLake Parquet row-group row limit.
pub const DEFAULT_DUCKLAKE_PARQUET_ROW_GROUP_SIZE: &str = "2500000";
/// Default memory budget for one in-memory DuckDB instance.
pub const DEFAULT_DUCKDB_MEMORY_LIMIT: &str = "1GiB";
/// Default execution threads for one in-memory DuckDB instance.
pub const DEFAULT_DUCKDB_THREADS: u32 = 1;
/// Default root directory each in-memory DuckDB instance spills to when one
/// operation does not fit in `duckdb_memory_limit`.
///
/// DuckDB's own default is the relative path `.tmp`, resolved against the
/// process working directory and identical for every instance in the process.
/// That is not usable here: DuckDB names its spill files
/// `duckdb_temp_storage_<block size>-<n>.tmp` and `duckdb_temp_block-<id>.block`
/// with no instance identity in the name, so two instances pointed at one
/// directory delete and overwrite each other's blocks. The path below is a root:
/// each connection gets its own subdirectory under it.
///
/// `/tmp` is mode 1777 in the distroless runtime image, so it is writable by
/// the image's `nonroot` user without the image having to create anything.
pub const DEFAULT_DUCKDB_TEMP_DIRECTORY: &str = "/tmp/etl-duckdb-spill";
/// Default on-disk spill budget for one in-memory DuckDB instance.
///
/// DuckDB's default is 90% of free disk space. On Fargate the ephemeral volume
/// is shared with the image layers and the log-router sidecar, so an unbounded
/// spill trades a killed query for a killed task. `2 * pool_size` instances
/// share the volume, each with this budget.
pub const DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE: &str = "2GiB";

/// DuckLake writer options shared by replication and external maintenance.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DuckLakeWriterConfig {
    target_file_size: Option<String>,
    parquet_row_group_size_bytes: Option<String>,
    parquet_row_group_size: Option<String>,
    duckdb_memory_limit: Option<String>,
    duckdb_threads: Option<u32>,
    duckdb_temp_directory: Option<String>,
    duckdb_max_temp_directory_size: Option<String>,
    destination_schema: Option<String>,
}

impl DuckLakeWriterConfig {
    /// Creates DuckLake writer configuration from optional overrides.
    pub fn new(
        target_file_size: Option<String>,
        parquet_row_group_size_bytes: Option<String>,
        parquet_row_group_size: Option<String>,
    ) -> Self {
        Self {
            target_file_size,
            parquet_row_group_size_bytes,
            parquet_row_group_size,
            duckdb_memory_limit: None,
            duckdb_threads: None,
            duckdb_temp_directory: None,
            duckdb_max_temp_directory_size: None,
            destination_schema: None,
        }
    }

    /// Adds per-instance DuckDB resource limits.
    pub fn with_duckdb_resource_limits(
        mut self,
        duckdb_memory_limit: Option<String>,
        duckdb_threads: Option<u32>,
    ) -> Self {
        self.duckdb_memory_limit = duckdb_memory_limit;
        self.duckdb_threads = duckdb_threads;
        self
    }

    /// Adds the per-instance on-disk spill location and its budget.
    ///
    /// `duckdb_memory_limit` bounds what one instance may hold in memory; this
    /// pair decides what happens to the operation that does not fit. Without a
    /// usable spill target the oversized operation is a hard error, and the
    /// pinned DuckLake extension turns some of those errors into an
    /// unrecoverable database invalidation.
    pub fn with_duckdb_temp_spill(
        mut self,
        duckdb_temp_directory: Option<String>,
        duckdb_max_temp_directory_size: Option<String>,
    ) -> Self {
        self.duckdb_temp_directory =
            duckdb_temp_directory.filter(|directory| !directory.trim().is_empty());
        self.duckdb_max_temp_directory_size =
            duckdb_max_temp_directory_size.filter(|size| !size.trim().is_empty());
        self
    }

    /// Names the destination schema every DuckLake option this writer sets is
    /// confined to.
    ///
    /// The Bayes lake is shared: an option set without a scope is a catalog
    /// row that reconfigures every other writer's Parquet output. Without a
    /// destination schema there is nothing to confine the options to, and the
    /// writer sets none.
    pub fn with_destination_schema(mut self, destination_schema: Option<String>) -> Self {
        self.destination_schema = destination_schema.filter(|schema| !schema.trim().is_empty());
        self
    }

    /// Returns the destination schema DuckLake options are scoped to.
    pub fn destination_schema(&self) -> Option<&str> {
        self.destination_schema.as_deref()
    }

    /// Returns the configured target data-file size or its default.
    pub fn target_file_size(&self) -> &str {
        self.target_file_size.as_deref().unwrap_or(DEFAULT_DUCKLAKE_TARGET_FILE_SIZE)
    }

    /// Returns the configured Parquet row-group byte limit or its default.
    pub fn parquet_row_group_size_bytes(&self) -> &str {
        self.parquet_row_group_size_bytes
            .as_deref()
            .unwrap_or(DEFAULT_DUCKLAKE_PARQUET_ROW_GROUP_SIZE_BYTES)
    }

    /// Returns the configured Parquet row-group row limit or its default.
    pub fn parquet_row_group_size(&self) -> &str {
        self.parquet_row_group_size.as_deref().unwrap_or(DEFAULT_DUCKLAKE_PARQUET_ROW_GROUP_SIZE)
    }

    /// Returns the memory budget for one DuckDB instance.
    pub fn duckdb_memory_limit(&self) -> &str {
        self.duckdb_memory_limit.as_deref().unwrap_or(DEFAULT_DUCKDB_MEMORY_LIMIT)
    }

    /// Returns the execution threads for one DuckDB instance.
    pub fn duckdb_threads(&self) -> u32 {
        self.duckdb_threads.unwrap_or(DEFAULT_DUCKDB_THREADS)
    }

    /// Returns the root directory DuckDB instances spill into.
    pub fn duckdb_temp_directory(&self) -> &str {
        self.duckdb_temp_directory.as_deref().unwrap_or(DEFAULT_DUCKDB_TEMP_DIRECTORY)
    }

    /// Returns the on-disk spill budget for one DuckDB instance.
    pub fn duckdb_max_temp_directory_size(&self) -> &str {
        self.duckdb_max_temp_directory_size
            .as_deref()
            .unwrap_or(DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE)
    }
}

/// Per-table creation options for BigQuery destinations.
///
/// Applied only when a table is created or recreated (first replication,
/// a replication state reset, or a source `TRUNCATE`), never to a table
/// that already exists.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct BigQueryTableOptionsConfig {
    /// Source tables whose BigQuery counterparts use custom physical layouts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<BigQueryTableOptions>,
}

impl BigQueryTableOptionsConfig {
    /// Returns whether no table creation options are configured.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

impl Validate for BigQueryTableOptionsConfig {
    fn validate(&self) -> Result<(), ValidationError> {
        // Keep this limited to ETL-owned invariants. BigQuery validates
        // destination-specific column rules and limits when it executes DDL.
        let mut table_ids = HashSet::with_capacity(self.tables.len());

        for (table_index, table) in self.tables.iter().enumerate() {
            let table_field = format!("table_options.tables[{table_index}]");

            if !table_ids.insert(table.table_id) {
                return Err(ValidationError::InvalidFieldValue {
                    field: format!("{table_field}.table_id"),
                    constraint: format!(
                        "must be unique; table id {} is configured more than once",
                        table.table_id
                    ),
                });
            }

            if table.partition_by.is_none() && table.cluster_by.is_empty() {
                return Err(ValidationError::InvalidFieldValue {
                    field: table_field,
                    constraint: "must configure partition_by, cluster_by, or both".to_owned(),
                });
            }

            if let Some(partition_by) = &table.partition_by {
                validate_bigquery_partition_by(partition_by, &table_field)?;
            }

            for (column_index, column) in table.cluster_by.iter().enumerate() {
                validate_bigquery_column_name(
                    column,
                    &format!("{table_field}.cluster_by[{column_index}]"),
                )?;
            }
        }

        Ok(())
    }
}

/// Validates one BigQuery partitioning configuration.
fn validate_bigquery_partition_by(
    partition_by: &BigQueryPartitionBy,
    table_field: &str,
) -> Result<(), ValidationError> {
    match partition_by {
        BigQueryPartitionBy::TimeColumn { column, .. } => {
            validate_bigquery_column_name(column, &format!("{table_field}.partition_by.column"))
        }
        BigQueryPartitionBy::IntegerRange { column, start, end, interval } => {
            validate_bigquery_column_name(column, &format!("{table_field}.partition_by.column"))?;

            if start >= end {
                return Err(ValidationError::InvalidFieldValue {
                    field: format!("{table_field}.partition_by.end"),
                    constraint: "must be greater than partition_by.start".to_owned(),
                });
            }

            if *interval <= 0 {
                return Err(ValidationError::InvalidFieldValue {
                    field: format!("{table_field}.partition_by.interval"),
                    constraint: "must be greater than 0".to_owned(),
                });
            }

            Ok(())
        }
        BigQueryPartitionBy::IngestionTime { .. } => Ok(()),
    }
}

/// Validates that an ETL table option names a column.
fn validate_bigquery_column_name(column: &str, field: &str) -> Result<(), ValidationError> {
    if column.is_empty() {
        return Err(ValidationError::InvalidFieldValue {
            field: field.to_owned(),
            constraint: "must not be empty".to_owned(),
        });
    }

    Ok(())
}

/// BigQuery creation options for one source table.
///
/// At least one of [`Self::partition_by`] or [`Self::cluster_by`] must be
/// configured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct BigQueryTableOptions {
    /// Source PostgreSQL table OID, stable across renames for the relation's
    /// lifetime.
    pub table_id: u32,
    /// Optional BigQuery partitioning configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_by: Option<BigQueryPartitionBy>,
    /// Ordered BigQuery clustering columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cluster_by: Vec<String>,
}

/// Supported BigQuery table partitioning strategies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BigQueryPartitionBy {
    /// Partition by a replicated `DATE`, `TIMESTAMP`, or `DATETIME` column.
    TimeColumn {
        /// Source column name.
        column: String,
        /// Partition granularity.
        #[serde(default)]
        granularity: BigQueryTimePartitionGranularity,
    },
    /// Partition by ranges of a replicated integer column.
    IntegerRange {
        /// Source column name.
        column: String,
        /// Inclusive start of the first partition range.
        start: i64,
        /// Exclusive end of the last partition range.
        end: i64,
        /// Width of each partition range.
        interval: i64,
    },
    /// Partition by the time at which BigQuery ingests each row.
    IngestionTime {
        /// Partition granularity.
        #[serde(default)]
        granularity: BigQueryTimePartitionGranularity,
    },
}

/// Time granularity for BigQuery time-based partitioning.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum BigQueryTimePartitionGranularity {
    /// Hourly partitions.
    Hour,
    /// Daily partitions.
    #[default]
    Day,
    /// Monthly partitions.
    Month,
    /// Yearly partitions.
    Year,
}

/// Table engine used by the ClickHouse destination when creating replicated
/// tables.
///
/// `ReplacingMergeTree` (default) gives current-state reads via `FINAL` and
/// reclaims deleted rows on `OPTIMIZE ... FINAL CLEANUP`. `MergeTree` is an
/// append-only event-log layout retained for PK-less source tables.
///
/// Applied only when a table is created or recreated. ClickHouse cannot
/// alter a table's engine, so a mismatch against an existing table is a
/// write error, not a silent no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ClickHouseEngine {
    MergeTree,
    #[default]
    ReplacingMergeTree,
}

impl ClickHouseEngine {
    /// The literal engine name ClickHouse uses in `system.tables.engine` and
    /// in `CREATE TABLE ... ENGINE = <name>(...)`. Distinct from the
    /// snake_case form used in YAML / CLI (`merge_tree`,
    /// `replacing_merge_tree`).
    pub const fn as_clickhouse_str(self) -> &'static str {
        match self {
            ClickHouseEngine::MergeTree => "MergeTree",
            ClickHouseEngine::ReplacingMergeTree => "ReplacingMergeTree",
        }
    }

    /// Minimum ClickHouse server `(major, minor)` required to support this
    /// engine, or `None` if any version works.
    ///
    /// `ReplacingMergeTree` requires >= 23.5 because earlier versions reject
    /// the `(version, is_deleted)` argument pair we emit.
    pub const fn min_server_version(self) -> Option<(u32, u32)> {
        match self {
            ClickHouseEngine::MergeTree => None,
            ClickHouseEngine::ReplacingMergeTree => Some((23, 5)),
        }
    }
}

/// Runtime backend used for DuckLake external maintenance coordination.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DuckLakeMaintenanceMode {
    #[default]
    Disabled,
    Kubernetes,
    Postgres,
}

/// Per-table sort-order configuration for DuckLake destinations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct DuckLakeTableSortingConfig {
    /// Source tables whose DuckLake counterparts should have a sort order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<DuckLakeTableSortConfig>,
}

impl DuckLakeTableSortingConfig {
    /// Returns whether no table sort orders are configured.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

impl Validate for DuckLakeTableSortingConfig {
    fn validate(&self) -> Result<(), ValidationError> {
        let mut table_keys = HashSet::with_capacity(self.tables.len());

        for (table_index, table) in self.tables.iter().enumerate() {
            let key = (table.schema.as_str(), table.table.as_str());

            if !table_keys.insert(key) {
                return Err(ValidationError::InvalidFieldValue {
                    field: format!("table_sorting.tables[{table_index}]"),
                    constraint: format!(
                        "must be unique; table {}.{} is configured more than once",
                        table.schema, table.table
                    ),
                });
            }
        }

        Ok(())
    }
}

/// Sort-order configuration for one source table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct DuckLakeTableSortConfig {
    /// Source PostgreSQL schema name.
    pub schema: String,
    /// Source PostgreSQL table name.
    pub table: String,
    /// Columns used by the DuckLake sort order.
    pub sort_by: DuckLakeSortBy,
}

/// Selector used to resolve a DuckLake table's sort columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DuckLakeSortBy {
    /// Use the explicitly configured columns in their listed order.
    Columns {
        /// Ordered sort columns.
        columns: Vec<DuckLakeSortColumn>,
    },
    /// Use the source PostgreSQL primary-key columns in key-definition order.
    PrimaryKey,
}

/// One column in an explicit DuckLake sort order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct DuckLakeSortColumn {
    /// Source column name.
    pub name: String,
    /// Sort direction.
    #[serde(default)]
    pub direction: DuckLakeSortDirection,
    /// Optional null placement. DuckLake's default is used when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nulls: Option<DuckLakeSortNulls>,
}

/// Direction of a DuckLake sort column.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DuckLakeSortDirection {
    /// Sort values in ascending order.
    #[default]
    Asc,
    /// Sort values in descending order.
    Desc,
}

/// Null placement for a DuckLake sort column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DuckLakeSortNulls {
    /// Place null values before non-null values.
    First,
    /// Place null values after non-null values.
    Last,
}

/// Supported product destination kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DestinationKind {
    /// Google BigQuery destination.
    BigQuery,
    /// ClickHouse destination.
    ClickHouse,
    /// DuckLake destination.
    Ducklake,
    /// Iceberg destination.
    Iceberg,
    /// Snowflake destination.
    Snowflake,
}

impl DestinationKind {
    /// Returns the stable destination name used in metrics and tags.
    pub const fn as_str(self) -> &'static str {
        match self {
            DestinationKind::BigQuery => "bigquery",
            DestinationKind::ClickHouse => "clickhouse",
            DestinationKind::Ducklake => "ducklake",
            DestinationKind::Iceberg => "iceberg",
            DestinationKind::Snowflake => "snowflake",
        }
    }
}

/// Configuration for supported ETL data destinations.
///
/// Specifies the destination type and its associated configuration parameters.
/// Each variant corresponds to a different supported destination system.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfig {
    /// Google BigQuery destination configuration.
    ///
    /// Use this variant to configure a BigQuery destination, including
    /// project and dataset identifiers, service account credentials, and
    /// optional staleness settings.
    BigQuery {
        /// Google Cloud project identifier.
        project_id: String,
        /// BigQuery dataset identifier.
        dataset_id: String,
        /// Service account key for authenticating with BigQuery.
        service_account_key: SecretString,
        /// Maximum staleness in minutes for BigQuery CDC reads.
        ///
        /// If not set, the default staleness behavior is used. See
        /// <https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness>.
        /// Applied only when the physical table is created or recreated;
        /// changing it does not affect a table that already exists.
        max_staleness_mins: Option<u16>,
        /// Size of the BigQuery Storage Write API connection pool.
        ///
        /// Controls the number of concurrent connections maintained in the pool
        /// for writing to BigQuery. The maximum number of inflight requests is
        /// calculated as `connection_pool_size * 100`.
        ///
        /// A higher connection pool size allows more parallel writes but
        /// consumes more resources.
        #[serde(default = "default_connection_pool_size")]
        connection_pool_size: usize,
        /// Per-table partitioning and clustering, applied only when the
        /// physical table is created or recreated.
        #[serde(default, skip_serializing_if = "BigQueryTableOptionsConfig::is_empty")]
        table_options: BigQueryTableOptionsConfig,
    },
    #[serde(rename = "clickhouse")]
    ClickHouse {
        /// ClickHouse HTTP(S) endpoint URL.
        url: Url,
        /// ClickHouse user name
        user: String,
        /// ClickHouse password (omit for passwordless access)
        password: Option<SecretString>,
        /// ClickHouse target database
        database: String,
        /// Table engine used for replicated tables. Defaults to
        /// `ReplacingMergeTree`; set to `merge_tree` for the append-only
        /// event-log layout. Applied only when a table is created or
        /// recreated; changing it does not affect a table that already
        /// exists.
        #[serde(default)]
        engine: ClickHouseEngine,
    },
    Iceberg {
        #[serde(flatten)]
        config: IcebergConfig,
    },
    Ducklake {
        /// DuckLake catalog URL.
        catalog_url: SecretString,
        /// DuckLake data path.
        data_path: String,
        /// Size of the DuckDB connection pool.
        #[serde(default = "default_ducklake_pool_size")]
        pool_size: u32,
        /// Optional memory budget for each DuckDB instance.
        duckdb_memory_limit: Option<String>,
        /// Optional execution threads for each DuckDB instance.
        duckdb_threads: Option<u32>,
        /// Optional root directory each DuckDB instance spills into.
        duckdb_temp_directory: Option<String>,
        /// Optional on-disk spill budget for each DuckDB instance.
        duckdb_max_temp_directory_size: Option<String>,
        /// Optional S3-compatible storage access key ID.
        s3_access_key_id: Option<SecretString>,
        /// Optional S3-compatible storage secret access key.
        s3_secret_access_key: Option<SecretString>,
        /// Optional S3-compatible storage region.
        s3_region: Option<String>,
        /// Optional S3-compatible storage endpoint.
        s3_endpoint: Option<String>,
        /// Optional S3 URL style.
        s3_url_style: Option<String>,
        /// Optional S3 SSL toggle.
        s3_use_ssl: Option<bool>,
        /// Optional metadata schema for DuckLake metadata tables.
        metadata_schema: Option<String>,
        /// Optional schema receiving every replicated source table.
        destination_schema: Option<String>,
        /// Optional DuckLake maintenance target file size.
        maintenance_target_file_size: Option<String>,
        /// Optional Parquet row-group byte limit.
        parquet_row_group_size_bytes: Option<String>,
        /// Optional Parquet row-group row limit.
        parquet_row_group_size: Option<String>,
        /// Optional DuckLake snapshot-retention interval.
        expire_snapshots_older_than: Option<String>,
        /// External maintenance coordination backend.
        #[serde(default)]
        maintenance_mode: DuckLakeMaintenanceMode,
        /// Optional per-table sort orders applied during DuckLake maintenance.
        #[serde(default, skip_serializing_if = "DuckLakeTableSortingConfig::is_empty")]
        table_sorting: DuckLakeTableSortingConfig,
    },
    Snowflake {
        /// Snowflake account identifier in "ORGNAME-ACCOUNTNAME" format.
        account_id: String,
        /// Snowflake user with RSA public key configured.
        user: String,
        /// RSA private key in PEM format (PKCS#8 or PKCS#1).
        private_key: SecretString,
        /// Optional passphrase for encrypted private key.
        private_key_passphrase: Option<SecretString>,
        /// Target database name.
        database: String,
        /// Target schema name.
        schema: String,
        /// Snowflake role.
        role: Option<String>,
    },
}

impl DestinationConfig {
    /// Default connection pool size for BigQuery destinations.
    pub const DEFAULT_CONNECTION_POOL_SIZE: usize = 4;
    /// Default connection pool size for DuckLake destinations.
    pub const DEFAULT_DUCKLAKE_POOL_SIZE: u32 = 4;

    /// Returns the destination kind represented by this config.
    pub fn kind(&self) -> DestinationKind {
        match self {
            DestinationConfig::BigQuery { .. } => DestinationKind::BigQuery,
            DestinationConfig::ClickHouse { .. } => DestinationKind::ClickHouse,
            DestinationConfig::Iceberg { .. } => DestinationKind::Iceberg,
            DestinationConfig::Ducklake { .. } => DestinationKind::Ducklake,
            DestinationConfig::Snowflake { .. } => DestinationKind::Snowflake,
        }
    }
}

impl Validate for DestinationConfig {
    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            DestinationConfig::BigQuery { table_options, .. } => table_options.validate(),
            DestinationConfig::Iceberg { config } => config.validate(),
            DestinationConfig::Ducklake {
                table_sorting,
                duckdb_memory_limit,
                duckdb_threads,
                duckdb_temp_directory,
                duckdb_max_temp_directory_size,
                ..
            } => {
                table_sorting.validate()?;
                validate_duckdb_resource_limits(
                    duckdb_memory_limit.as_deref(),
                    *duckdb_threads,
                    duckdb_temp_directory.as_deref(),
                    duckdb_max_temp_directory_size.as_deref(),
                )
            }
            DestinationConfig::ClickHouse { .. } | DestinationConfig::Snowflake { .. } => Ok(()),
        }
    }
}

fn validate_duckdb_resource_limits(
    duckdb_memory_limit: Option<&str>,
    duckdb_threads: Option<u32>,
    duckdb_temp_directory: Option<&str>,
    duckdb_max_temp_directory_size: Option<&str>,
) -> Result<(), ValidationError> {
    if duckdb_memory_limit.is_some_and(|limit| limit.trim().is_empty()) {
        return Err(ValidationError::InvalidFieldValue {
            field: "destination.ducklake.duckdb_memory_limit".to_owned(),
            constraint: "must not be empty".to_owned(),
        });
    }
    if duckdb_threads == Some(0) {
        return Err(ValidationError::InvalidFieldValue {
            field: "destination.ducklake.duckdb_threads".to_owned(),
            constraint: "must be greater than zero".to_owned(),
        });
    }
    // An empty string is the one value DuckDB accepts and reads as "never
    // spill". Configuring the knob that way is always a mistake here, so it is
    // rejected rather than silently restoring the failure mode it exists to
    // remove. Omitting the field entirely still selects the default.
    if duckdb_temp_directory.is_some_and(|directory| directory.trim().is_empty()) {
        return Err(ValidationError::InvalidFieldValue {
            field: "destination.ducklake.duckdb_temp_directory".to_owned(),
            constraint: "must not be empty; omit the field to use the default".to_owned(),
        });
    }
    if duckdb_max_temp_directory_size.is_some_and(|size| size.trim().is_empty()) {
        return Err(ValidationError::InvalidFieldValue {
            field: "destination.ducklake.duckdb_max_temp_directory_size".to_owned(),
            constraint: "must not be empty; omit the field to use the default".to_owned(),
        });
    }
    Ok(())
}

/// Configuration for the iceberg destination with two variants
///
/// 1. Supabase - for analytics buckets on Supabase
/// 2. Rest - for other REST catalogs.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergConfig {
    Supabase {
        /// Supabase project_ref
        project_ref: String,
        /// Name of the warehouse in the catalog
        warehouse_name: String,
        /// If present, the iceberg catalog namespace where tables will be
        /// created. If missing, multiple catlog namespaces will be
        /// created, one per source schema.
        namespace: Option<String>,
        /// Catalog authentication token
        catalog_token: SecretString,
        /// The S3 access key id
        s3_access_key_id: SecretString,
        /// The S3 secret access key
        s3_secret_access_key: SecretString,
        /// The S3 region
        s3_region: String,
    },
    Rest {
        /// Iceberg catalog uri
        catalog_uri: String,
        /// Name of the warehouse in the catalog
        warehouse_name: String,
        /// If present, the iceberg catalog namespace where tables will be
        /// created. If missing, multiple catlog namespaces will be
        /// created, one per source schema.
        namespace: Option<String>,
        /// The S3 access key id
        s3_access_key_id: SecretString,
        /// The S3 secret access key
        s3_secret_access_key: SecretString,
        /// The S3 endpoint
        s3_endpoint: String,
    },
}

impl Validate for IcebergConfig {}

/// Same as [`IcebergConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergConfigWithoutSecrets {
    Supabase {
        /// Supabase project_ref
        project_ref: String,
        /// Name of the warehouse in the catalog
        warehouse_name: String,
        /// If present, the iceberg catalog namespace where tables will be
        /// created. If missing, multiple catlog namespaces will be
        /// created, one per source schema.
        namespace: Option<String>,
        /// The S3 region
        s3_region: String,
    },
    Rest {
        /// Iceberg catalog uri
        catalog_uri: String,
        /// Name of the warehouse in the catalog
        warehouse_name: String,
        /// Iceberg catalog namespace where tables will be created
        namespace: Option<String>,
        /// The S3 endpoint
        s3_endpoint: String,
    },
}

impl Validate for IcebergConfigWithoutSecrets {}

impl From<IcebergConfig> for IcebergConfigWithoutSecrets {
    fn from(value: IcebergConfig) -> Self {
        match value {
            IcebergConfig::Supabase {
                project_ref,
                warehouse_name,
                namespace,
                catalog_token: _,
                s3_access_key_id: _,
                s3_secret_access_key: _,
                s3_region,
            } => IcebergConfigWithoutSecrets::Supabase {
                project_ref,
                warehouse_name,
                namespace,
                s3_region,
            },
            IcebergConfig::Rest {
                catalog_uri,
                warehouse_name,
                namespace,
                s3_access_key_id: _,
                s3_secret_access_key: _,
                s3_endpoint,
            } => IcebergConfigWithoutSecrets::Rest {
                catalog_uri,
                warehouse_name,
                namespace,
                s3_endpoint,
            },
        }
    }
}

/// Same as [`DestinationConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfigWithoutSecrets {
    /// Google BigQuery destination configuration.
    ///
    /// Use this variant to configure a BigQuery destination, including
    /// project and dataset identifiers, service account credentials, and
    /// optional staleness settings.
    BigQuery {
        /// Google Cloud project identifier.
        project_id: String,
        /// BigQuery dataset identifier.
        dataset_id: String,
        /// Maximum staleness in minutes for BigQuery CDC reads.
        ///
        /// If not set, the default staleness behavior is used. See
        /// <https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness>.
        /// Applied only when the physical table is created or recreated;
        /// changing it does not affect a table that already exists.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
        /// Size of the BigQuery Storage Write API connection pool.
        ///
        /// Controls the number of concurrent connections maintained in the pool
        /// for writing to BigQuery. The maximum number of inflight requests is
        /// calculated as `connection_pool_size * 100`.
        ///
        /// A higher connection pool size allows more parallel writes but
        /// consumes more resources.
        #[serde(default = "default_connection_pool_size")]
        connection_pool_size: usize,
        /// Per-table partitioning and clustering, applied only when the
        /// physical table is created or recreated.
        #[serde(default, skip_serializing_if = "BigQueryTableOptionsConfig::is_empty")]
        table_options: BigQueryTableOptionsConfig,
    },
    #[serde(rename = "clickhouse")]
    ClickHouse {
        /// ClickHouse HTTP(S) endpoint URL.
        url: Url,
        /// ClickHouse user name
        user: String,
        /// ClickHouse target database
        database: String,
        /// Table engine used for replicated tables. Defaults to
        /// `ReplacingMergeTree`; set to `merge_tree` for the append-only
        /// event-log layout. Applied only when a table is created or
        /// recreated; changing it does not affect a table that already
        /// exists.
        #[serde(default)]
        engine: ClickHouseEngine,
    },
    Iceberg {
        #[serde(flatten)]
        config: IcebergConfigWithoutSecrets,
    },
    Ducklake {
        /// DuckLake data path.
        data_path: String,
        /// Size of the DuckDB connection pool.
        #[serde(default = "default_ducklake_pool_size")]
        pool_size: u32,
        /// Optional memory budget for each DuckDB instance.
        duckdb_memory_limit: Option<String>,
        /// Optional execution threads for each DuckDB instance.
        duckdb_threads: Option<u32>,
        /// Optional root directory each DuckDB instance spills into.
        duckdb_temp_directory: Option<String>,
        /// Optional on-disk spill budget for each DuckDB instance.
        duckdb_max_temp_directory_size: Option<String>,
        /// Optional S3-compatible storage region.
        s3_region: Option<String>,
        /// Optional S3-compatible storage endpoint.
        s3_endpoint: Option<String>,
        /// Optional S3 URL style.
        s3_url_style: Option<String>,
        /// Optional S3 SSL toggle.
        s3_use_ssl: Option<bool>,
        /// Optional metadata schema for DuckLake metadata tables.
        metadata_schema: Option<String>,
        /// Optional schema receiving every replicated source table.
        destination_schema: Option<String>,
        /// Optional DuckLake maintenance target file size.
        maintenance_target_file_size: Option<String>,
        /// Optional Parquet row-group byte limit.
        parquet_row_group_size_bytes: Option<String>,
        /// Optional Parquet row-group row limit.
        parquet_row_group_size: Option<String>,
        /// Optional DuckLake snapshot-retention interval.
        expire_snapshots_older_than: Option<String>,
        /// External maintenance coordination backend.
        #[serde(default)]
        maintenance_mode: DuckLakeMaintenanceMode,
        /// Optional per-table sort orders applied during DuckLake maintenance.
        #[serde(default, skip_serializing_if = "DuckLakeTableSortingConfig::is_empty")]
        table_sorting: DuckLakeTableSortingConfig,
    },
    Snowflake {
        /// Snowflake account identifier in "ORGNAME-ACCOUNTNAME" format.
        account_id: String,
        /// Snowflake user with RSA public key configured.
        user: String,
        /// Target database name.
        database: String,
        /// Target schema name.
        schema: String,
        /// Snowflake role.
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<String>,
    },
}

impl Validate for DestinationConfigWithoutSecrets {
    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            DestinationConfigWithoutSecrets::BigQuery { table_options, .. } => {
                table_options.validate()
            }
            DestinationConfigWithoutSecrets::Iceberg { config } => config.validate(),
            DestinationConfigWithoutSecrets::Ducklake {
                table_sorting,
                duckdb_memory_limit,
                duckdb_threads,
                duckdb_temp_directory,
                duckdb_max_temp_directory_size,
                ..
            } => {
                table_sorting.validate()?;
                validate_duckdb_resource_limits(
                    duckdb_memory_limit.as_deref(),
                    *duckdb_threads,
                    duckdb_temp_directory.as_deref(),
                    duckdb_max_temp_directory_size.as_deref(),
                )
            }
            DestinationConfigWithoutSecrets::ClickHouse { .. }
            | DestinationConfigWithoutSecrets::Snowflake { .. } => Ok(()),
        }
    }
}

impl From<DestinationConfig> for DestinationConfigWithoutSecrets {
    fn from(value: DestinationConfig) -> Self {
        match value {
            DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
                max_staleness_mins,
                connection_pool_size,
                table_options,
            } => DestinationConfigWithoutSecrets::BigQuery {
                project_id,
                dataset_id,
                max_staleness_mins,
                connection_pool_size,
                table_options,
            },
            DestinationConfig::ClickHouse { url, user, password: _, database, engine } => {
                DestinationConfigWithoutSecrets::ClickHouse { url, user, database, engine }
            }
            DestinationConfig::Iceberg { config } => {
                DestinationConfigWithoutSecrets::Iceberg { config: config.into() }
            }
            DestinationConfig::Ducklake {
                catalog_url: _,
                data_path,
                pool_size,
                duckdb_memory_limit,
                duckdb_threads,
                duckdb_temp_directory,
                duckdb_max_temp_directory_size,
                s3_access_key_id: _,
                s3_secret_access_key: _,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                destination_schema,
                maintenance_target_file_size,
                parquet_row_group_size_bytes,
                parquet_row_group_size,
                expire_snapshots_older_than,
                maintenance_mode,
                table_sorting,
            } => DestinationConfigWithoutSecrets::Ducklake {
                data_path,
                pool_size,
                duckdb_memory_limit,
                duckdb_threads,
                duckdb_temp_directory,
                duckdb_max_temp_directory_size,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                destination_schema,
                maintenance_target_file_size,
                parquet_row_group_size_bytes,
                parquet_row_group_size,
                expire_snapshots_older_than,
                maintenance_mode,
                table_sorting,
            },
            DestinationConfig::Snowflake {
                account_id,
                user,
                private_key: _,
                private_key_passphrase: _,
                database,
                schema,
                role,
            } => DestinationConfigWithoutSecrets::Snowflake {
                account_id,
                user,
                database,
                schema,
                role,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ducklake_writer_config_resolves_defaults_and_overrides() {
        let defaults = DuckLakeWriterConfig::default();
        assert_eq!(defaults.target_file_size(), "256MiB");
        assert_eq!(defaults.parquet_row_group_size_bytes(), "128MiB");
        assert_eq!(defaults.parquet_row_group_size(), "2500000");
        assert_eq!(defaults.duckdb_memory_limit(), "1GiB");
        assert_eq!(defaults.duckdb_threads(), 1);
        assert_eq!(defaults.duckdb_temp_directory(), "/tmp/etl-duckdb-spill");
        assert_eq!(defaults.duckdb_max_temp_directory_size(), "2GiB");

        let configured = DuckLakeWriterConfig::new(
            Some("64MB".to_owned()),
            Some("32MB".to_owned()),
            Some("500000".to_owned()),
        );
        assert_eq!(configured.target_file_size(), "64MB");
        assert_eq!(configured.parquet_row_group_size_bytes(), "32MB");
        assert_eq!(configured.parquet_row_group_size(), "500000");
        let configured = configured.with_duckdb_resource_limits(Some("2GiB".to_owned()), Some(2));
        assert_eq!(configured.duckdb_memory_limit(), "2GiB");
        assert_eq!(configured.duckdb_threads(), 2);
        let configured = configured
            .with_duckdb_temp_spill(Some("/var/spill".to_owned()), Some("8GiB".to_owned()));
        assert_eq!(configured.duckdb_temp_directory(), "/var/spill");
        assert_eq!(configured.duckdb_max_temp_directory_size(), "8GiB");
        // A blank override is not a way to turn spilling off: it falls back to
        // the default the same way an omitted field does.
        let blank =
            configured.clone().with_duckdb_temp_spill(Some("   ".to_owned()), Some(String::new()));
        assert_eq!(blank.duckdb_temp_directory(), "/tmp/etl-duckdb-spill");
        assert_eq!(blank.duckdb_max_temp_directory_size(), "2GiB");
    }

    #[test]
    fn ducklake_without_secrets_omits_catalog_url() {
        let config = DestinationConfig::Ducklake {
            catalog_url: "postgres://user:pass@localhost:5432/ducklake_catalog".to_owned().into(),
            data_path: "s3://bucket/path".to_owned(),
            pool_size: 4,
            duckdb_memory_limit: None,
            duckdb_threads: None,
            duckdb_temp_directory: None,
            duckdb_max_temp_directory_size: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_region: None,
            s3_endpoint: None,
            s3_url_style: None,
            s3_use_ssl: None,
            metadata_schema: None,
            destination_schema: Some("raw_source".to_owned()),
            maintenance_target_file_size: None,
            parquet_row_group_size_bytes: None,
            parquet_row_group_size: None,
            expire_snapshots_older_than: None,
            maintenance_mode: DuckLakeMaintenanceMode::Kubernetes,
            table_sorting: DuckLakeTableSortingConfig {
                tables: vec![DuckLakeTableSortConfig {
                    schema: "public".to_owned(),
                    table: "events".to_owned(),
                    sort_by: DuckLakeSortBy::PrimaryKey,
                }],
            },
        };

        let without_secrets = DestinationConfigWithoutSecrets::from(config);
        let json = serde_json::to_value(without_secrets).unwrap();
        let serialized = json.to_string();

        assert!(!serialized.contains("catalog_url"));
        assert!(!serialized.contains("user:pass"));
        assert_eq!(json["ducklake"]["destination_schema"], "raw_source");
        assert_eq!(
            json["ducklake"]["table_sorting"]["tables"][0]["sort_by"]["kind"],
            "primary_key"
        );
    }

    /// DuckLake options are catalog rows, not session settings, and the Bayes
    /// lake is shared with writers that keep DuckDB's default
    /// `preserve_insertion_order`. The writer only ever sets options it can
    /// confine to its own destination schema.
    #[test]
    fn ducklake_writer_config_scopes_options_to_destination_schema() {
        assert_eq!(DuckLakeWriterConfig::default().destination_schema(), None);
        assert_eq!(
            DuckLakeWriterConfig::default()
                .with_destination_schema(Some("raw_laplace".to_owned()))
                .destination_schema(),
            Some("raw_laplace")
        );
        assert_eq!(
            DuckLakeWriterConfig::default()
                .with_destination_schema(Some("   ".to_owned()))
                .destination_schema(),
            None,
            "a blank schema names nothing and must not become a catalog-wide option"
        );
    }

    /// An empty `temp_directory` is the one value DuckDB reads as "never
    /// spill", so it must not be settable through configuration.
    #[test]
    fn ducklake_resource_limits_reject_a_blank_temp_spill_target() {
        for (temp_directory, max_size) in
            [(Some("   ".to_owned()), None), (None, Some(String::new()))]
        {
            let config = DestinationConfig::Ducklake {
                catalog_url: "postgres://user:pass@localhost:5432/ducklake_catalog"
                    .to_owned()
                    .into(),
                data_path: "s3://bucket/path".to_owned(),
                pool_size: 1,
                duckdb_memory_limit: None,
                duckdb_threads: None,
                duckdb_temp_directory: temp_directory,
                duckdb_max_temp_directory_size: max_size,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_region: None,
                s3_endpoint: None,
                s3_url_style: None,
                s3_use_ssl: None,
                metadata_schema: None,
                destination_schema: None,
                maintenance_target_file_size: None,
                parquet_row_group_size_bytes: None,
                parquet_row_group_size: None,
                expire_snapshots_older_than: None,
                maintenance_mode: DuckLakeMaintenanceMode::Disabled,
                table_sorting: DuckLakeTableSortingConfig::default(),
            };

            assert!(matches!(config.validate(), Err(ValidationError::InvalidFieldValue { .. })));
        }
    }

    #[test]
    fn ducklake_resource_limits_reject_zero_threads() {
        let config = DestinationConfig::Ducklake {
            catalog_url: "postgres://user:pass@localhost:5432/ducklake_catalog".to_owned().into(),
            data_path: "s3://bucket/path".to_owned(),
            pool_size: 1,
            duckdb_memory_limit: Some("1GiB".to_owned()),
            duckdb_threads: Some(0),
            duckdb_temp_directory: None,
            duckdb_max_temp_directory_size: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_region: None,
            s3_endpoint: None,
            s3_url_style: None,
            s3_use_ssl: None,
            metadata_schema: None,
            destination_schema: None,
            maintenance_target_file_size: None,
            parquet_row_group_size_bytes: None,
            parquet_row_group_size: None,
            expire_snapshots_older_than: None,
            maintenance_mode: DuckLakeMaintenanceMode::Disabled,
            table_sorting: DuckLakeTableSortingConfig::default(),
        };

        assert!(matches!(config.validate(), Err(ValidationError::InvalidFieldValue { .. })));
    }

    #[test]
    fn ducklake_table_sorting_deserializes_column_and_primary_key_modes() {
        let config: DuckLakeTableSortingConfig = serde_json::from_value(serde_json::json!({
            "tables": [
                {
                    "schema": "public",
                    "table": "events",
                    "sort_by": {
                        "kind": "columns",
                        "columns": [
                            {
                                "name": "tenant_id"
                            },
                            {
                                "name": "created_at",
                                "direction": "desc",
                                "nulls": "first"
                            }
                        ]
                    }
                },
                {
                    "schema": "public",
                    "table": "accounts",
                    "sort_by": {
                        "kind": "primary_key"
                    }
                }
            ]
        }))
        .unwrap();

        assert_eq!(config.tables.len(), 2);
        let DuckLakeSortBy::Columns { columns } = &config.tables[0].sort_by else {
            panic!("Expected explicit columns");
        };
        assert_eq!(columns[0].direction, DuckLakeSortDirection::Asc);
        assert_eq!(columns[0].nulls, None);
        assert_eq!(columns[1].direction, DuckLakeSortDirection::Desc);
        assert_eq!(columns[1].nulls, Some(DuckLakeSortNulls::First));
        assert_eq!(config.tables[1].sort_by, DuckLakeSortBy::PrimaryKey);
    }

    #[test]
    fn ducklake_table_sorting_rejects_duplicate_tables() {
        let config = DuckLakeTableSortingConfig {
            tables: vec![
                DuckLakeTableSortConfig {
                    schema: "public".to_owned(),
                    table: "events".to_owned(),
                    sort_by: DuckLakeSortBy::PrimaryKey,
                },
                DuckLakeTableSortConfig {
                    schema: "public".to_owned(),
                    table: "events".to_owned(),
                    sort_by: DuckLakeSortBy::Columns { columns: vec![] },
                },
            ],
        };

        config.validate().unwrap_err();
    }

    #[test]
    fn bigquery_table_options_deserialize_typed_partitioning_and_clustering() {
        let config: BigQueryTableOptionsConfig = serde_json::from_value(serde_json::json!({
            "tables": [
                {
                    "table_id": 16384,
                    "partition_by": {
                        "kind": "time_column",
                        "column": "created_at",
                        "granularity": "month"
                    },
                    "cluster_by": ["tenant_id", "event_type"]
                },
                {
                    "table_id": 16385,
                    "partition_by": {
                        "kind": "integer_range",
                        "column": "id",
                        "start": 0,
                        "end": 1000,
                        "interval": 100
                    }
                },
                {
                    "table_id": 16386,
                    "cluster_by": ["tenant_id"]
                }
            ]
        }))
        .unwrap();

        assert_eq!(config.tables.len(), 3);
        assert_eq!(config.tables[0].table_id, 16384);
        assert_eq!(
            config.tables[0].partition_by,
            Some(BigQueryPartitionBy::TimeColumn {
                column: "created_at".to_owned(),
                granularity: BigQueryTimePartitionGranularity::Month,
            })
        );
        assert_eq!(config.tables[0].cluster_by, ["tenant_id", "event_type"]);
        assert_eq!(
            config.tables[1].partition_by,
            Some(BigQueryPartitionBy::IntegerRange {
                column: "id".to_owned(),
                start: 0,
                end: 1000,
                interval: 100,
            })
        );
        assert_eq!(config.tables[1].table_id, 16385);
        assert!(config.tables[1].cluster_by.is_empty());
        assert_eq!(config.tables[2].table_id, 16386);
        assert_eq!(config.tables[2].partition_by, None);
        assert_eq!(config.tables[2].cluster_by, ["tenant_id"]);

        let serialized = serde_json::to_value(&config).unwrap();
        assert!(serialized["tables"][1].get("cluster_by").is_none());
        assert!(serialized["tables"][2].get("partition_by").is_none());
    }

    #[test]
    fn empty_bigquery_table_options_use_the_default_layout() {
        let config: BigQueryTableOptionsConfig =
            serde_json::from_value(serde_json::json!({})).unwrap();

        assert!(config.is_empty());
        assert_eq!(serde_json::to_value(config).unwrap(), serde_json::json!({}));
    }

    #[test]
    fn bigquery_table_options_validate_etl_owned_invariants() {
        let table = |table_id, partition_by, cluster_by: &[&str]| BigQueryTableOptions {
            table_id,
            partition_by,
            cluster_by: cluster_by.iter().map(|column| (*column).to_owned()).collect(),
        };
        let config = |tables| BigQueryTableOptionsConfig { tables };

        let valid = config(vec![
            table(
                1,
                Some(BigQueryPartitionBy::IntegerRange {
                    column: "id".to_owned(),
                    start: 0,
                    end: 100,
                    interval: 10,
                }),
                &[],
            ),
            table(2, None, &["tenant_id"]),
        ]);
        valid.validate().unwrap();

        let invalid = [
            (config(vec![table(1, None, &[])]), "table_options.tables[0]"),
            (
                config(vec![table(1, None, &["id"]), table(1, None, &["tenant_id"])]),
                "table_options.tables[1].table_id",
            ),
            (
                config(vec![table(
                    1,
                    Some(BigQueryPartitionBy::TimeColumn {
                        column: String::new(),
                        granularity: BigQueryTimePartitionGranularity::Day,
                    }),
                    &[],
                )]),
                "table_options.tables[0].partition_by.column",
            ),
            (config(vec![table(1, None, &[""])]), "table_options.tables[0].cluster_by[0]"),
            (
                config(vec![table(
                    1,
                    Some(BigQueryPartitionBy::IntegerRange {
                        column: "id".to_owned(),
                        start: 100,
                        end: 100,
                        interval: 10,
                    }),
                    &[],
                )]),
                "table_options.tables[0].partition_by.end",
            ),
            (
                config(vec![table(
                    1,
                    Some(BigQueryPartitionBy::IntegerRange {
                        column: "id".to_owned(),
                        start: 0,
                        end: 100,
                        interval: 0,
                    }),
                    &[],
                )]),
                "table_options.tables[0].partition_by.interval",
            ),
        ];

        for (config, expected_field) in invalid {
            let ValidationError::InvalidFieldValue { field, .. } = config.validate().unwrap_err();
            assert_eq!(field, expected_field);
        }
    }

    #[test]
    fn destination_kind_names_match_metrics_labels() {
        assert_eq!(DestinationKind::BigQuery.as_str(), "bigquery");
        assert_eq!(DestinationKind::ClickHouse.as_str(), "clickhouse");
        assert_eq!(DestinationKind::Ducklake.as_str(), "ducklake");
        assert_eq!(DestinationKind::Iceberg.as_str(), "iceberg");
        assert_eq!(DestinationKind::Snowflake.as_str(), "snowflake");
    }
}
