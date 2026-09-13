//! Host configuration for an embedded DuckLake destination.

use std::sync::Arc;

use etl::{error::EtlResult, schema::TableName};

use crate::ducklake::DuckLakeTableName;

/// Creates a fully initialized database for a new connection-pool generation.
pub(super) type ConnectionInitializer =
    Arc<dyn Fn() -> EtlResult<duckdb::Connection> + Send + Sync>;

/// Maps a source table before its destination identity is persisted.
pub(super) type TableNameMapper =
    Arc<dyn Fn(&TableName) -> EtlResult<DuckLakeTableName> + Send + Sync>;

/// Optional host policies; absent callbacks retain the standalone defaults.
#[derive(Clone, Default)]
pub(super) struct EmbeddingOptions {
    pub(super) streaming_batch: crate::ducklake::DuckLakeStreamingBatchConfig,
    pub(super) connection_initializer: Option<ConnectionInitializer>,
    pub(super) table_name_mapper: Option<TableNameMapper>,
    /// Host-owned PostgreSQL pool for the catalog metadata queries.
    ///
    /// When absent the destination derives a pool from the catalog URL. A host
    /// whose catalog credential rotates (for example an IAM auth token that
    /// expires after fifteen minutes) supplies a pool whose connect options it
    /// refreshes itself, because a URL can only ever carry one credential.
    pub(super) metadata_pg_pool: Option<sqlx::PgPool>,
}
