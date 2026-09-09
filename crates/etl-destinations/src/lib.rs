//! ETL destination implementations.
//!
//! Provides implementations of the ETL destination trait for various data
//! warehouses and analytics platforms, enabling data replication from Postgres
//! to cloud services.

#[cfg(any(
    feature = "support",
    feature = "bigquery",
    feature = "clickhouse",
    feature = "ducklake",
    feature = "iceberg",
    feature = "snowflake"
))]
pub mod recovery;
#[cfg(any(feature = "support", feature = "bigquery", feature = "ducklake", feature = "snowflake"))]
pub mod retry;
#[cfg(any(feature = "support", feature = "ducklake", feature = "snowflake"))]
pub mod sql;
#[cfg(any(
    feature = "bigquery",
    feature = "clickhouse",
    feature = "ducklake",
    feature = "iceberg",
    feature = "snowflake"
))]
mod table_name;

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "ducklake")]
pub mod ducklake;
#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "snowflake")]
pub mod snowflake;
