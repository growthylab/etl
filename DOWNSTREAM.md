# GrowthyLab ETL engine

Branch `growthy/bayes-replicator` is based on Supabase ETL commit
`9cd1c04c542bb59de58323a86cb8aeb451e7ae8b`. Bayes pins an immutable commit
in Cargo.toml. No upstream pull request is part of this change.

The engine preserves the deployed behavior for custom PostgreSQL type metadata
(including extension-owned pgvector and custom arrays), transactional schema
messages, all-table publication validation and periodic discovery, and stopped
single-table reset with durable schema/checkpoint retention. Source migrations
are kept byte-for-byte compatible with the previously deployed migration set.

The config crate carries the matching DuckLake schema/resource settings. The
maintenance dependency uses the dynamically linked DuckDB 1.5.5 version shared
with Bayes. Error reports preserve their source chain in JSON tracing.

Bayes owns the destination implementation and service binary in its Rust
workspace; it does not compile this fork's etl-replicator or etl-destinations.

Local validation (PostgreSQL 17/pgvector in OrbStack):
- etl-config library: 46 tests
- etl library: 262 tests
- migration integration suite: 13 tests
- all-table discovery / fail-closed publication integration: 2 tests

The Bayes regression suite additionally exercises the engine through a real
DuckLake destination, including COPY, CDC, DDL, restart, custom types and resync.
