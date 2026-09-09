# GrowthyLab ETL engine

Branch `growthy/bayes-replicator` is based on Supabase ETL commit
`9cd1c04c542bb59de58323a86cb8aeb451e7ae8b`. Bayes tracks this branch in Cargo.toml; Cargo.lock records the resolved commit.
Push necessary, validated changes directly to this fork branch without opening
a fork PR. After each push, update Bayes's ETL lockfile resolution to the branch
head and include that update in the Bayes change. Existing builds use the locked
commit until it is updated. No upstream pull request is required now.

The engine preserves the deployed behavior for custom PostgreSQL type metadata
(including extension-owned pgvector and custom arrays), transactional schema
messages, all-table publication validation and periodic discovery, and stopped
single-table reset with durable schema/checkpoint retention. Source migrations
are kept byte-for-byte compatible with the previously deployed migration set.

The config crate carries the matching DuckLake schema/resource settings. The
maintenance dependency uses the dynamically linked DuckDB 1.5.5 version shared
with Bayes. Error reports preserve their source chain in JSON tracing.

Bayes owns its destination adapter and service binary. It uses ETL crates through
existing public interfaces first. A fork change is allowed only when those
interfaces cannot satisfy a necessary requirement and the minimal change has an
independent rationale for a future upstream PR. Reducing Bayes line count alone
is not a reason to expose private modules or move application code into ETL.
Scheduling, table ownership, deployment policy and adapters remain in Bayes.

The maintenance extension exposes existing single-table merge/rewrite operations
for a caller-owned connection. The original public runner creates its own pool
and runs catalog-wide work, which cannot meet an embedded writer's shared-instance
and table-scope requirements. This minimal API is intended for a future upstream
contribution supporting embedded maintenance; it does not introduce a Bayes
scheduler or change the standalone runner's selection policy. Transaction/error
handling and actual rewrite result reporting also benefit the existing runner.

Local validation (PostgreSQL 17/pgvector in OrbStack):
- etl-config library: 46 tests
- etl library: 262 tests
- migration integration suite: 13 tests
- all-table discovery / fail-closed publication integration: 2 tests

The Bayes regression suite additionally exercises the engine through a real
DuckLake destination, including COPY, CDC, DDL, restart, custom types and resync.
