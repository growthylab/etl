//! Where streaming batches record replay progress, and what the writer already
//! knows about it.
//!
//! Every streaming batch appends its last sequence key to a progress table in
//! the same transaction as its data, so the durable frontier always matches the
//! committed data. The writer is the only one advancing a table's frontier, so
//! it can remember the frontier each confirmed commit leaves behind instead of
//! reading it back from the lake before every batch. The durable frontier is
//! read only when the remembered one is unknown: after a restart, after an
//! attempt whose outcome was not confirmed, and after a replay epoch change.
//!
//! DuckLake detects conflicts per table: an insert conflicts with any
//! concurrent delete from the same table. Several pipelines sharing one catalog
//! and one progress table therefore conflict whenever one of them prunes its
//! old progress rows while another commits. A host can give each pipeline its
//! own progress table to prune under its own writer pause. Batches keep
//! appending to the shared table too, which nobody deletes from any more, and
//! frontier reads take the later of both, so moving to a separate table and
//! rolling back to a version that only knows the shared table both keep the
//! replay position.

use std::{collections::HashMap, sync::Arc};

use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    event::EventSequenceKey,
};
use parking_lot::Mutex;

use crate::ducklake::DuckLakeTableName;

/// Progress table shared by every pipeline that does not name its own.
pub(super) const SHARED_STREAMING_PROGRESS_TABLE: &str = "__etl_streaming_progress";

/// The lake table streaming batches record their replay progress in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StreamingProgressTable {
    name: Arc<str>,
}

impl StreamingProgressTable {
    /// Returns a pipeline-specific table named
    /// `__etl_streaming_progress_<suffix>`.
    ///
    /// The suffix is restricted to lowercase ASCII letters, digits and
    /// underscores: DuckDB folds identifier case, so two suffixes differing
    /// only in case would name one physical table and share it again. The name
    /// keeps the reserved `__etl_` prefix that hides helper tables from table
    /// discovery.
    pub(super) fn with_suffix(suffix: &str) -> EtlResult<Self> {
        if suffix.is_empty()
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "Invalid DuckLake streaming progress table suffix",
                format!(
                    "Suffix `{suffix}` must be nonempty lowercase ASCII letters, digits or \
                     underscores"
                )
            ));
        }

        Ok(Self { name: Arc::from(format!("{SHARED_STREAMING_PROGRESS_TABLE}_{suffix}")) })
    }

    /// Returns the unquoted table name inside the lake's default schema.
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    /// Returns the shared table, when this is not that table.
    ///
    /// A pipeline with its own table keeps appending every frontier to the
    /// shared table as well and never deletes from it, so a version that only
    /// knows the shared table can take over after a rollback. Inserts do not
    /// conflict with each other; only deletes do.
    pub(super) fn legacy(&self) -> Option<&'static str> {
        (self.name.as_ref() != SHARED_STREAMING_PROGRESS_TABLE)
            .then_some(SHARED_STREAMING_PROGRESS_TABLE)
    }

    /// Returns every table a committed batch appends its frontier to: this
    /// table first, then the shared table when this is not it.
    pub(super) fn written_tables(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.name()).chain(self.legacy())
    }
}

impl Default for StreamingProgressTable {
    fn default() -> Self {
        Self { name: Arc::from(SHARED_STREAMING_PROGRESS_TABLE) }
    }
}

/// One table's durable frontier as the writer last confirmed it.
#[derive(Clone, Debug)]
struct KnownFrontier {
    replay_epoch: String,
    last_sequence_key: Option<EventSequenceKey>,
}

/// Durable frontiers this writer confirmed, per destination table.
///
/// An entry is present only while it is certain: it is recorded from a durable
/// read or from a confirmed commit, and it is forgotten before every attempt
/// that could change the table's frontier, so an attempt that fails, times out
/// or is cancelled leaves the table unknown and the next batch reads the
/// durable frontier again.
#[derive(Clone, Debug, Default)]
pub(super) struct StreamingProgressCache {
    frontiers: Arc<Mutex<HashMap<DuckLakeTableName, KnownFrontier>>>,
}

impl StreamingProgressCache {
    /// Returns the confirmed frontier of `table_name` in `replay_epoch`.
    ///
    /// The outer `None` means unknown; `Some(None)` means the table has no
    /// progress in that epoch yet.
    pub(super) fn get(
        &self,
        table_name: &DuckLakeTableName,
        replay_epoch: &str,
    ) -> Option<Option<EventSequenceKey>> {
        self.frontiers
            .lock()
            .get(table_name)
            .filter(|known| known.replay_epoch == replay_epoch)
            .map(|known| known.last_sequence_key)
    }

    /// Records a frontier read from the lake or left behind by a confirmed
    /// commit.
    pub(super) fn record(
        &self,
        table_name: &DuckLakeTableName,
        replay_epoch: &str,
        last_sequence_key: Option<EventSequenceKey>,
    ) {
        self.frontiers.lock().insert(
            table_name.clone(),
            KnownFrontier { replay_epoch: replay_epoch.to_owned(), last_sequence_key },
        );
    }

    /// Forgets the frontier of `table_name` until the lake is read again.
    pub(super) fn forget(&self, table_name: &DuckLakeTableName) {
        self.frontiers.lock().remove(table_name);
    }
}

/// A destination's streaming progress: where batches record it and which
/// frontiers the writer already confirmed.
#[derive(Clone, Debug, Default)]
pub(super) struct StreamingProgress {
    table: StreamingProgressTable,
    cache: StreamingProgressCache,
}

impl StreamingProgress {
    /// Creates the progress state of a destination recording into `table`.
    pub(super) fn new(table: StreamingProgressTable) -> Self {
        Self { table, cache: StreamingProgressCache::default() }
    }

    /// Returns the table streaming batches record their progress in.
    pub(super) fn table(&self) -> &StreamingProgressTable {
        &self.table
    }

    /// Returns the frontiers this writer confirmed.
    pub(super) fn cache(&self) -> &StreamingProgressCache {
        &self.cache
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::PgLsn;

    use super::*;

    fn key(lsn: u64, ordinal: u64) -> EventSequenceKey {
        EventSequenceKey::new(PgLsn::from(lsn), ordinal)
    }

    #[test]
    fn suffix_names_a_reserved_helper_and_rejects_unsafe_input() {
        let table = StreamingProgressTable::with_suffix("pipeline_7").unwrap();
        assert_eq!(table.name(), "__etl_streaming_progress_pipeline_7");
        assert_eq!(table.legacy(), Some(SHARED_STREAMING_PROGRESS_TABLE));
        assert_eq!(
            table.written_tables().collect::<Vec<_>>(),
            ["__etl_streaming_progress_pipeline_7", SHARED_STREAMING_PROGRESS_TABLE]
        );
        assert!(DuckLakeTableName::new("main", table.name()).is_internal_helper());

        let shared = StreamingProgressTable::default();
        assert_eq!(shared.name(), SHARED_STREAMING_PROGRESS_TABLE);
        assert_eq!(shared.legacy(), None);
        assert_eq!(shared.written_tables().collect::<Vec<_>>(), [SHARED_STREAMING_PROGRESS_TABLE]);

        for suffix in ["", "a-b", "a\"b", "a b", "é", "x;drop", "Pipeline_7", "PIPELINE_7"] {
            let error = StreamingProgressTable::with_suffix(suffix).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ConfigError);
        }
    }

    #[test]
    fn cache_is_scoped_to_one_epoch_and_forgets_on_request() {
        let cache = StreamingProgressCache::default();
        let table = DuckLakeTableName::new("public", "users");
        let other = DuckLakeTableName::new("public", "orders");
        assert_eq!(cache.get(&table, "e1"), None);

        cache.record(&table, "e1", None);
        assert_eq!(cache.get(&table, "e1"), Some(None));
        cache.record(&table, "e1", Some(key(10, 2)));
        cache.record(&other, "e1", Some(key(3, 0)));
        assert_eq!(cache.get(&table, "e1"), Some(Some(key(10, 2))));
        // A frontier from another epoch is never reused.
        assert_eq!(cache.get(&table, "e2"), None);

        cache.forget(&table);
        assert_eq!(cache.get(&table, "e1"), None);
        assert_eq!(cache.get(&other, "e1"), Some(Some(key(3, 0))));

        // Clones share state: the destination hands clones to table tasks.
        let clone = cache.clone();
        clone.forget(&other);
        assert_eq!(cache.get(&other, "e1"), None);
    }
}
