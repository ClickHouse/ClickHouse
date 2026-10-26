-- `MODIFY COLUMN b ... DEFAULT 0` of `b ALIAS a + 1` drops the implicit index of `b`, but its files built over
-- `a + 1` stay in the parts, and `MATERIALIZE COLUMN b` carries them over. A later change of only the default
-- of the now physical column rewrites nothing, so it must not re-create the index over those files.
-- The queries read the column, because a trivial `count` could bypass the index.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_replicated;

CREATE TABLE t_local (a UInt64, payload String, b UInt64 ALIAS a + 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_local SELECT number, toString(number) FROM numbers(100000);
ALTER TABLE t_local MODIFY COLUMN b UInt64 DEFAULT 0;
ALTER TABLE t_local MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_local MODIFY COLUMN b UInt64 DEFAULT 1;
SELECT sum(a + 1) FROM t_local WHERE b = 0;
SELECT sum(a + 1) FROM t_local WHERE b = 0 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_local' ORDER BY name;

-- A change of the type rewrites the column and builds fresh index files, so the index comes back.
ALTER TABLE t_local MODIFY COLUMN b UInt32 DEFAULT 1 SETTINGS mutations_sync = 2;
SELECT sum(a + 1) FROM t_local WHERE b = 0;
SELECT sum(a + 1) FROM t_local WHERE b = 0 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_local' ORDER BY name;

CREATE TABLE t_replicated (a UInt64, payload String, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated_05452', 'r1') ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_replicated SELECT number, toString(number) FROM numbers(100000);
ALTER TABLE t_replicated MODIFY COLUMN b UInt64 DEFAULT 0;
ALTER TABLE t_replicated MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_replicated MODIFY COLUMN b UInt64 DEFAULT 1;
SELECT sum(a + 1) FROM t_replicated WHERE b = 0;
SELECT sum(a + 1) FROM t_replicated WHERE b = 0 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_replicated' ORDER BY name;

DROP TABLE t_local;
DROP TABLE t_replicated;
