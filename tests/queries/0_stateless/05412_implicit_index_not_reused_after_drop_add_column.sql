-- An implicit index is identified only by its name. Dropping an `ALIAS` column leaves the files of its
-- implicit index in the existing parts, so a column added later under the same name must not get an
-- implicit index that would read those files and prune wrong.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_physical;
DROP TABLE IF EXISTS t_replicated;
DROP TABLE IF EXISTS t_batch;

-- An expression alias is replaced by another one.
CREATE TABLE t_alias (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_alias SELECT number FROM numbers(100000);
ALTER TABLE t_alias DROP COLUMN b;
ALTER TABLE t_alias ADD COLUMN b UInt64 ALIAS a + 1000000;
SELECT count() FROM t_alias WHERE b = 1000001;
SELECT count() FROM t_alias WHERE b = 1000001 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_alias' ORDER BY name;

-- An expression alias is replaced by a physical column, which reads the default in the existing parts.
CREATE TABLE t_physical (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_physical SELECT number FROM numbers(100000);
ALTER TABLE t_physical DROP COLUMN b;
ALTER TABLE t_physical ADD COLUMN b UInt64;
SELECT count() FROM t_physical WHERE b = 0;
SELECT count() FROM t_physical WHERE b = 0 SETTINGS use_skip_indexes = 0;

-- The same on a replicated table, including an unrelated `ALTER` afterwards.
CREATE TABLE t_replicated (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated_05412', 'r1') ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_replicated SELECT number FROM numbers(100000);
ALTER TABLE t_replicated DROP COLUMN b;
ALTER TABLE t_replicated ADD COLUMN b UInt64 ALIAS a + 1000000;
ALTER TABLE t_replicated ADD COLUMN c UInt8;
SELECT count() FROM t_replicated WHERE b = 1000001;
SELECT count() FROM t_replicated WHERE b = 1000001 SETTINGS use_skip_indexes = 0;

-- The alias is dropped and added again within one `ALTER`.
CREATE TABLE t_batch (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_batch SELECT number FROM numbers(100000);
ALTER TABLE t_batch DROP COLUMN b, ADD COLUMN b UInt64 ALIAS a + 1000000;
SELECT count() FROM t_batch WHERE b = 1000001;
SELECT count() FROM t_batch WHERE b = 1000001 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_batch' ORDER BY name;

-- A column added under a new name still gets its implicit index.
ALTER TABLE t_alias ADD COLUMN d UInt64 ALIAS a * 2;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_alias' ORDER BY name;

DROP TABLE t_alias;
DROP TABLE t_physical;
DROP TABLE t_replicated;
DROP TABLE t_batch;
