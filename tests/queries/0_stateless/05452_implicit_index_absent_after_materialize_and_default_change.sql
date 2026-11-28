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

-- A change of the type is not guaranteed to rewrite every part, because its mutation can be killed (see below),
-- so the index stays absent.
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

-- A change of the type that keeps the stored bytes (`UInt32` <-> `DateTime`) rewrites nothing either.
DROP TABLE IF EXISTS t_local_dt;
DROP TABLE IF EXISTS t_replicated_dt;

CREATE TABLE t_local_dt (a UInt64, payload String, b UInt32 ALIAS toUInt32(a + 1))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_local_dt SELECT number, toString(number) FROM numbers(100000);
ALTER TABLE t_local_dt MODIFY COLUMN b UInt32 DEFAULT 0;
ALTER TABLE t_local_dt MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_local_dt MODIFY COLUMN b DateTime DEFAULT toDateTime(0);
ALTER TABLE t_local_dt MODIFY COLUMN b UInt32 DEFAULT 0;
SELECT sum(a + 1) FROM t_local_dt WHERE b = 0;
SELECT sum(a + 1) FROM t_local_dt WHERE b = 0 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_local_dt' ORDER BY name;

CREATE TABLE t_replicated_dt (a UInt64, payload String, b UInt32 ALIAS toUInt32(a + 1))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated_dt_05452', 'r1') ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_replicated_dt SELECT number, toString(number) FROM numbers(100000);
ALTER TABLE t_replicated_dt MODIFY COLUMN b UInt32 DEFAULT 0;
ALTER TABLE t_replicated_dt MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_replicated_dt MODIFY COLUMN b DateTime DEFAULT toDateTime(0);
ALTER TABLE t_replicated_dt MODIFY COLUMN b UInt32 DEFAULT 0;
SELECT sum(a + 1) FROM t_replicated_dt WHERE b = 0;
SELECT sum(a + 1) FROM t_replicated_dt WHERE b = 0 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_replicated_dt' ORDER BY name;

-- `EPHEMERAL` does not remove the data of the column from the parts, so turning the column physical again reads it back.
DROP TABLE IF EXISTS t_local_eph;
DROP TABLE IF EXISTS t_replicated_eph;

CREATE TABLE t_local_eph (a UInt64, payload String, b UInt64 ALIAS a + 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_local_eph SELECT number, toString(number) FROM numbers(100000);
ALTER TABLE t_local_eph MODIFY COLUMN b UInt64 DEFAULT 0;
ALTER TABLE t_local_eph MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_local_eph MODIFY COLUMN b UInt64 EPHEMERAL 0;
ALTER TABLE t_local_eph MODIFY COLUMN b UInt64 DEFAULT 0;
SELECT sum(a + 1) FROM t_local_eph WHERE b = 0;
SELECT sum(a + 1) FROM t_local_eph WHERE b = 0 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_local_eph' ORDER BY name;

CREATE TABLE t_replicated_eph (a UInt64, payload String, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated_eph_05452', 'r1') ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_replicated_eph SELECT number, toString(number) FROM numbers(100000);
ALTER TABLE t_replicated_eph MODIFY COLUMN b UInt64 DEFAULT 0;
ALTER TABLE t_replicated_eph MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_replicated_eph MODIFY COLUMN b UInt64 EPHEMERAL 0;
ALTER TABLE t_replicated_eph MODIFY COLUMN b UInt64 DEFAULT 0;
SELECT sum(a + 1) FROM t_replicated_eph WHERE b = 0;
SELECT sum(a + 1) FROM t_replicated_eph WHERE b = 0 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_replicated_eph' ORDER BY name;

-- The mutations of the type changes are killed before they rewrite any part, so the parts keep the files built over `a + 1`.
DROP TABLE IF EXISTS t_local_kill;

CREATE TABLE t_local_kill (a UInt64, payload String, b UInt64 ALIAS a + 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_local_kill SELECT number, toString(number) FROM numbers(100000);
ALTER TABLE t_local_kill MODIFY COLUMN b UInt64 DEFAULT 0;
ALTER TABLE t_local_kill MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_local_kill;
ALTER TABLE t_local_kill MODIFY COLUMN b UInt32 DEFAULT 0 SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_local_kill MODIFY COLUMN b UInt64 DEFAULT 0 SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_local_kill' SYNC FORMAT Null;
SYSTEM START MERGES t_local_kill;
SELECT sum(a + 1) FROM t_local_kill WHERE b = 0;
SELECT sum(a + 1) FROM t_local_kill WHERE b = 0 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_local_kill' ORDER BY name;

-- A column that the policy did not cover before gets the index when a change of the type makes it covered.
DROP TABLE IF EXISTS t_local_str;

CREATE TABLE t_local_str (a UInt64, s String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, add_minmax_index_for_string_columns = 0;
INSERT INTO t_local_str SELECT number, toString(number) FROM numbers(1000);
ALTER TABLE t_local_str MODIFY COLUMN s UInt64 SETTINGS mutations_sync = 2;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_local_str' ORDER BY name;

DROP TABLE t_local;
DROP TABLE t_replicated;
DROP TABLE t_local_dt;
DROP TABLE t_replicated_dt;
DROP TABLE t_local_eph;
DROP TABLE t_replicated_eph;
DROP TABLE t_local_kill;
DROP TABLE t_local_str;
