-- Tags: zookeeper, no-replicated-database
-- zookeeper: the ReplicatedMergeTree case.
-- no-replicated-database: a Replicated database refuses an ALTER that combines RENAME with DELETE (QUERY_IS_PROHIBITED).

-- A CLEAR COLUMN of a column renamed earlier in the same ALTER did not recompute the MATERIALIZED columns
-- that depend on it on compact and packed parts.

SET mutations_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, c UInt32 MATERIALIZED a * 2) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact (k, b) VALUES (1, 41);
SELECT 'compact', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
ALTER TABLE t_compact RENAME COLUMN b TO x, CLEAR COLUMN x;
SELECT 'compact', k, x, a, c FROM t_compact;
ALTER TABLE t_compact MODIFY COLUMN a REMOVE MATERIALIZED, MODIFY COLUMN c REMOVE MATERIALIZED;
SELECT 'compact-stored', k, x, a, c FROM t_compact;
DROP TABLE t_compact;

DROP TABLE IF EXISTS t_packed;
CREATE TABLE t_packed (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_packed (k, b) VALUES (1, 41);
SELECT 'packed', part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed' AND active;
ALTER TABLE t_packed RENAME COLUMN b TO x, CLEAR COLUMN x;
SELECT 'packed', k, x, a FROM t_packed;
DROP TABLE t_packed;

DROP TABLE IF EXISTS t_dynamic;
CREATE TABLE t_dynamic (k UInt32, b Dynamic, a Dynamic MATERIALIZED b) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_dynamic (k, b) VALUES (1, 41);
SELECT 'dynamic', part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_dynamic' AND active;
ALTER TABLE t_dynamic RENAME COLUMN b TO x, CLEAR COLUMN x;
SELECT 'dynamic', k, x, a, dynamicType(a) FROM t_dynamic;
SELECT 'dynamic-columns', groupArray(column) FROM (SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_dynamic' AND active AND NOT startsWith(column, '_') ORDER BY column);
CHECK TABLE t_dynamic SETTINGS check_query_single_value_result = 1;
DETACH TABLE t_dynamic;
ATTACH TABLE t_dynamic;
SELECT 'dynamic-reload', k, x, a, (SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_dynamic') FROM t_dynamic;
DROP TABLE t_dynamic;

DROP TABLE IF EXISTS t_marker;
CREATE TABLE t_marker (k UInt8, q UInt32, c UInt32 MATERIALIZED q + 1) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_marker (k, q) VALUES (1, 0);
ALTER TABLE t_marker MODIFY COLUMN q UInt32 DEFAULT 99;
ALTER TABLE t_marker RENAME COLUMN q TO r, CLEAR COLUMN r;
SELECT 'marker', r, c FROM t_marker;
DROP TABLE t_marker;

DROP TABLE IF EXISTS t_swap;
CREATE TABLE t_swap (k UInt32, a UInt32, c UInt32 MATERIALIZED a + 1) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_swap (k, a) VALUES (1, 41);
ALTER TABLE t_swap RENAME COLUMN a TO b, RENAME COLUMN c TO a, CLEAR COLUMN b;
SELECT 'swap', b, a FROM t_swap;
CHECK TABLE t_swap SETTINGS check_query_single_value_result = 1;
DETACH TABLE t_swap;
ATTACH TABLE t_swap;
ALTER TABLE t_swap MODIFY COLUMN a REMOVE MATERIALIZED;
SELECT 'swap-stored', b, a FROM t_swap;
DROP TABLE t_swap;

DROP TABLE IF EXISTS t_swap_absent;
CREATE TABLE t_swap_absent (k UInt32, a UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_swap_absent SELECT number, number + 41 FROM numbers(10);
ALTER TABLE t_swap_absent ADD COLUMN c UInt32 MATERIALIZED a + 1;
ALTER TABLE t_swap_absent RENAME COLUMN a TO b, RENAME COLUMN c TO a, CLEAR COLUMN b;
ALTER TABLE t_swap_absent MODIFY COLUMN a REMOVE MATERIALIZED;
SELECT 'swap-absent-stored', sum(b), sum(a), count() FROM t_swap_absent;
DROP TABLE t_swap_absent;

DROP TABLE IF EXISTS t_index;
CREATE TABLE t_index (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, INDEX ia a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_index (k, b) VALUES (1, 41), (2, 41);
ALTER TABLE t_index RENAME COLUMN b TO x, CLEAR COLUMN x;
SELECT 'index', count() FROM t_index WHERE a = 1 SETTINGS force_data_skipping_indices = 'ia', use_skip_indexes = 1, use_query_condition_cache = 0;
DROP TABLE t_index;

DROP TABLE IF EXISTS t_projection;
CREATE TABLE t_projection (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, PROJECTION p (SELECT a, count() GROUP BY a)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_projection (k, b) VALUES (1, 41), (2, 41);
ALTER TABLE t_projection RENAME COLUMN b TO x, CLEAR COLUMN x;
SELECT 'projection', a, count() FROM t_projection GROUP BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_projection;

DROP TABLE IF EXISTS t_replicated;
CREATE TABLE t_replicated (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_replicated (k, b) VALUES (1, 41);
ALTER TABLE t_replicated RENAME COLUMN b TO x, CLEAR COLUMN x;
SELECT 'replicated', k, x, a FROM t_replicated;
DROP TABLE t_replicated SYNC;

DROP TABLE IF EXISTS t_delete;
CREATE TABLE t_delete (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_delete (k, b) VALUES (1, 41), (2, 42);
ALTER TABLE t_delete RENAME COLUMN b TO x, CLEAR COLUMN x, DELETE WHERE k = 2;
SELECT 'delete', k, x, a FROM t_delete;
DROP TABLE t_delete;

-- Unchanged by the fix (no recomputed dependent, or the part does not store the cleared column).
DROP TABLE IF EXISTS t_index_cleared;
CREATE TABLE t_index_cleared (k UInt32, b UInt32, INDEX ix b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_index_cleared (k, b) VALUES (1, 41), (2, 41);
ALTER TABLE t_index_cleared RENAME COLUMN b TO x, CLEAR COLUMN x;
SELECT 'index-cleared', count() FROM t_index_cleared WHERE x = 0 SETTINGS force_data_skipping_indices = 'ix', use_skip_indexes = 1, use_query_condition_cache = 0;
SELECT 'index-cleared', count() FROM t_index_cleared WHERE x = 41 SETTINGS force_data_skipping_indices = 'ix', use_skip_indexes = 1, use_query_condition_cache = 0;
DROP TABLE t_index_cleared;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (k UInt32, n Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_nested VALUES (1, [1, 2], [3, 4]);
ALTER TABLE t_nested RENAME COLUMN n.a TO n.c, CLEAR COLUMN n.c;
SELECT 'nested', k, n.c, n.b FROM t_nested;
DROP TABLE t_nested;

DROP TABLE IF EXISTS t_marker_swap;
CREATE TABLE t_marker_swap (k UInt8, a UInt32, c UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_marker_swap VALUES (1, 0, 42);
ALTER TABLE t_marker_swap RENAME COLUMN a TO b, RENAME COLUMN c TO a, CLEAR COLUMN b;
SELECT 'marker-swap', b, a FROM t_marker_swap;
CHECK TABLE t_marker_swap SETTINGS check_query_single_value_result = 1;
DETACH TABLE t_marker_swap;
ATTACH TABLE t_marker_swap;
SELECT 'marker-swap-reload', b, a, (SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_marker_swap') FROM t_marker_swap;
DROP TABLE t_marker_swap;

DROP TABLE IF EXISTS t_absent;
CREATE TABLE t_absent (k UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_absent VALUES (1);
ALTER TABLE t_absent ADD COLUMN y UInt32 DEFAULT 5;
ALTER TABLE t_absent ADD COLUMN a UInt32 MATERIALIZED y + 1;
ALTER TABLE t_absent MATERIALIZE COLUMN a;
ALTER TABLE t_absent MODIFY COLUMN y UInt32 DEFAULT 7;
ALTER TABLE t_absent RENAME COLUMN y TO x, CLEAR COLUMN x;
SELECT 'absent', k, x, a FROM t_absent;
DROP TABLE t_absent;
