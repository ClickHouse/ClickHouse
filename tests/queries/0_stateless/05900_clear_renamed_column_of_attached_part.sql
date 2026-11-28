-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: relies on `ReplicatedMergeTree` attaching a detached part with its old metadata version, so that
-- the rename reaches it only through the rename map of the next mutation

-- A CLEAR COLUMN of a column renamed while its partition was detached did not recompute the MATERIALIZED
-- columns that depend on it: the attached part learns the rename only from its metadata version.
-- Each case is followed by its control, the same statements without DETACH/ATTACH PARTITION.

SET mutations_sync = 2, alter_sync = 2;

-- Compact part, transitive dependent.
DROP TABLE IF EXISTS t_compact SYNC;
CREATE TABLE t_compact (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, c UInt32 MATERIALIZED a * 2)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_compact', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact (k, b) VALUES (1, 41);
ALTER TABLE t_compact DETACH PARTITION tuple();
ALTER TABLE t_compact RENAME COLUMN b TO x;
ALTER TABLE t_compact ATTACH PARTITION tuple();
ALTER TABLE t_compact CLEAR COLUMN x;
SELECT 'compact', k, x, a, c FROM t_compact;
SELECT 'compact-columns', groupArray(column) FROM (SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_compact' AND active AND NOT startsWith(column, '_') ORDER BY column);
CHECK TABLE t_compact SETTINGS check_query_single_value_result = 1;
ALTER TABLE t_compact MODIFY COLUMN a REMOVE MATERIALIZED, MODIFY COLUMN c REMOVE MATERIALIZED;
SELECT 'compact-stored', k, x, a, c FROM t_compact;
DROP TABLE t_compact SYNC;

DROP TABLE IF EXISTS t_compact_ctl SYNC;
CREATE TABLE t_compact_ctl (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, c UInt32 MATERIALIZED a * 2)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_compact_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact_ctl (k, b) VALUES (1, 41);
ALTER TABLE t_compact_ctl RENAME COLUMN b TO x;
ALTER TABLE t_compact_ctl CLEAR COLUMN x;
SELECT 'compact', k, x, a, c FROM t_compact_ctl;
SELECT 'compact-columns', groupArray(column) FROM (SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_compact_ctl' AND active AND NOT startsWith(column, '_') ORDER BY column);
CHECK TABLE t_compact_ctl SETTINGS check_query_single_value_result = 1;
ALTER TABLE t_compact_ctl MODIFY COLUMN a REMOVE MATERIALIZED, MODIFY COLUMN c REMOVE MATERIALIZED;
SELECT 'compact-stored', k, x, a, c FROM t_compact_ctl;
DROP TABLE t_compact_ctl SYNC;

-- Wide part (per-file storage).
DROP TABLE IF EXISTS t_wide SYNC;
CREATE TABLE t_wide (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, INDEX ia a TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_wide', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 1;
INSERT INTO t_wide (k, b) VALUES (1, 41), (2, 41);
ALTER TABLE t_wide DETACH PARTITION tuple();
ALTER TABLE t_wide RENAME COLUMN b TO x;
ALTER TABLE t_wide ATTACH PARTITION tuple();
ALTER TABLE t_wide CLEAR COLUMN x;
SELECT 'wide', k, x, a FROM t_wide ORDER BY k;
SELECT 'wide-index', count() FROM t_wide WHERE a = 1 SETTINGS force_data_skipping_indices = 'ia', use_skip_indexes = 1, use_query_condition_cache = 0;
SELECT 'wide-columns', groupArray(column) FROM (SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_wide' AND active AND NOT startsWith(column, '_') ORDER BY column);
CHECK TABLE t_wide SETTINGS check_query_single_value_result = 1;
ALTER TABLE t_wide MODIFY COLUMN a REMOVE MATERIALIZED;
SELECT 'wide-stored', k, x, a FROM t_wide ORDER BY k;
DROP TABLE t_wide SYNC;

DROP TABLE IF EXISTS t_wide_ctl SYNC;
CREATE TABLE t_wide_ctl (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, INDEX ia a TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_wide_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 1;
INSERT INTO t_wide_ctl (k, b) VALUES (1, 41), (2, 41);
ALTER TABLE t_wide_ctl RENAME COLUMN b TO x;
ALTER TABLE t_wide_ctl CLEAR COLUMN x;
SELECT 'wide', k, x, a FROM t_wide_ctl ORDER BY k;
SELECT 'wide-index', count() FROM t_wide_ctl WHERE a = 1 SETTINGS force_data_skipping_indices = 'ia', use_skip_indexes = 1, use_query_condition_cache = 0;
SELECT 'wide-columns', groupArray(column) FROM (SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_wide_ctl' AND active AND NOT startsWith(column, '_') ORDER BY column);
CHECK TABLE t_wide_ctl SETTINGS check_query_single_value_result = 1;
ALTER TABLE t_wide_ctl MODIFY COLUMN a REMOVE MATERIALIZED;
SELECT 'wide-stored', k, x, a FROM t_wide_ctl ORDER BY k;
DROP TABLE t_wide_ctl SYNC;

-- Wide part with packed storage.
DROP TABLE IF EXISTS t_packed SYNC;
CREATE TABLE t_packed (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_packed', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_packed (k, b) VALUES (1, 41);
ALTER TABLE t_packed DETACH PARTITION tuple();
ALTER TABLE t_packed RENAME COLUMN b TO x;
ALTER TABLE t_packed ATTACH PARTITION tuple();
ALTER TABLE t_packed CLEAR COLUMN x;
SELECT 'packed', k, x, a, (SELECT any(part_storage_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_packed' AND active) FROM t_packed;
DROP TABLE t_packed SYNC;

DROP TABLE IF EXISTS t_packed_ctl SYNC;
CREATE TABLE t_packed_ctl (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_packed_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_packed_ctl (k, b) VALUES (1, 41);
ALTER TABLE t_packed_ctl RENAME COLUMN b TO x;
ALTER TABLE t_packed_ctl CLEAR COLUMN x;
SELECT 'packed', k, x, a, (SELECT any(part_storage_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_ctl' AND active) FROM t_packed_ctl;
DROP TABLE t_packed_ctl SYNC;

-- Wide part with a stored Dynamic column and a Dynamic dependent.
DROP TABLE IF EXISTS t_dynamic SYNC;
CREATE TABLE t_dynamic (k UInt32, b Dynamic, a Dynamic MATERIALIZED b)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dynamic', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_dynamic (k, b) VALUES (1, 41);
ALTER TABLE t_dynamic DETACH PARTITION tuple();
ALTER TABLE t_dynamic RENAME COLUMN b TO x;
ALTER TABLE t_dynamic ATTACH PARTITION tuple();
ALTER TABLE t_dynamic CLEAR COLUMN x;
SELECT 'dynamic', k, x, a FROM t_dynamic;
CHECK TABLE t_dynamic SETTINGS check_query_single_value_result = 1;
SELECT 'dynamic-count', count() FROM t_dynamic;
DROP TABLE t_dynamic SYNC;

DROP TABLE IF EXISTS t_dynamic_ctl SYNC;
CREATE TABLE t_dynamic_ctl (k UInt32, b Dynamic, a Dynamic MATERIALIZED b)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dynamic_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_dynamic_ctl (k, b) VALUES (1, 41);
ALTER TABLE t_dynamic_ctl RENAME COLUMN b TO x;
ALTER TABLE t_dynamic_ctl CLEAR COLUMN x;
SELECT 'dynamic', k, x, a FROM t_dynamic_ctl;
CHECK TABLE t_dynamic_ctl SETTINGS check_query_single_value_result = 1;
SELECT 'dynamic-count', count() FROM t_dynamic_ctl;
DROP TABLE t_dynamic_ctl SYNC;

-- Wide part, a plain column with a JSON dependent whose recomputed value has fewer streams.
DROP TABLE IF EXISTS t_json SYNC;
CREATE TABLE t_json (k UInt32, b UInt32, a JSON MATERIALIZED if(b = 0, '{}', '{"p":1}'))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_json', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_json (k, b) VALUES (1, 41);
ALTER TABLE t_json DETACH PARTITION tuple();
ALTER TABLE t_json RENAME COLUMN b TO x;
ALTER TABLE t_json ATTACH PARTITION tuple();
ALTER TABLE t_json CLEAR COLUMN x;
SELECT 'json', k, x, a.p FROM t_json;
CHECK TABLE t_json SETTINGS check_query_single_value_result = 1;
SELECT 'json-count', count() FROM t_json;
DROP TABLE t_json SYNC;

DROP TABLE IF EXISTS t_json_ctl SYNC;
CREATE TABLE t_json_ctl (k UInt32, b UInt32, a JSON MATERIALIZED if(b = 0, '{}', '{"p":1}'))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_json_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_json_ctl (k, b) VALUES (1, 41);
ALTER TABLE t_json_ctl RENAME COLUMN b TO x;
ALTER TABLE t_json_ctl CLEAR COLUMN x;
SELECT 'json', k, x, a.p FROM t_json_ctl;
CHECK TABLE t_json_ctl SETTINGS check_query_single_value_result = 1;
SELECT 'json-count', count() FROM t_json_ctl;
DROP TABLE t_json_ctl SYNC;

-- Missing-column marker, compact and wide.
DROP TABLE IF EXISTS t_marker SYNC;
CREATE TABLE t_marker (k UInt8, q UInt32, c UInt32 MATERIALIZED q + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_marker', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_marker (k, q) VALUES (1, 0);
ALTER TABLE t_marker MODIFY COLUMN q UInt32 DEFAULT 99;
ALTER TABLE t_marker DETACH PARTITION tuple();
ALTER TABLE t_marker RENAME COLUMN q TO r;
ALTER TABLE t_marker ATTACH PARTITION tuple();
ALTER TABLE t_marker CLEAR COLUMN r;
SELECT 'marker', r, c FROM t_marker;
DROP TABLE t_marker SYNC;

DROP TABLE IF EXISTS t_marker_ctl SYNC;
CREATE TABLE t_marker_ctl (k UInt8, q UInt32, c UInt32 MATERIALIZED q + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_marker_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_marker_ctl (k, q) VALUES (1, 0);
ALTER TABLE t_marker_ctl MODIFY COLUMN q UInt32 DEFAULT 99;
ALTER TABLE t_marker_ctl RENAME COLUMN q TO r;
ALTER TABLE t_marker_ctl CLEAR COLUMN r;
SELECT 'marker', r, c FROM t_marker_ctl;
DROP TABLE t_marker_ctl SYNC;

DROP TABLE IF EXISTS t_marker_wide SYNC;
CREATE TABLE t_marker_wide (k UInt8, q UInt32, c UInt32 MATERIALIZED q + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_marker_wide', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_marker_wide (k, q) VALUES (1, 0);
ALTER TABLE t_marker_wide MODIFY COLUMN q UInt32 DEFAULT 99;
ALTER TABLE t_marker_wide DETACH PARTITION tuple();
ALTER TABLE t_marker_wide RENAME COLUMN q TO r;
ALTER TABLE t_marker_wide ATTACH PARTITION tuple();
ALTER TABLE t_marker_wide CLEAR COLUMN r;
SELECT 'marker-wide', r, c FROM t_marker_wide;
DROP TABLE t_marker_wide SYNC;

DROP TABLE IF EXISTS t_marker_wide_ctl SYNC;
CREATE TABLE t_marker_wide_ctl (k UInt8, q UInt32, c UInt32 MATERIALIZED q + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_marker_wide_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_marker_wide_ctl (k, q) VALUES (1, 0);
ALTER TABLE t_marker_wide_ctl MODIFY COLUMN q UInt32 DEFAULT 99;
ALTER TABLE t_marker_wide_ctl RENAME COLUMN q TO r;
ALTER TABLE t_marker_wide_ctl CLEAR COLUMN r;
SELECT 'marker-wide', r, c FROM t_marker_wide_ctl;
DROP TABLE t_marker_wide_ctl SYNC;

-- Renamed while detached, then renamed again and cleared in one ALTER.
DROP TABLE IF EXISTS t_chain SYNC;
CREATE TABLE t_chain (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_chain', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_chain (k, b) VALUES (1, 41);
ALTER TABLE t_chain DETACH PARTITION tuple();
ALTER TABLE t_chain RENAME COLUMN b TO y;
ALTER TABLE t_chain ATTACH PARTITION tuple();
ALTER TABLE t_chain RENAME COLUMN y TO x, CLEAR COLUMN x;
SELECT 'chain', k, x, a FROM t_chain;
DROP TABLE t_chain SYNC;

DROP TABLE IF EXISTS t_chain_ctl SYNC;
CREATE TABLE t_chain_ctl (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_chain_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_chain_ctl (k, b) VALUES (1, 41);
ALTER TABLE t_chain_ctl RENAME COLUMN b TO y;
ALTER TABLE t_chain_ctl RENAME COLUMN y TO x;
ALTER TABLE t_chain_ctl CLEAR COLUMN x;
SELECT 'chain', k, x, a FROM t_chain_ctl;
DROP TABLE t_chain_ctl SYNC;

-- CLEAR ... IN PARTITION, and a skip index over the dependent.
DROP TABLE IF EXISTS t_partition SYNC;
CREATE TABLE t_partition (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, INDEX ia a TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_partition', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_partition (k, b) VALUES (1, 41), (2, 41);
ALTER TABLE t_partition DETACH PARTITION tuple();
ALTER TABLE t_partition RENAME COLUMN b TO x;
ALTER TABLE t_partition ATTACH PARTITION tuple();
ALTER TABLE t_partition CLEAR COLUMN x IN PARTITION tuple();
SELECT 'partition', k, x, a FROM t_partition ORDER BY k;
SELECT 'partition-index', count() FROM t_partition WHERE a = 1 SETTINGS force_data_skipping_indices = 'ia', use_skip_indexes = 1, use_query_condition_cache = 0;
DROP TABLE t_partition SYNC;

DROP TABLE IF EXISTS t_partition_ctl SYNC;
CREATE TABLE t_partition_ctl (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1, INDEX ia a TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_partition_ctl', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_partition_ctl (k, b) VALUES (1, 41), (2, 41);
ALTER TABLE t_partition_ctl RENAME COLUMN b TO x;
ALTER TABLE t_partition_ctl CLEAR COLUMN x IN PARTITION tuple();
SELECT 'partition', k, x, a FROM t_partition_ctl ORDER BY k;
SELECT 'partition-index', count() FROM t_partition_ctl WHERE a = 1 SETTINGS force_data_skipping_indices = 'ia', use_skip_indexes = 1, use_query_condition_cache = 0;
DROP TABLE t_partition_ctl SYNC;
