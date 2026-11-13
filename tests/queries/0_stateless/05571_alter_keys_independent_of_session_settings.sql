-- Tags: zookeeper
-- zookeeper: the last case uses ReplicatedMergeTree.

-- A table's keys must stay what `CREATE` and a reload derive, whatever session the last `ALTER` ran in.
-- `transform_null_in` rewrites `in` to `nullIn`, which renames the key column and, for a Nullable argument, changes its type.
-- `cast_keep_nullable` makes `CAST` of a Nullable column Nullable.

SELECT '-- Nullable sorting key, INSERT after the ALTER';
DROP TABLE IF EXISTS t_null_in_sort;
-- One row per granule, so the primary index holds every key value.
CREATE TABLE t_null_in_sort (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY (x IN (NULL, 1))
SETTINGS allow_nullable_key = 1, index_granularity = 1, index_granularity_bytes = 10485760;
ALTER TABLE t_null_in_sort COMMENT COLUMN x 'c' SETTINGS transform_null_in = 1;
INSERT INTO t_null_in_sort VALUES (1), (2), (NULL), (3), (1);
SELECT count() FROM t_null_in_sort;
SELECT mark_number, `in(x, (NULL, 1))` FROM mergeTreeIndex(currentDatabase(), t_null_in_sort)
WHERE rows_in_granule > 0 ORDER BY part_name, mark_number;
DROP TABLE t_null_in_sort;

SELECT '-- Non-Nullable sorting key, merge after the ALTER';
DROP TABLE IF EXISTS t_null_in_merge;
CREATE TABLE t_null_in_merge (x UInt64) ENGINE = MergeTree ORDER BY (x IN (1, 2));
ALTER TABLE t_null_in_merge ADD COLUMN y UInt8 SETTINGS transform_null_in = 1;
INSERT INTO t_null_in_merge (x) VALUES (1), (2), (3);
INSERT INTO t_null_in_merge (x) VALUES (4), (1);
OPTIMIZE TABLE t_null_in_merge FINAL;
SELECT count(), sum(x) FROM t_null_in_merge;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_null_in_merge' AND active;
DROP TABLE t_null_in_merge;

SELECT '-- Nullable partition key, reload after the ALTER';
DROP TABLE IF EXISTS t_null_in_partition;
CREATE TABLE t_null_in_partition (x Nullable(UInt64)) ENGINE = MergeTree PARTITION BY (x IN (NULL, 1)) ORDER BY tuple()
SETTINGS allow_nullable_key = 1;
ALTER TABLE t_null_in_partition COMMENT COLUMN x 'c' SETTINGS transform_null_in = 1;
INSERT INTO t_null_in_partition VALUES (1), (2), (NULL);
DETACH TABLE t_null_in_partition;
ATTACH TABLE t_null_in_partition;
SELECT partition, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_null_in_partition' AND active
ORDER BY partition;
SELECT x FROM t_null_in_partition ORDER BY x;
DROP TABLE t_null_in_partition;

SELECT '-- Sampling key, SAMPLE after the ALTER';
DROP TABLE IF EXISTS t_cast_sample;
CREATE TABLE t_cast_sample (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY CAST(x, 'UInt32') SAMPLE BY CAST(x, 'UInt32');
ALTER TABLE t_cast_sample COMMENT COLUMN x 'c' SETTINGS cast_keep_nullable = 1;
INSERT INTO t_cast_sample VALUES (1), (2), (3);
SELECT count() FROM t_cast_sample SAMPLE 0.5;
DROP TABLE t_cast_sample;

SELECT '-- ReplicatedMergeTree, INSERT after a comment-only ALTER';
DROP TABLE IF EXISTS t_null_in_replicated;
CREATE TABLE t_null_in_replicated (x Nullable(UInt64))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_null_in_replicated', 'r1') ORDER BY (x IN (NULL, 1)) PRIMARY KEY (x IN (NULL, 1))
SETTINGS allow_nullable_key = 1;
ALTER TABLE t_null_in_replicated COMMENT COLUMN x 'c' SETTINGS transform_null_in = 1;
INSERT INTO t_null_in_replicated VALUES (1), (2), (NULL);
SELECT count() FROM t_null_in_replicated;
DROP TABLE t_null_in_replicated;
