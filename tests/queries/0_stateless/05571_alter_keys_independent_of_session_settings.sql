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

-- The same holds for projections: their parts persist the projection's sorting key and column types.

SELECT '-- Projection sorting key, INSERT after the ALTER';
DROP TABLE IF EXISTS t_cast_projection;
CREATE TABLE t_cast_projection (x Nullable(UInt64), y UInt64, PROJECTION p (SELECT x, y ORDER BY CAST(x, 'UInt32')))
ENGINE = MergeTree ORDER BY y;
ALTER TABLE t_cast_projection COMMENT COLUMN x 'c' SETTINGS cast_keep_nullable = 1;
INSERT INTO t_cast_projection VALUES (1, 10), (2, 20), (3, 30);
SELECT count() FROM t_cast_projection;
SELECT name, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_cast_projection' AND active GROUP BY name;
DROP TABLE t_cast_projection;

SELECT '-- Projection sorting key, merge after the ALTER';
DROP TABLE IF EXISTS t_null_in_projection;
CREATE TABLE t_null_in_projection (x UInt64, y UInt64, PROJECTION p (SELECT x, y ORDER BY (x IN (1, 2))))
ENGINE = MergeTree ORDER BY y;
ALTER TABLE t_null_in_projection COMMENT COLUMN x 'c' SETTINGS transform_null_in = 1;
INSERT INTO t_null_in_projection VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t_null_in_projection VALUES (4, 40), (1, 50);
OPTIMIZE TABLE t_null_in_projection FINAL;
SELECT count(), sum(x) FROM t_null_in_projection;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_null_in_projection' AND active;
SELECT name, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_null_in_projection' AND active GROUP BY name;
DROP TABLE t_null_in_projection;

SELECT '-- Projection added by the ALTER, INSERT after it';
DROP TABLE IF EXISTS t_cast_added_projection;
CREATE TABLE t_cast_added_projection (x Nullable(UInt64), y UInt64) ENGINE = MergeTree ORDER BY y;
ALTER TABLE t_cast_added_projection ADD PROJECTION p (SELECT x, y ORDER BY CAST(x, 'UInt32')) SETTINGS cast_keep_nullable = 1;
INSERT INTO t_cast_added_projection VALUES (1, 10), (2, 20), (3, 30);
SELECT count() FROM t_cast_added_projection;
SELECT name, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_cast_added_projection' AND active GROUP BY name;
DROP TABLE t_cast_added_projection;

SELECT '-- Aggregate projection, an unrelated ALTER is not refused';
DROP TABLE IF EXISTS t_cast_aggregate_projection;
CREATE TABLE t_cast_aggregate_projection (x Nullable(UInt64), PROJECTION p (SELECT CAST(x, 'UInt32') AS k, count() GROUP BY k))
ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_cast_aggregate_projection COMMENT COLUMN x 'c' SETTINGS cast_keep_nullable = 1;
-- The INSERT and the SELECT pin the default: a projection is still calculated and read with the session's cast_keep_nullable (#116824).
INSERT INTO t_cast_aggregate_projection SETTINGS cast_keep_nullable = 0 VALUES (1), (2), (2);
SELECT CAST(x, 'UInt32') AS k, count() FROM t_cast_aggregate_projection GROUP BY k ORDER BY k SETTINGS cast_keep_nullable = 0;
SELECT name, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_cast_aggregate_projection' AND active GROUP BY name;
DROP TABLE t_cast_aggregate_projection;

SELECT '-- Projection sorting key, another type-affecting setting, INSERT after the ALTER';
DROP TABLE IF EXISTS t_date32_projection;
-- enable_extended_results_for_datetime_functions makes toStartOfMonth of a Date32 a Date32.
CREATE TABLE t_date32_projection (d Date32, y UInt64, PROJECTION p (SELECT d, y ORDER BY toStartOfMonth(d)))
ENGINE = MergeTree ORDER BY y;
ALTER TABLE t_date32_projection COMMENT COLUMN d 'c' SETTINGS enable_extended_results_for_datetime_functions = 1;
INSERT INTO t_date32_projection VALUES ('2020-01-15', 10), ('2020-02-15', 20), ('2020-03-15', 30);
SELECT count() FROM t_date32_projection;
SELECT name, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_date32_projection' AND active GROUP BY name;
DROP TABLE t_date32_projection;

SELECT '-- Projection sorting key with a folded constant, reload after the ALTER';
DROP TABLE IF EXISTS t_folded_constant_projection;
CREATE TABLE t_folded_constant_projection (y UInt64, PROJECTION p (SELECT y ORDER BY toStartOfMonth(toDate32('2020-01-15')) + y))
ENGINE = MergeTree ORDER BY y SETTINGS index_granularity = 1;
ALTER TABLE t_folded_constant_projection COMMENT COLUMN y 'c' SETTINGS enable_extended_results_for_datetime_functions = 1;
INSERT INTO t_folded_constant_projection VALUES (1), (2), (3);
SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_folded_constant_projection';
DETACH TABLE t_folded_constant_projection;
ATTACH TABLE t_folded_constant_projection;
SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_folded_constant_projection';
SELECT y FROM mergeTreeProjection(currentDatabase(), t_folded_constant_projection, p)
ORDER BY toStartOfMonth(toDate32('2020-01-15')) + y LIMIT 1 SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1;
SELECT name, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_folded_constant_projection' AND active GROUP BY name;
DROP TABLE t_folded_constant_projection;

SELECT '-- ReplicatedMergeTree projection, INSERT after a comment-only ALTER';
DROP TABLE IF EXISTS t_cast_projection_replicated;
CREATE TABLE t_cast_projection_replicated (x Nullable(UInt64), y UInt64, PROJECTION p (SELECT x, y ORDER BY CAST(x, 'UInt32')))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_cast_projection_replicated', 'r1') ORDER BY y;
ALTER TABLE t_cast_projection_replicated COMMENT COLUMN x 'c' SETTINGS cast_keep_nullable = 1;
INSERT INTO t_cast_projection_replicated VALUES (1, 10), (2, 20), (3, 30);
SELECT count() FROM t_cast_projection_replicated;
SELECT name, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_cast_projection_replicated' AND active GROUP BY name;
DROP TABLE t_cast_projection_replicated;
