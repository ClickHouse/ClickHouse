-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database, no-parallel-replicas: the EXPLAIN checks read the partition step of the local plan.

-- A NULL in a set or a constant stands for every NULL row, while a key over `assumeNotNull(k)` gives each such
-- row the value stored under its own NULL: `nullIf(2, 2)` stores 2, `if(c, NULL, number)` stores `number`.
-- Every table holds the same rows; `t_flat` cannot prune by `k`, so its count is the truth.

DROP TABLE IF EXISTS t_flat;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_idx;
DROP TABLE IF EXISTS t_ifnull;
DROP VIEW IF EXISTS v_rows;

-- Each table is filled from the view, not from another table: a part with sparse serialization drops the
-- values stored under NULLs.
CREATE VIEW v_rows AS
    SELECT number AS n, nullIf(toUInt64(2), toUInt64(2)) AS k FROM numbers(10)
    UNION ALL SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(10);

CREATE TABLE t_flat (n UInt64, k Nullable(UInt64)) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t_part (n UInt64, k Nullable(UInt64)) ENGINE = MergeTree PARTITION BY assumeNotNull(k) ORDER BY n;
CREATE TABLE t_pk (n UInt64, k Nullable(UInt64)) ENGINE = MergeTree ORDER BY assumeNotNull(k) SETTINGS index_granularity = 1;
CREATE TABLE t_idx (n UInt64, k Nullable(UInt64), INDEX i assumeNotNull(k) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 1;
-- `ifNull` maps every NULL to the same value, so its partition pruning is sound and must stay.
CREATE TABLE t_ifnull (n UInt64, k Nullable(UInt64)) ENGINE = MergeTree PARTITION BY ifNull(k, 0) ORDER BY n;

INSERT INTO t_flat SELECT * FROM v_rows;
INSERT INTO t_part SELECT * FROM v_rows;
INSERT INTO t_pk SELECT * FROM v_rows;
INSERT INTO t_idx SELECT * FROM v_rows;
INSERT INTO t_ifnull SELECT * FROM v_rows;

-- { echoOn }
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE has([NULL, 1], k) GROUP BY _table ORDER BY _table;
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE k IN (NULL) GROUP BY _table ORDER BY _table SETTINGS transform_null_in = 1;
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE k IN (NULL, 1) GROUP BY _table ORDER BY _table SETTINGS transform_null_in = 1;
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE (n, k) IN ((1, NULL), (2, NULL)) GROUP BY _table ORDER BY _table SETTINGS transform_null_in = 1;
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE k IN (SELECT arrayJoin([NULL, 1::Nullable(UInt64)])) GROUP BY _table ORDER BY _table SETTINGS transform_null_in = 1;

-- Already answered correctly, and must stay so.
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE k IS NULL GROUP BY _table ORDER BY _table;
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE isNotDistinctFrom(k, NULL) GROUP BY _table ORDER BY _table;
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE NOT has([NULL], k) GROUP BY _table ORDER BY _table;
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE k NOT IN (NULL, 2) GROUP BY _table ORDER BY _table SETTINGS transform_null_in = 1;
SELECT _table, count() FROM merge('^t_(flat|part|pk|idx|ifnull)$') WHERE k IN (1, 2) GROUP BY _table ORDER BY _table;

-- A set without a NULL still prunes through `assumeNotNull`, and a NULL still prunes through `ifNull`.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT n FROM t_part WHERE k IN (1, 2)) WHERE explain ILIKE '%Condition: (assumeNotNull(k) in 2-element set)%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT n FROM t_ifnull WHERE k IN (NULL, 1) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%Condition: (ifNull(k, 0) in 2-element set)%';
-- { echoOff }

-- Lazy FINAL builds its key set from the rows it read first, so it reached the same pruning with a NULL in it.
DROP TABLE IF EXISTS t_final;
CREATE TABLE t_final (n UInt64, k Nullable(UInt64), v UInt8, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY assumeNotNull(k) ORDER BY (n, k)
SETTINGS allow_nullable_key = 1, index_granularity = 100, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_final;
INSERT INTO t_final SELECT number, nullIf(toUInt64(1), toUInt64(1)), number = 0, 1 FROM numbers(1000);
INSERT INTO t_final SELECT number, nullIf(toUInt64(2), toUInt64(2)), number = 0, 2 FROM numbers(1000);

-- { echoOn }
SELECT arraySort(groupArray(ver)) FROM t_final FINAL WHERE v = 1
SETTINGS query_plan_optimize_lazy_final = 1, do_not_merge_across_partitions_select_final = 0,
         enable_automatic_decision_for_merging_across_partitions_for_final = 0, max_threads = 1;
-- { echoOff }

-- A NULL nested in a tuple or an array constant takes the same route.
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_array;
CREATE TABLE t_tuple (x Tuple(Nullable(UInt64), UInt8)) ENGINE = MergeTree ORDER BY assumeNotNull(x.1) SETTINGS index_granularity = 2;
CREATE TABLE t_array (a Array(Nullable(UInt64))) ENGINE = MergeTree ORDER BY assumeNotNull(a[1]) SETTINGS index_granularity = 2;
INSERT INTO t_tuple SELECT (nullIf(toUInt64(2), toUInt64(2)), toUInt8(1)) FROM numbers(10);
INSERT INTO t_tuple SELECT (toUInt64(number), toUInt8(number % 2)) FROM numbers(30);
INSERT INTO t_array SELECT [nullIf(toUInt64(2), toUInt64(2)), toUInt64(1)] FROM numbers(10);
INSERT INTO t_array SELECT [toUInt64(number), toUInt64(number)] FROM numbers(30);

-- { echoOn }
SELECT count() FROM t_tuple WHERE x <=> (NULL::Nullable(UInt64), 1::UInt8);
SELECT count() FROM t_tuple WHERE x <=> (NULL, 1);
SELECT count() FROM t_tuple WHERE x IN ((NULL::Nullable(UInt64), 1::UInt8)) SETTINGS transform_null_in = 1;
SELECT count() FROM t_tuple WHERE has([(NULL::Nullable(UInt64), 1::UInt8)], x);
SELECT count() FROM t_array WHERE a = [NULL, 1];
SELECT count() FROM t_array WHERE has([[NULL, 1]], a);
-- { echoOff }

DROP TABLE t_flat;
DROP TABLE t_part;
DROP TABLE t_pk;
DROP TABLE t_idx;
DROP TABLE t_ifnull;
DROP TABLE t_final;
DROP TABLE t_tuple;
DROP TABLE t_array;
DROP VIEW v_rows;
