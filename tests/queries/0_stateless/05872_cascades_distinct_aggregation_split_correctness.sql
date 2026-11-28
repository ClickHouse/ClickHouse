-- The count of the rows of an aggregation by `x` (see `cascades_distinct_aggregation_split`) must be the count of
-- `uniqExact(x)` for every type that the rule accepts. For floats the two could differ, so the rule must not
-- apply to them.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_distinct_values;
DROP TABLE IF EXISTS t_distinct_values_by_float;
DROP TABLE IF EXISTS t_distinct_values_by_int;
DROP TABLE IF EXISTS t_distinct_values_empty;
DROP TABLE IF EXISTS t_distinct_values_measured;
DROP TABLE IF EXISTS t_distinct_values_dim;

CREATE TABLE t_distinct_values
(
    k UInt32,
    u64 UInt64,
    i8 Int8,
    i128 Int128,
    u256 UInt256,
    d Date,
    d32 Date32,
    dt DateTime('UTC'),
    e8 Enum8('a' = -1, 'b' = 0, 'c' = 1),
    e16 Enum16('x' = -300, 'y' = 300),
    uuid UUID,
    ip4 IPv4,
    n Nullable(Int32),
    lc LowCardinality(UInt64),
    lcn LowCardinality(Nullable(UInt32)),
    saf SimpleAggregateFunction(max, UInt64),
    only_null Nullable(UInt8),
    f64 Float64,
    f32 Float32,
    nf Nullable(Float64)
)
ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', index_granularity = 128;
-- Sorted by the counted column, so the nodes can aggregate in the order of the key.
CREATE TABLE t_distinct_values_by_float (f Float64, k UInt32) ENGINE = MergeTree ORDER BY (f, k)
  SETTINGS auto_statistics_types = '', index_granularity = 32;
CREATE TABLE t_distinct_values_by_int (i Int64, k UInt32) ENGINE = MergeTree ORDER BY (i, k)
  SETTINGS auto_statistics_types = '', index_granularity = 32;
CREATE TABLE t_distinct_values_empty (k UInt32, u64 UInt64, n Nullable(Int32)) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
-- These two have the distinct counts that the inserts measure, and no hint.
CREATE TABLE t_distinct_values_measured (x UInt64) ENGINE = MergeTree ORDER BY tuple()
  SETTINGS auto_statistics_types = 'basic, uniq_v2', index_granularity = 256;
CREATE TABLE t_distinct_values_dim (k UInt64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = 'basic, uniq_v2', index_granularity = 32;
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_distinct_values;
SYSTEM STOP MERGES t_distinct_values_measured;
SYSTEM STOP MERGES t_distinct_values_dim;
SYSTEM STOP MERGES t_distinct_values_by_float;
SYSTEM STOP MERGES t_distinct_values_by_int;

-- Three parts with the same values, so the same value is on several nodes.
-- The floats come from their bits: two NaN values with different bits, the two zeros and the infinities
-- (`if` and `multiIf` of two NaN constants would give one of them).
INSERT INTO t_distinct_values SELECT
    number,
    number % 211,
    number % 100 - 50,
    toInt128(number % 37) * toInt128('100000000000000000000000') - 5,
    toUInt256(number % 41) * toUInt256('1000000000000000000000000000000000000000'),
    toDate('2026-01-01') + number % 59,
    toDate32('1950-01-01') + number % 61,
    toDateTime('2026-01-01 00:00:00', 'UTC') + number % 67,
    ['a', 'b', 'c'][number % 3 + 1],
    ['x', 'y'][number % 2 + 1],
    toUUID(concat('00000000-0000-0000-0000-0000000000', leftPad(toString(number % 71), 2, '0'))),
    toIPv4(concat('10.0.0.', toString(number % 73))),
    if(number % 5 = 0, NULL, number % 79),
    number % 83,
    if(number % 4 = 0, NULL, number % 89),
    number % 97,
    NULL,
    reinterpretAsFloat64(multiIf(number % 11 = 0, 0x7FF8000000000001, number % 11 = 1, 0x7FF8000000000002,
            number % 11 = 2, 0x0000000000000000, number % 11 = 3, 0x8000000000000000, number % 11 = 4, 0x7FF0000000000000,
            number % 11 = 5, 0xFFF0000000000000, reinterpretAsUInt64(toFloat64(number % 13) / 4))),
    reinterpretAsFloat32(multiIf(number % 7 = 0, toUInt32(0x7FC00001), number % 7 = 1, toUInt32(0x7FC00002),
            number % 7 = 2, toUInt32(0), number % 7 = 3, toUInt32(0x80000000), reinterpretAsUInt32(toFloat32(number % 9)))),
    if(number % 6 = 0, NULL, reinterpretAsFloat64(multiIf(number % 6 = 1, 0x7FF8000000000001, number % 6 = 2, 0x7FF8000000000002,
            number % 6 = 3, 0x8000000000000000, number % 6 = 4, 0x0000000000000000, reinterpretAsUInt64(toFloat64(number % 17)))))
FROM numbers(3000);
INSERT INTO t_distinct_values SELECT * REPLACE (k + 3000 AS k) FROM t_distinct_values ORDER BY u64, k;
INSERT INTO t_distinct_values SELECT * REPLACE (k + 6000 AS k) FROM t_distinct_values WHERE k < 3000 ORDER BY i8, k;

INSERT INTO t_distinct_values_by_float SELECT f64, k FROM t_distinct_values WHERE k < 3000;
INSERT INTO t_distinct_values_by_float SELECT f64, k FROM t_distinct_values WHERE k >= 6000;
INSERT INTO t_distinct_values_by_int SELECT i8, k FROM t_distinct_values WHERE k < 3000;
INSERT INTO t_distinct_values_by_int SELECT i8, k FROM t_distinct_values WHERE k >= 6000;
INSERT INTO t_distinct_values_measured SETTINGS materialize_statistics_on_insert = 1 SELECT number % 10007 FROM numbers(50000);
INSERT INTO t_distinct_values_dim SETTINGS materialize_statistics_on_insert = 1 SELECT number FROM numbers(129);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET count_distinct_implementation = 'uniqExact';
-- Without partial aggregation before the shuffle the split has one form for every type: a shuffle of the rows by
-- the counted column. With it, the two forms of the split have almost the same cost for keys of 8 to 32 bytes.
SET distributed_plan_partial_aggregation_before_shuffle = 0;
SET distributed_plan_force_shuffle_aggregation = 0;
SET cascades_aggregation_pushdown = 0;
SET optimize_aggregation_in_order = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- The tables stand in for large ones with many distinct values in every column, so the costs select the split
-- for every type that the rule accepts.
SET param__internal_join_table_stat_hints = '{"t_distinct_values": {"cardinality": 1000000000, "column_bytes": {"u64": 8, "i8": 1, "i128": 16, "u256": 32, "d": 2, "d32": 4, "dt": 4, "e8": 1, "e16": 2, "uuid": 16, "ip4": 4, "n": 5, "lc": 8, "lcn": 5, "saf": 8, "only_null": 2, "f64": 8, "f32": 4, "nf": 9}, "distinct_keys": {"u64": 200000000, "i8": 200000000, "i128": 200000000, "u256": 200000000, "d": 200000000, "d32": 200000000, "dt": 200000000, "e8": 200000000, "e16": 200000000, "uuid": 200000000, "ip4": 200000000, "n": 200000000, "lc": 200000000, "lcn": 200000000, "saf": 200000000, "only_null": 200000000, "f64": 200000000, "f32": 200000000, "nf": 200000000}}, "t_distinct_values_by_float": {"cardinality": 1000000000, "column_bytes": {"f": 8}, "distinct_keys": {"f": 200000000}}, "t_distinct_values_by_int": {"cardinality": 1000000000, "column_bytes": {"i": 8}, "distinct_keys": {"i": 200000000}}, "t_distinct_values_empty": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"u64": 200000000, "n": 200000000}}}';

SET cascades_distinct_aggregation_split = 1;

-- The steps of each plan in one line, from the top one to the read. The split has a `ShuffleExchange`, and the
-- plans of the floats must not have one. A query over the lines of `EXPLAIN` cannot be distributed itself, so
-- it may run locally.
SET distributed_plan_fallback_to_local_execution = 1;
SELECT '-- column, the plan with the setting on';
SELECT 'u64', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT u64) FROM t_distinct_values);
SELECT 'i8', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT i8) FROM t_distinct_values);
SELECT 'i128', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT i128) FROM t_distinct_values);
SELECT 'u256', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT u256) FROM t_distinct_values);
SELECT 'd', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT d) FROM t_distinct_values);
SELECT 'd32', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT d32) FROM t_distinct_values);
SELECT 'dt', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT dt) FROM t_distinct_values);
SELECT 'e8', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT e8) FROM t_distinct_values);
SELECT 'e16', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT e16) FROM t_distinct_values);
SELECT 'uuid', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT uuid) FROM t_distinct_values);
SELECT 'ip4', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT ip4) FROM t_distinct_values);
SELECT 'n', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT n) FROM t_distinct_values);
SELECT 'lc', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT lc) FROM t_distinct_values);
SELECT 'lcn', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT lcn) FROM t_distinct_values);
SELECT 'saf', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT saf) FROM t_distinct_values);
SELECT 'only_null', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT only_null) FROM t_distinct_values);
SELECT 'f64', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT f64) FROM t_distinct_values);
SELECT 'f32', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT f32) FROM t_distinct_values);
SELECT 'nf', arrayStringConcat(groupArray(trimLeft(explain)), ' <- ') FROM (EXPLAIN description = 0 SELECT count(DISTINCT nf) FROM t_distinct_values);
SET distributed_plan_fallback_to_local_execution = 0;

SELECT '-- column, the count with the setting on';
SELECT 'u64', count(DISTINCT u64) FROM t_distinct_values;
SELECT 'i8', count(DISTINCT i8) FROM t_distinct_values;
SELECT 'i128', count(DISTINCT i128) FROM t_distinct_values;
SELECT 'u256', count(DISTINCT u256) FROM t_distinct_values;
SELECT 'd', count(DISTINCT d) FROM t_distinct_values;
SELECT 'd32', count(DISTINCT d32) FROM t_distinct_values;
SELECT 'dt', count(DISTINCT dt) FROM t_distinct_values;
SELECT 'e8', count(DISTINCT e8) FROM t_distinct_values;
SELECT 'e16', count(DISTINCT e16) FROM t_distinct_values;
SELECT 'uuid', count(DISTINCT uuid) FROM t_distinct_values;
SELECT 'ip4', count(DISTINCT ip4) FROM t_distinct_values;
SELECT 'n', count(DISTINCT n) FROM t_distinct_values;
SELECT 'lc', count(DISTINCT lc) FROM t_distinct_values;
SELECT 'lcn', count(DISTINCT lcn) FROM t_distinct_values;
SELECT 'saf', count(DISTINCT saf) FROM t_distinct_values;
SELECT 'only_null', count(DISTINCT only_null) FROM t_distinct_values;
SELECT 'f64', count(DISTINCT f64) FROM t_distinct_values;
SELECT 'f32', count(DISTINCT f32) FROM t_distinct_values;
SELECT 'nf', count(DISTINCT nf) FROM t_distinct_values;

SELECT '-- column, the count with the setting off';
SET cascades_distinct_aggregation_split = 0;
SELECT 'u64', count(DISTINCT u64) FROM t_distinct_values;
SELECT 'i8', count(DISTINCT i8) FROM t_distinct_values;
SELECT 'i128', count(DISTINCT i128) FROM t_distinct_values;
SELECT 'u256', count(DISTINCT u256) FROM t_distinct_values;
SELECT 'd', count(DISTINCT d) FROM t_distinct_values;
SELECT 'd32', count(DISTINCT d32) FROM t_distinct_values;
SELECT 'dt', count(DISTINCT dt) FROM t_distinct_values;
SELECT 'e8', count(DISTINCT e8) FROM t_distinct_values;
SELECT 'e16', count(DISTINCT e16) FROM t_distinct_values;
SELECT 'uuid', count(DISTINCT uuid) FROM t_distinct_values;
SELECT 'ip4', count(DISTINCT ip4) FROM t_distinct_values;
SELECT 'n', count(DISTINCT n) FROM t_distinct_values;
SELECT 'lc', count(DISTINCT lc) FROM t_distinct_values;
SELECT 'lcn', count(DISTINCT lcn) FROM t_distinct_values;
SELECT 'saf', count(DISTINCT saf) FROM t_distinct_values;
SELECT 'only_null', count(DISTINCT only_null) FROM t_distinct_values;
SELECT 'f64', count(DISTINCT f64) FROM t_distinct_values;
SELECT 'f32', count(DISTINCT f32) FROM t_distinct_values;
SELECT 'nf', count(DISTINCT nf) FROM t_distinct_values;

-- An aggregation in the order of its key compares the keys and does not hash them. For integers the split
-- gives the same count. For floats it would count `0.` and `-0.` as one value.
SET optimize_aggregation_in_order = 1;
SET cascades_distinct_aggregation_split = 1;
SELECT '-- a table sorted by an integer column, aggregation in order: the plan, the count with the setting on and off';
EXPLAIN SELECT count(DISTINCT i) FROM t_distinct_values_by_int;
SELECT count(DISTINCT i) FROM t_distinct_values_by_int;
SELECT count(DISTINCT i) FROM t_distinct_values_by_int SETTINGS cascades_distinct_aggregation_split = 0;
SELECT '-- the same for a float column: no split';
EXPLAIN SELECT count(DISTINCT f) FROM t_distinct_values_by_float;
SELECT count(DISTINCT f) FROM t_distinct_values_by_float;
SELECT count(DISTINCT f) FROM t_distinct_values_by_float SETTINGS cascades_distinct_aggregation_split = 0;
SET optimize_aggregation_in_order = 0;

-- The aggregation by the column has one row, NULL, and `count(only_null)` does not count it.
SELECT '-- only NULL values: one row with zero also with empty_result_for_aggregation_by_empty_set';
SELECT count(DISTINCT only_null) FROM t_distinct_values SETTINGS empty_result_for_aggregation_by_empty_set = 1;

-- The rule does not see an empty table: the plan reads nothing before the distributed optimizer runs.
SELECT '-- an empty table: one row with zero, or no rows with empty_result_for_aggregation_by_empty_set';
SELECT count(DISTINCT u64) FROM t_distinct_values_empty;
SELECT count(DISTINCT n) FROM t_distinct_values_empty;
SELECT count(DISTINCT n) FROM t_distinct_values_empty SETTINGS empty_result_for_aggregation_by_empty_set = 1;

-- No row passes this filter, but the plan cannot know it, so the two aggregations of the split get no rows.
SELECT '-- a filter that no row passes: the plan, one row with zero, no rows with empty_result_for_aggregation_by_empty_set, the setting off';
EXPLAIN SELECT count(DISTINCT u64) FROM t_distinct_values WHERE u64 % 3 = 5 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(DISTINCT u64) FROM t_distinct_values WHERE u64 % 3 = 5;
SELECT count(DISTINCT u64) FROM t_distinct_values WHERE u64 % 3 = 5 SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT count(DISTINCT u64) FROM t_distinct_values WHERE u64 % 3 = 5 SETTINGS cascades_distinct_aggregation_split = 0;

-- A `ROLLUP` over an aggregation without keys has one row. If its statistics had the distinct values of the
-- state as rows, the join would build its hash table from the 129 rows of the other table, over the limit.
SELECT '-- a join with a ROLLUP of one row, with a limit of 16 rows for the hash table';
SELECT count() FROM t_distinct_values_dim AS dim ALL INNER JOIN
    (SELECT uniqExact(x) AS v FROM t_distinct_values_measured GROUP BY ALL WITH ROLLUP) AS a ON dim.k = a.v % 129
SETTINGS max_rows_in_join = 16, join_overflow_mode = 'throw', join_algorithm = 'hash', use_statistics = 1, min_marks_per_part_to_load_statistics = 0;

SELECT '-- the name and the type of the result column are those of uniqExact';
SELECT count(DISTINCT n) FROM t_distinct_values FORMAT TSVWithNamesAndTypes;
SELECT uniqExact(u64) AS u FROM t_distinct_values FORMAT TSVWithNamesAndTypes;

DROP TABLE t_distinct_values;
DROP TABLE t_distinct_values_measured;
DROP TABLE t_distinct_values_dim;
DROP TABLE t_distinct_values_by_float;
DROP TABLE t_distinct_values_by_int;
DROP TABLE t_distinct_values_empty;
