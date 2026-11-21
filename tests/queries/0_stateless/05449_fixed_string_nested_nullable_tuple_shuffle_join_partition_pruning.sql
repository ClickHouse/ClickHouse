-- The comparison of `FixedString` with `String` is zero-padded, also when the `FixedString` is inside a composite type,
-- while the conversion of `FixedString` to `String` keeps the trailing zero bytes.

-- A `Nullable` tuple nested in a tuple, in the order of arrays: compared as the byte-identical all-`String` form.
SELECT
    [tuple(toNullable(tuple(toFixedString('a', 2), nan)))] > [tuple(toNullable(tuple('a', 1.)))],
    [tuple(toNullable(tuple('a', nan)))] > [tuple(toNullable(tuple('a', 1.)))];
SELECT
    [tuple(toNullable(tuple(toFixedString('a', 2), 1.)))] > [tuple(toNullable(tuple('a', CAST(NULL AS Nullable(Float64)))))],
    [tuple(toNullable(tuple('a', 1.)))] > [tuple(toNullable(tuple('a', CAST(NULL AS Nullable(Float64)))))];
SELECT
    [tuple(CAST(NULL AS Nullable(Tuple(FixedString(2)))))] < [tuple(toNullable(tuple('a')))],
    [tuple(CAST(NULL AS Nullable(Tuple(String))))] < [tuple(toNullable(tuple('a')))],
    [tuple(CAST(NULL AS Nullable(Tuple(FixedString(2)))))] = [tuple(CAST(NULL AS Nullable(Tuple(String))))];

-- Null-safe comparison of `Nullable` tuples.
SELECT
    isNotDistinctFrom(toNullable(tuple(toFixedString('a', 2))), toNullable(tuple('a'))),
    isNotDistinctFrom(tuple(toFixedString('a', 2)), tuple('a')),
    isDistinctFrom(toNullable(tuple(toFixedString('a', 2))), toNullable(tuple('a'))),
    isNotDistinctFrom(toNullable(tuple(toFixedString('a', 2))), CAST(NULL AS Nullable(Tuple(String)))),
    isDistinctFrom(CAST(NULL AS Nullable(Tuple(FixedString(2)))), CAST(NULL AS Nullable(Tuple(String))));

-- Partition pruning by an `Array(FixedString)` key gives the same result as without it.
DROP TABLE IF EXISTS t_array_fs;
CREATE TABLE t_array_fs (a Array(FixedString(2))) ENGINE = MergeTree PARTITION BY a ORDER BY tuple();
INSERT INTO t_array_fs VALUES (['a']), (['b']);
SELECT count() FROM t_array_fs WHERE a = ['a'] SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_array_fs WHERE a = ['a'];
SELECT count() FROM t_array_fs WHERE a < ['b'] SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_array_fs WHERE a < ['b'];
DROP TABLE t_array_fs;

-- The shuffle of a distributed `JOIN` hashes the `FixedString` and `String` keys as the `JOIN` compares them.
DROP TABLE IF EXISTS probe_fs;
DROP TABLE IF EXISTS build_s;
CREATE TABLE probe_fs (k FixedString(8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE build_s (k String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO probe_fs SELECT toString(number) FROM numbers(10000);
INSERT INTO build_s SELECT toString(number * 10) FROM numbers(1000);
SELECT count() FROM probe_fs AS p JOIN build_s AS b ON p.k = b.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0, distributed_plan_join_runtime_filters = 0;
DROP TABLE probe_fs;
DROP TABLE build_s;

-- The conversion of the `USING` key keeps removing the trailing zero bytes after the plan is serialized.
DROP TABLE IF EXISTS f_using;
DROP TABLE IF EXISTS s_using;
CREATE TABLE f_using (k FixedString(2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE s_using (k String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f_using VALUES ('a'), ('b');
INSERT INTO s_using VALUES ('a'), ('b');
SELECT count(), sum(length(k)) FROM f_using JOIN s_using USING k;
SELECT count(), sum(length(k)) FROM f_using JOIN s_using USING k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_join_runtime_filters = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0;
DROP TABLE f_using;
DROP TABLE s_using;
