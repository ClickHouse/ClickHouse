-- an optimization must not take one lambda for another lambda that has the same captured columns

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;

-- an aggregate projection
CREATE TABLE t_projection
(
    a UInt32,
    arr Array(UInt8),
    PROJECTION p_capture (SELECT sum(a) GROUP BY arrayMap(x -> x + a, arr)),
    PROJECTION p_constant (SELECT sum(a) GROUP BY arrayMap(x -> x + 1, arr)),
    PROJECTION p_nested (SELECT sum(a) GROUP BY arrayMap(x -> arraySum(y -> y + x + a, arr), arr))
)
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_projection VALUES (1, [1]), (2, [2, 3]), (3, []), (4, [1]);

SELECT arrayMap(x -> x * a, arr) AS k, sum(a) FROM t_projection GROUP BY k ORDER BY k;
SELECT arrayMap(x -> x * 1, arr) AS k, sum(a) FROM t_projection GROUP BY k ORDER BY k;
SELECT arrayMap(x -> arraySum(y -> y * x + a, arr), arr) AS k, sum(a) FROM t_projection GROUP BY k ORDER BY k;

-- the partition key: the implicit projection and the aggregation of each partition independently
CREATE TABLE t_partition (a UInt32, arr Array(UInt8)) ENGINE = MergeTree ORDER BY a PARTITION BY arraySum(x -> x + a, arr);
INSERT INTO t_partition VALUES (1, [0]), (2, [0]), (3, [0]), (1, [1]), (2, [1]);

SELECT arraySum(x -> x * a, arr) AS k, count() FROM t_partition GROUP BY k ORDER BY k;
SELECT arraySum(x -> x * a, arr) AS k, count() FROM t_partition GROUP BY k ORDER BY k
SETTINGS optimize_use_implicit_projections = 0, allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1, max_threads = 4;

-- the sorting key: the read in order and the aggregation in order
CREATE TABLE t_order (a UInt32, arr Array(UInt8)) ENGINE = MergeTree ORDER BY arraySum(x -> x + a, arr);
INSERT INTO t_order VALUES (3, [1]), (2, [3]), (5, [1]), (1, [6]), (10, [0]);

SELECT a, arraySum(x -> x * a, arr) AS k FROM t_order ORDER BY k, a SETTINGS optimize_read_in_order = 1;
SELECT arraySum(x -> x * a, arr) AS k, count() FROM t_order GROUP BY k ORDER BY k SETTINGS optimize_aggregation_in_order = 1;
