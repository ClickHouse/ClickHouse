-- The shuffle exchange of the Cascades optimizer can only cast a join key before hashing, while a
-- `FixedString` vs `String` key pair also needs the trailing zero bytes of the `String` side trimmed
-- (`toFixedString('a', 3) = 'a\0\0'` holds). Such a pair must not partition the inputs: the fixed
-- string would hash as `'a'` and its match as `'a\0\0'`, and the rows would land on different nodes.

SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
-- The test profile installed in CI sets a non-zero max_rows_to_group_by, which keeps aggregations local.
SET max_rows_to_group_by = 0;
SET cascades_aggregation_pushdown = 0;
SET materialize_statistics_on_insert = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Pin the statistics and the cost model so that the shuffle join is the cheapest alternative wherever it is allowed.
SET param__internal_cascades_cost_config = '{"work_weight":1,"network_weight":0.01,"sequential_weight":100000}';
SET param__internal_join_table_stat_hints = '{
    "t_fixed": { "cardinality": 100000000, "distinct_keys": { "k": 100000000 } },
    "t_str":   { "cardinality": 100000000, "distinct_keys": { "k": 100000000 } },
    "t_str2":  { "cardinality": 100000000, "distinct_keys": { "k": 100000000 } }
}';

DROP TABLE IF EXISTS t_fixed;
DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_str2;

CREATE TABLE t_fixed (k FixedString(8), v UInt64) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_str (k String, v UInt64) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_str2 (k String, v UInt64) ENGINE = MergeTree() ORDER BY k;

-- The keys of `t_str` match those of `t_fixed` only up to trailing zero bytes.
INSERT INTO t_fixed SELECT toFixedString(toString(number), 8), number FROM numbers(100000);
INSERT INTO t_str SELECT concat(toString(number), '\0\0'), number * 2 FROM numbers(100000);
INSERT INTO t_str2 SELECT concat(toString(number), '\0\0'), number * 2 FROM numbers(100000);

SELECT '-- 1. FixedString vs String keys: all 100000 pairs must match';
SELECT count(), sum(l.v + r.v) FROM t_fixed AS l JOIN t_str AS r ON l.k = r.k
SETTINGS distributed_plan_fallback_to_local_execution = 0;

SELECT '-- 2. Baseline without Cascades';
SELECT count(), sum(l.v + r.v) FROM t_fixed AS l JOIN t_str AS r ON l.k = r.k
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- 3. Such a key pair does not shuffle the join (shuffle hash joins, hash joins)';
SELECT countIf(explain LIKE '%Shuffle HashJoin%'), countIf(explain LIKE '%HashJoin%')
FROM (
    EXPLAIN
    SELECT count(), sum(l.v + r.v) FROM t_fixed AS l JOIN t_str AS r ON l.k = r.k
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1
)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- 4. Control: the same shape with String keys on both sides is shuffled (shuffle hash joins, hash joins)';
SELECT countIf(explain LIKE '%Shuffle HashJoin%'), countIf(explain LIKE '%HashJoin%')
FROM (
    EXPLAIN
    SELECT count(), sum(l.v + r.v) FROM t_str2 AS l JOIN t_str AS r ON l.k = r.k
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1
)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT count(), sum(l.v + r.v) FROM t_str2 AS l JOIN t_str AS r ON l.k = r.k
SETTINGS distributed_plan_fallback_to_local_execution = 0;

DROP TABLE t_fixed;
DROP TABLE t_str;
DROP TABLE t_str2;
