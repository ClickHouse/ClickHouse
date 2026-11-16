-- `ORDER BY ... WITH FILL` gets a distributed plan from the Cascades optimizer. The fill runs on one
-- node above the sorted gather, so every query must give exactly the non-distributed result.

DROP TABLE IF EXISTS t_cascades_fill;

CREATE TABLE t_cascades_fill (a UInt64, g UInt64, b String) ENGINE = MergeTree ORDER BY a
    SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_cascades_fill;
INSERT INTO t_cascades_fill SELECT number * 4, number % 20 * 3, toString(number) FROM numbers(200);

SET enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
-- Without it a rejected plan runs locally and still gives the correct result.
SET distributed_plan_fallback_to_local_execution = 0;
-- The functional-test profile sets a non-zero limit, and then Cascades keeps every aggregation on one node.
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- A large table-size hint makes Cascades always read the table on several nodes.
SET param__internal_join_table_stat_hints = '{"t_cascades_fill": {"cardinality": 100000000, "avg_row_bytes": 24, "distinct_keys": {"g": 20}}}';

SELECT '-- STEP 2, whole stream' SETTINGS make_distributed_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_cascades_fill ORDER BY a WITH FILL STEP 2)
SETTINGS make_distributed_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_cascades_fill ORDER BY a WITH FILL STEP 2);
EXPLAIN SELECT a FROM t_cascades_fill ORDER BY a WITH FILL STEP 2
SETTINGS explain_query_plan_default = 'legacy';

SELECT '-- STEP 2, LIMIT' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_fill ORDER BY a WITH FILL STEP 2 LIMIT 6 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_fill ORDER BY a WITH FILL STEP 2 LIMIT 6;
-- The `LIMIT` must stay above `Filling`, because the fill adds rows.
EXPLAIN SELECT a FROM t_cascades_fill ORDER BY a WITH FILL STEP 2 LIMIT 6
SETTINGS explain_query_plan_default = 'legacy';

SELECT '-- FROM 2 TO 30 STEP 3' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_fill WHERE a < 20 ORDER BY a WITH FILL FROM 2 TO 30 STEP 3
SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_fill WHERE a < 20 ORDER BY a WITH FILL FROM 2 TO 30 STEP 3;

SELECT '-- DESC WITH FILL STALENESS' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_fill WHERE a > 780 ORDER BY a DESC WITH FILL STEP -2 STALENESS -6
SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_fill WHERE a > 780 ORDER BY a DESC WITH FILL STEP -2 STALENESS -6;

SELECT '-- INTERPOLATE' SETTINGS make_distributed_plan = 0;
SELECT a, b FROM t_cascades_fill ORDER BY a WITH FILL STEP 2 INTERPOLATE (b AS b) LIMIT 6
SETTINGS make_distributed_plan = 0;
SELECT a, b FROM t_cascades_fill ORDER BY a WITH FILL STEP 2 INTERPOLATE (b AS b) LIMIT 6;
EXPLAIN SELECT a, b FROM t_cascades_fill ORDER BY a WITH FILL STEP 2 INTERPOLATE (b AS b)
SETTINGS explain_query_plan_default = 'legacy';

SELECT '-- DateTime WITH FILL STEP INTERVAL' SETTINGS make_distributed_plan = 0;
SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + INTERVAL a SECOND AS ts
FROM t_cascades_fill WHERE a < 12
ORDER BY ts WITH FILL STEP INTERVAL 1 SECOND
SETTINGS make_distributed_plan = 0;
SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + INTERVAL a SECOND AS ts
FROM t_cascades_fill WHERE a < 12
ORDER BY ts WITH FILL STEP INTERVAL 1 SECOND;

SELECT '-- DateTime64 WITH FILL FROM/TO STEP INTERVAL 500 MILLISECOND' SETTINGS make_distributed_plan = 0;
SELECT count() FROM (
    SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') + INTERVAL a SECOND AS ts
    FROM t_cascades_fill WHERE a < 12
    ORDER BY ts WITH FILL
        FROM toDateTime64('2019-12-31 23:59:58.000', 3, 'UTC')
        TO toDateTime64('2020-01-01 00:00:12.000', 3, 'UTC')
        STEP INTERVAL 500 MILLISECOND)
SETTINGS make_distributed_plan = 0;
SELECT count() FROM (
    SELECT toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') + INTERVAL a SECOND AS ts
    FROM t_cascades_fill WHERE a < 12
    ORDER BY ts WITH FILL
        FROM toDateTime64('2019-12-31 23:59:58.000', 3, 'UTC')
        TO toDateTime64('2020-01-01 00:00:12.000', 3, 'UTC')
        STEP INTERVAL 500 MILLISECOND);

-- The aggregation is distributed and `Filling` runs above it.
SELECT '-- GROUP BY, WITH FILL FROM 0 TO 64' SETTINGS make_distributed_plan = 0;
SELECT g, count() AS c FROM t_cascades_fill GROUP BY g ORDER BY g WITH FILL FROM 0 TO 64
SETTINGS make_distributed_plan = 0;
SELECT g, count() AS c FROM t_cascades_fill GROUP BY g ORDER BY g WITH FILL FROM 0 TO 64;
EXPLAIN SELECT g, count() AS c FROM t_cascades_fill GROUP BY g ORDER BY g WITH FILL FROM 0 TO 64
SETTINGS explain_query_plan_default = 'legacy';

-- Fills `a` inside each value of the sorting prefix `g`.
SELECT '-- ORDER BY g, a WITH FILL by sorting prefix' SETTINGS make_distributed_plan = 0;
SELECT count(), sum(g), sum(a) FROM (
    SELECT g, a FROM t_cascades_fill WHERE a < 200 ORDER BY g, a WITH FILL STEP 40)
SETTINGS make_distributed_plan = 0, use_with_fill_by_sorting_prefix = 1;
SELECT count(), sum(g), sum(a) FROM (
    SELECT g, a FROM t_cascades_fill WHERE a < 200 ORDER BY g, a WITH FILL STEP 40)
SETTINGS use_with_fill_by_sorting_prefix = 1;

DROP TABLE t_cascades_fill;
