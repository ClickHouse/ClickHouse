-- A partial aggregation of a distributed plan emits a state per group on every node that has rows of the group: with 4
-- nodes and 10 groups, between 10 and 40 states, depending on where the rows of a group are placed, which the optimizer
-- does not know. So the partial aggregation and the exchange above it report an unknown estimate (their search value is
-- the 10 groups), while the merge emits the 10 groups. The table has 1,000,000 rows, enough for the optimizer to split
-- the aggregation.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET max_rows_to_group_by = 0;
SET explain_query_plan_default = 'legacy';
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_partial_states;
CREATE TABLE t_partial_states (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = 'uniq';
INSERT INTO t_partial_states SELECT number % 10, number FROM numbers(1000000);

SELECT '-- the partial aggregation and the exchange above it: unknown; the merge: 10 groups';
SELECT extract(trimLeft(explain), '^[A-Za-z]+') || ' ' || extract(explain, 'rows: [^,)]+') FROM (EXPLAIN estimates = 1 SELECT k, count() FROM t_partial_states GROUP BY k)
WHERE explain LIKE '%ggregat%' OR explain LIKE '%Exchange%';

SELECT '-- keyless: unknown as well, the merge emits 1 row';
SELECT extract(trimLeft(explain), '^[A-Za-z]+') || ' ' || extract(explain, 'rows: [^,)]+') FROM (EXPLAIN estimates = 1 SELECT sum(v) FROM t_partial_states)
WHERE explain LIKE '%ggregat%' OR explain LIKE '%Exchange%';

DROP TABLE t_partial_states;
