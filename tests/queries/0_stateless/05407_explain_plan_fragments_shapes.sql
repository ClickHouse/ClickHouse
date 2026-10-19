-- `EXPLAIN PLAN fragments = 1`: the shapes of the fragment graph that `05339_explain_plan_fragments` does not cover.
-- The queries are only explained, never executed.

DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS big;

CREATE TABLE small(sid UInt64, s Array(Int64)) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE big(bid UInt64, b Array(Int64)) ENGINE = MergeTree ORDER BY bid;

INSERT INTO small SELECT number, [number] FROM numbers(0, 1000);
INSERT INTO big SELECT number, [number] FROM numbers(0, 100000);

SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 0;
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;
-- The test harness randomizes these, and aggregation in order changes the plan.
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;

SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 0,
    use_statistics = 0,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';

-- The aggregated side of the join is one fragment longer than the other side: the level of the join fragment is
-- determined by the longest branch, so `F2` is a leaf next to `F0`.
SELECT '-- uneven branches';
EXPLAIN fragments = 1
SELECT a.k, a.c
FROM (SELECT bid % 5000 AS k, count() AS c FROM big GROUP BY k) AS a, small
WHERE small.sid = a.k
SETTINGS distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- hash exchange on two keys';
EXPLAIN fragments = 1
SELECT count()
FROM big, small
WHERE small.sid = big.bid AND small.sid % 7 = big.bid % 7
SETTINGS distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- persisted exchanges';
EXPLAIN fragments = 1
SELECT bid % 10 AS k, sum(bid)
FROM big
GROUP BY k
SETTINGS distributed_plan_force_exchange_kind = 'Persisted';

-- The runtime filter is built in `F1` and applied in `F0`, so its merge stages and their exchanges are added to the
-- distributed plan. They carry no query data and are left out of the fragment graph; the steps that build and apply
-- the filter stay in the data fragments.
SELECT '-- runtime filter merge stages are left out';
EXPLAIN fragments = 1
SELECT count()
FROM big, small
WHERE small.sid = big.bid
SETTINGS enable_join_runtime_filters = 1, distributed_plan_join_runtime_filters = 1, distributed_plan_max_rows_to_broadcast = 0;

DROP TABLE small;
DROP TABLE big;
