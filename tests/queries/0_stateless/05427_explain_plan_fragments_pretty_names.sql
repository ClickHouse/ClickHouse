-- `EXPLAIN PLAN fragments = 1, pretty = 1`: a runtime filter or an `IN` set keeps its name in every fragment,
-- and distinct filters and sets in different fragments get distinct names.
-- The queries are only explained, never executed. Only the fragment headers and the lines naming filters and sets are kept.

DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS big;

CREATE TABLE small(sid UInt64) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE big(bid UInt64) ENGINE = MergeTree ORDER BY bid;

INSERT INTO small SELECT number FROM numbers(0, 1000);
INSERT INTO big SELECT number FROM numbers(0, 100000);

SET query_plan_join_swap_table = 0;
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SET max_rows_to_group_by = 0;
-- The test harness randomizes these, and aggregation in order changes the plan.
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
-- The test harness randomizes these too: a random join order or probe-row threshold changes or drops the runtime filters.
SET join_runtime_filter_min_probe_rows = 0, query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
-- A join order limit below the number of tables keeps the three-table join in query order, which changes where the filters are built.
SET query_plan_optimize_join_order_limit = 10;
-- In ClickHouse Cloud a scan on shared storage is replaced by `ReadFromMergeTreeAtWorker`, which prints no table name.
SET distributed_plan_prefer_replicas_over_workers = 1;

SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    use_statistics = 0,
    enable_join_runtime_filters = 1,
    distributed_plan_join_runtime_filters = 1,
    distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';

-- The filter is built in one fragment and applied in another.
SELECT '-- runtime filter across an exchange';
SELECT explain
FROM
(
    EXPLAIN fragments = 1, pretty = 1, actions = 1
    SELECT count()
    FROM big, small
    WHERE small.sid = big.bid
)
WHERE explain LIKE 'Query fragment%' OR explain ILIKE '%runtime%' OR explain LIKE '%RF_%';

SELECT '-- two runtime filters';
SELECT explain
FROM
(
    EXPLAIN fragments = 1, pretty = 1, actions = 1
    SELECT count()
    FROM big AS b1, big AS b2, small
    WHERE small.sid = b1.bid AND b2.bid = b1.bid
)
WHERE explain LIKE 'Query fragment%' OR explain ILIKE '%runtime%' OR explain LIKE '%RF_%';

-- With a broadcast join, the filter is built in the fragment that reads the broadcast side, which has a lower id than
-- the fragment that applies it.
SELECT '-- runtime filter applied in a fragment with a higher id';
SELECT explain
FROM
(
    EXPLAIN fragments = 1, pretty = 1, actions = 1
    SELECT count()
    FROM big, small
    WHERE small.sid = big.bid
    SETTINGS distributed_plan_max_rows_to_broadcast = 20000
)
WHERE explain LIKE 'Query fragment%' OR explain ILIKE '%runtime%' OR explain LIKE '%RF_%';

-- The build key of the filter is computed in a fragment below the one that builds the filter.
SELECT '-- runtime filter on a received expression';
SELECT explain
FROM
(
    EXPLAIN fragments = 1, pretty = 1, actions = 1
    SELECT count()
    FROM big, (SELECT sid % 10 AS k, count() FROM small GROUP BY k) AS s
    WHERE big.bid = s.k
)
WHERE explain LIKE 'Query fragment%' OR explain ILIKE '%runtime%' OR explain LIKE '%RF_%';

SELECT '-- distinct IN sets in different fragments';
SELECT explain
FROM
(
    EXPLAIN fragments = 1, pretty = 1, actions = 1
    SELECT count()
    FROM (SELECT bid FROM big WHERE bid IN (SELECT sid FROM small WHERE sid < 10)) AS a,
         (SELECT sid FROM small WHERE sid IN (SELECT bid FROM big WHERE bid > 990)) AS s
    WHERE s.sid = a.bid
    SETTINGS enable_join_runtime_filters = 0
)
WHERE explain LIKE 'Query fragment%' OR explain LIKE '%subquery%';

DROP TABLE small;
DROP TABLE big;
