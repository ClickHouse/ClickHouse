SET enable_parallel_replicas = 0;
SET enable_cascades_optimizer = 0;
SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1;
SET distributed_plan_optimize_exchanges = 1;
SET distributed_plan_max_rows_to_broadcast = 20;
SET use_statistics = 1;

CREATE TABLE merging_broadcast_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE merging_broadcast_right (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE merging_broadcast_no_stats (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';

INSERT INTO merging_broadcast_left SELECT number, number FROM numbers(1000);
INSERT INTO merging_broadcast_right SELECT number % 10, number FROM numbers(1000);
INSERT INTO merging_broadcast_no_stats SELECT number % 10, number FROM numbers(1000);

SELECT sum(l.v)
FROM merging_broadcast_left AS l
JOIN
(
    SELECT k
    FROM
    (
        SELECT k, rank() OVER (PARTITION BY k ORDER BY sum(v) DESC) AS ranking
        FROM merging_broadcast_right
        GROUP BY k
    )
    WHERE ranking <= 1
) AS r ON l.k = r.k
SETTINGS distributed_plan_fallback_to_local_execution = 0;

SELECT sum(l.v)
FROM merging_broadcast_left AS l
JOIN
(
    SELECT k
    FROM
    (
        SELECT k, rank() OVER (PARTITION BY k ORDER BY sum(v) DESC) AS ranking
        FROM merging_broadcast_right
        GROUP BY k
    )
    WHERE ranking <= 1
) AS r ON l.k = r.k
SETTINGS make_distributed_plan = 0;

SELECT
    countIf(explain LIKE '%MergingAggregated%') > 0,
    countIf(explain LIKE '%BroadcastExchange%') > 0
FROM
(
    EXPLAIN
    SELECT sum(l.v)
    FROM merging_broadcast_left AS l
    JOIN
    (
        SELECT k
        FROM
        (
            SELECT k, rank() OVER (PARTITION BY k ORDER BY sum(v) DESC) AS ranking
            FROM merging_broadcast_right
            GROUP BY k
        )
        WHERE ranking <= 1
    ) AS r ON l.k = r.k
);

SELECT countIf(explain LIKE '%BroadcastExchange%') > 0
FROM
(
    EXPLAIN
    SELECT sum(l.v)
    FROM merging_broadcast_left AS l
    JOIN
    (
        SELECT k
        FROM
        (
            SELECT k, rank() OVER (PARTITION BY k ORDER BY sum(v) DESC) AS ranking
            FROM merging_broadcast_right
            GROUP BY k
        )
        WHERE ranking <= 1
    ) AS r ON l.k = r.k
    SETTINGS distributed_plan_max_rows_to_broadcast = 0
);

SELECT countIf(explain LIKE '%BroadcastExchange%') > 0
FROM
(
    EXPLAIN
    SELECT sum(l.v)
    FROM merging_broadcast_left AS l
    JOIN
    (
        SELECT k
        FROM
        (
            SELECT k, rank() OVER (PARTITION BY k ORDER BY sum(v) DESC) AS ranking
            FROM merging_broadcast_right
            GROUP BY k
        )
        WHERE ranking <= 1
    ) AS r ON l.k = r.k
    SETTINGS use_statistics = 0
);

SELECT countIf(explain LIKE '%BroadcastExchange%') > 0
FROM
(
    EXPLAIN
    SELECT sum(l.v)
    FROM merging_broadcast_left AS l
    JOIN
    (
        SELECT k
        FROM
        (
            SELECT k, rank() OVER (PARTITION BY k ORDER BY sum(v) DESC) AS ranking
            FROM merging_broadcast_no_stats
            GROUP BY k
        )
        WHERE ranking <= 1
    ) AS r ON l.k = r.k
);

DROP TABLE merging_broadcast_left;
DROP TABLE merging_broadcast_right;
DROP TABLE merging_broadcast_no_stats;
