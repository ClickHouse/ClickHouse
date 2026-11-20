-- The build side of the join is read in full by every replica, and lazy materialization splits that read in two: the
-- main half reads the sorting column, the lazy half the payload of the top rows. Both load the same compressed blocks of
-- the compact part, which compresses its columns together, and those must be counted once in the duplicated bytes.

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;

CREATE TABLE t_probe (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_build (id UInt64, x UInt64, payload String) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, compress_per_column_in_compact_parts = 0;

INSERT INTO t_probe SELECT number, number * 3 FROM numbers(2000000);
INSERT INTO t_build SELECT number * 2, cityHash64(number), repeat(hex(cityHash64(number)), 4) FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0;
-- Keep the build side on the right and unfiltered, so that the same read is duplicated in both queries.
SET query_plan_join_swap_table = 0, enable_join_runtime_filters = 0, query_plan_optimize_join_order_randomize = 0;
SET use_columns_cache = 0, use_query_condition_cache = 0, max_rows_to_read = 0;
SET query_plan_max_limit_for_lazy_materialization = 0;

SELECT p.v, b.payload FROM t_probe AS p INNER JOIN t_build AS b ON p.id = b.id ORDER BY b.x LIMIT 10
FORMAT Null SETTINGS query_plan_optimize_lazy_materialization = 1, log_comment = '05838_lazy';
SELECT p.v, b.payload FROM t_probe AS p INNER JOIN t_build AS b ON p.id = b.id ORDER BY b.x LIMIT 10
FORMAT Null SETTINGS query_plan_optimize_lazy_materialization = 0, log_comment = '05838_not_lazy';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log, text_log;

-- The lazy half adds only what the lazy read of the probe side loads, which is small.
WITH duplicated AS
(
    SELECT q.log_comment AS name, toUInt64(extract(t.message, 'duplicated bytes=(\\d+)')) AS bytes
    FROM system.text_log AS t
    INNER JOIN system.query_log AS q ON t.query_id = q.query_id
    WHERE q.event_date >= yesterday() AND q.event_time >= now() - INTERVAL 15 MINUTE AND q.current_database = currentDatabase()
        AND q.log_comment IN ('05838_lazy', '05838_not_lazy') AND q.type = 'QueryFinish'
        AND t.event_date >= yesterday() AND t.message LIKE 'Collected statistics:%'
)
SELECT not_lazy > 0 AND lazy BETWEEN not_lazy AND not_lazy * 1.25 AS counted_once
FROM
(
    SELECT anyIf(bytes, name = '05838_not_lazy') AS not_lazy, anyIf(bytes, name = '05838_lazy') AS lazy
    FROM duplicated
);

DROP TABLE t_probe;
DROP TABLE t_build;
