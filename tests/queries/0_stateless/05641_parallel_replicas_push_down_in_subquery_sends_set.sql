-- A filter that parallel replicas or distributed index analysis get from the initiator
-- is checked on the replicas against the set of the initiator, if `IN` is made global.
-- https://github.com/ClickHouse/ClickHouse/issues/129593

CREATE TABLE events (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 10, distributed_index_analysis_min_parts_to_activate = 0, distributed_index_analysis_min_indexes_bytes_to_activate = 0;
SYSTEM STOP MERGES events;
INSERT INTO events SELECT number, number FROM numbers(1000) SETTINGS max_block_size = 100, min_insert_block_size_rows = 100, max_insert_threads = 1;
CREATE VIEW events_view AS SELECT id, v FROM events;

-- Empty everywhere but on the initiator.
CREATE VIEW lookup AS SELECT toUInt32(number * 100) AS id FROM numbers(10) WHERE queryID() = initialQueryID();

SET distributed_product_mode = 'global', analyzer_inline_views = 0, serialize_query_plan = 0, automatic_parallel_replicas_mode = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

SELECT 'parallel replicas';
SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 0,
    parallel_replicas_allow_view_over_mergetree = 0, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_filter_pushdown = 1, parallel_replicas_local_plan = 0;
SELECT count() FROM events_view WHERE id IN (SELECT id FROM lookup);
SELECT count() FROM events_view WHERE v IN lookup;
SELECT count() FROM (SELECT id FROM events ORDER BY id) WHERE id IN (SELECT id FROM lookup) SETTINGS parallel_replicas_allow_in_with_subquery = 0;
-- The initiator reads in order only if the replicas do: they have to get `k = 1` without the `IN`.
CREATE TABLE ordered (k UInt32, id UInt32) ENGINE = MergeTree ORDER BY (k, id) SETTINGS index_granularity = 10 AS SELECT number % 2, number FROM numbers(1000);
-- Over a subquery, the replicas would get the whole query, because its sorting is redundant.
CREATE VIEW ordered_view AS SELECT * FROM ordered ORDER BY id;
SELECT count() FROM ordered_view WHERE ((k = 1 AND id - 1 IN (SELECT id FROM lookup)) AS c) AND (c OR id > 3)
    SETTINGS parallel_replicas_local_plan = 1, optimize_read_in_order = 1, max_rows_to_transfer = 2;
DROP VIEW ordered_view;
DROP TABLE ordered;

SELECT 'distributed index analysis';
SET enable_parallel_replicas = 0, distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1,
    use_statistics_for_part_pruning = 0, use_query_condition_cache = 0;
SELECT count() FROM events WHERE id IN (SELECT id FROM lookup);
SELECT count() FROM events WHERE id IN lookup;

DROP VIEW lookup;
DROP VIEW events_view;
DROP TABLE events;
