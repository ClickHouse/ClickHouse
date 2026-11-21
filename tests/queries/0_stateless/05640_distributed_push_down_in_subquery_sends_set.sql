-- Tags: shard

-- A filter over a subquery that reads a `Distributed` table is also sent to the shards.
-- If `IN` is made global, it is checked there against the set of the initiator, or not at all.
-- https://github.com/ClickHouse/ClickHouse/issues/129593

SET distributed_product_mode = 'global', allow_push_predicate_ast_for_distributed_subqueries = 1, prefer_localhost_replica = 0, serialize_query_plan = 0,
    enable_parallel_replicas = 0, analyzer_inline_views = 0;

CREATE TABLE events_local (id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
INSERT INTO events_local SELECT number FROM numbers(1000);
CREATE TABLE events AS events_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), events_local);
CREATE VIEW events_view AS SELECT id FROM events;
CREATE VIEW events_union AS SELECT id FROM events UNION ALL SELECT id FROM events;

-- The same name gives other rows on a shard: 0..4 on the initiator, 10..14 and 20..24 on the shards.
CREATE VIEW lookup AS SELECT toUInt32(number + 10 * shardNum()) AS id FROM numbers(5);

-- Fails unless the shards use the filter to read less.
SET max_rows_to_read_leaf = 100;

SELECT 'a table that differs on the shards';
SELECT count() FROM events_view WHERE id GLOBAL IN lookup;
SELECT count() FROM events_union WHERE id GLOBAL IN (SELECT id FROM lookup);
SELECT count() FROM events_view WHERE id GLOBAL IN (SELECT id FROM lookup) AND id + 0 IN (SELECT id FROM lookup);
CREATE TEMPORARY TABLE temp AS SELECT toUInt32(number) AS id FROM numbers(3);
SELECT count() FROM events_view WHERE id IN temp AND id IN (SELECT id FROM lookup);
SELECT count() FROM events_view WHERE id IN (SELECT id FROM lookup) SETTINGS read_overflow_mode_leaf = 'break', set_overflow_mode = 'break', timeout_overflow_mode = 'break', transfer_overflow_mode = 'break';
SELECT count() FROM events_view WHERE id IN (SELECT id FROM lookup) SETTINGS use_index_for_in_with_subqueries_max_values = 2, max_rows_to_read_leaf = 0;

SELECT 'only distinct values are sent';
SELECT count() FROM events_view WHERE id IN (SELECT arrayJoin(range(1000)) % 5) SETTINGS max_rows_to_transfer = 6;

SELECT 'a local read uses the same set';
CREATE TABLE small (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '' AS SELECT number, number FROM numbers(50);
SELECT count(id) FROM (SELECT id, x FROM small UNION ALL SELECT id, id FROM events) WHERE x IN (SELECT id FROM lookup)
    SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, max_rows_to_read_leaf = 300;

CREATE TABLE parts (id UInt32) ENGINE = MergeTree PARTITION BY intDiv(id, 50) ORDER BY id AS SELECT number FROM numbers(200);
SELECT count() FROM (SELECT _partition_id AS p FROM remote('127.0.0.{1,2}', currentDatabase(), parts)) WHERE p IN (SELECT toString(id) FROM lookup WHERE id = 1)
    SETTINGS prefer_localhost_replica = 1, max_rows_to_read_leaf = 150;

DROP TABLE parts;
DROP TABLE small;
DROP VIEW lookup;
DROP VIEW events_union;
DROP VIEW events_view;
DROP TABLE events;
DROP TABLE events_local;
