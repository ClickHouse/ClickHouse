-- Tags: shard

-- A filter over a subquery that reads a `Distributed` table is also sent to the shards.
-- If `IN` is made global and its set cannot be sent, the condition stays on the initiator.
-- https://github.com/ClickHouse/ClickHouse/issues/129593

SET distributed_product_mode = 'global', allow_push_predicate_ast_for_distributed_subqueries = 1, prefer_localhost_replica = 0, serialize_query_plan = 0,
    enable_parallel_replicas = 0, analyzer_inline_views = 0;

CREATE TABLE events_local (id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
INSERT INTO events_local SELECT number FROM numbers(1000);
CREATE TABLE events AS events_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), events_local);
CREATE VIEW events_view AS SELECT id FROM events;

-- The same name gives other rows on a shard: 0..4 on the initiator, 10..14 and 20..24 on the shards.
CREATE VIEW lookup AS SELECT toUInt32(number + 10 * shardNum()) AS id FROM numbers(5);

SELECT count() FROM events_view WHERE id IN (SELECT arrayJoin(range(5000))) SETTINGS max_rows_to_read_leaf = 3000;
SELECT count() FROM events_view WHERE id IN (SELECT arrayJoin(range(2500))) SETTINGS max_rows_to_read_leaf = 3000, read_overflow_mode_leaf = 'break', max_block_size = 10;
SELECT count() FROM events_view WHERE id IN (SELECT arrayJoin(range(1200))) AND id + 1 IN (SELECT arrayJoin(range(1300))) SETTINGS max_rows_to_read_leaf = 3000;
SELECT count() FROM events_view WHERE id IN (SELECT cityHash64(number) FROM numbers(100000)) SETTINGS max_network_bytes = 100000;
SELECT count() FROM events_view WHERE id IN (SELECT id FROM lookup) SETTINGS max_rows_to_transfer = 2;
SELECT count() FROM events_view WHERE id IN (SELECT id FROM lookup) SETTINGS max_bytes_to_transfer = 2, transfer_overflow_mode = 'break';
SELECT count() FROM (SELECT id FROM events SETTINGS max_rows_in_set = 2) WHERE id IN (SELECT id FROM lookup);
SELECT count() FROM (SELECT nullIf(id, 7) AS n FROM events SETTINGS transform_null_in = 0) WHERE n IN (SELECT nullIf(id, 1) FROM lookup) SETTINGS transform_null_in = 1;
-- 2 and 3 match `true` only if the setting is disabled.
SELECT count() FROM (SELECT toUInt8(id % 4) AS u FROM events SETTINGS transform_null_in = 1) WHERE u IN (SELECT true FROM lookup) SETTINGS transform_null_in = 0;
-- The other conjuncts are sent, but not under `NOT`.
SELECT count() FROM (SELECT id < 50 AND id IN (SELECT id FROM lookup) AS c FROM events_view) WHERE c SETTINGS max_rows_to_transfer = 2, max_rows_to_read_leaf = 100;
SELECT count() FROM (SELECT id < 50 AND id IN (SELECT id FROM lookup) AS c FROM events_view) WHERE NOT c SETTINGS max_rows_to_transfer = 2;

DROP VIEW lookup;
DROP VIEW events_view;
DROP TABLE events;
DROP TABLE events_local;
