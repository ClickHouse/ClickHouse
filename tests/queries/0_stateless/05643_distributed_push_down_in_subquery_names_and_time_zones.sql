-- Tags: shard

-- A filter over a subquery that reads a `Distributed` table is also sent to the shards.
-- The table with the set of its `IN` has to mean the same there: its name, and the time zones of its types.
-- https://github.com/ClickHouse/ClickHouse/issues/129593

SET distributed_product_mode = 'global', allow_push_predicate_ast_for_distributed_subqueries = 1, prefer_localhost_replica = 0, serialize_query_plan = 0,
    enable_parallel_replicas = 0, analyzer_inline_views = 0;

CREATE TABLE events_local (id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
INSERT INTO events_local SELECT number FROM numbers(1000);
CREATE TABLE events AS events_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), events_local);

-- The same name gives other rows on a shard: 0..4 on the initiator, 10..14 and 20..24 on the shards.
CREATE VIEW lookup AS SELECT toUInt32(number + 10 * shardNum()) AS id FROM numbers(5);

-- A shard that is an initiator too has the same subquery.
CREATE VIEW hop_global AS SELECT id FROM events WHERE id + 10 GLOBAL IN (SELECT id FROM lookup);
CREATE VIEW hop_pushed AS SELECT id FROM (SELECT id FROM events) WHERE id + 10 IN (SELECT id FROM lookup);
SELECT count() FROM (SELECT id FROM remote('127.0.0.2', currentDatabase(), hop_global)) WHERE id IN (SELECT id FROM lookup);
SELECT count() FROM (SELECT id FROM remote('127.0.0.2', currentDatabase(), hop_pushed)) WHERE id IN (SELECT id FROM lookup);

CREATE TABLE days (d Date, t DateTime, t64 Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY d;
INSERT INTO days SELECT toDate('2024-01-01') + number AS d, d, d FROM numbers(10);
CREATE TEMPORARY TABLE temp_days (t DateTime) AS SELECT t FROM days;
SET session_timezone = 'Pacific/Kiritimati';
SELECT count() FROM (SELECT d FROM remote('127.0.0.2', currentDatabase(), days)) WHERE d IN (SELECT t FROM days);
SELECT count() FROM (SELECT d FROM remote('127.0.0.2', currentDatabase(), days)) WHERE (d, [d]) IN (SELECT t64, [t] FROM days);
SELECT count() FROM (SELECT d FROM remote('127.0.0.2', currentDatabase(), days)) WHERE d IN temp_days;
SELECT count() FROM (SELECT t FROM remote('127.0.0.2', currentDatabase(), days)) WHERE t IN (SELECT t::Variant(DateTime, String) FROM days);

DROP TABLE days;
DROP VIEW hop_pushed;
DROP VIEW hop_global;
DROP VIEW lookup;
DROP TABLE events;
DROP TABLE events_local;
