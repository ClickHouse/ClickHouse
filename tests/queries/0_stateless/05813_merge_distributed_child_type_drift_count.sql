-- Tags: shard

-- A `Merge` table whose sources declare `id` with types that do not convert to the common type
-- without changing the order is read at `FetchColumns`. A `Distributed` source must then be asked
-- for exactly the columns to read, not for the whole query.

DROP TABLE IF EXISTS m_out;
DROP TABLE IF EXISTS m_in;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS v_alias;
DROP TABLE IF EXISTS d_i2;
DROP TABLE IF EXISTS d_i;
DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_i;
DROP TABLE IF EXISTS t_u;

CREATE TABLE t_u (id UInt64, t String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_u VALUES (1, 'a'), (2, 'a');
CREATE TABLE t_i (id Int64, t String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_i VALUES (11, 'b'), (12, 'b'), (13, 'b');
CREATE TABLE d_i AS t_i ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_i);
CREATE TABLE d_i2 AS t_i ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_i);
CREATE TABLE m (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(t_u|d_i)$');
CREATE TABLE m_in ENGINE = Merge(currentDatabase(), '^d_i$');
CREATE TABLE m_out ENGINE = Merge(currentDatabase(), '^(t_u|m_in)$');

-- A local table with the same columns as `d_i`, listed before an `Alias` of `d_i`.
CREATE TABLE t_l (id Int64, t String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_l VALUES (21, 'l');
CREATE TABLE v_alias ENGINE = Alias(currentDatabase(), 'd_i');

-- { echoOn }

SELECT count() FROM m;
SELECT count(*) FROM m;
SELECT count(1) FROM m;
SELECT sum(1) FROM m;
SELECT count(id) FROM m;
SELECT count(t) FROM m;
SELECT count() FROM m WHERE t != '';
SELECT count() FROM (SELECT 1 FROM m);
SELECT count() FROM m SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM m SETTINGS prefer_localhost_replica = 1;
SELECT count() FROM m SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM m SETTINGS serialize_query_plan = 1;

SELECT count() FROM merge(currentDatabase(), '^(t_u|d_i)$');
SELECT count(*) FROM merge(currentDatabase(), '^(t_u|d_i)$');
SELECT count(1) FROM merge(currentDatabase(), '^(t_u|d_i)$');
SELECT sum(1) FROM merge(currentDatabase(), '^(t_u|d_i)$');
SELECT count(id) FROM merge(currentDatabase(), '^(t_u|d_i)$');
SELECT count(t) FROM merge(currentDatabase(), '^(t_u|d_i)$');
SELECT count() FROM merge(currentDatabase(), '^(t_u|d_i)$') WHERE t != '';
SELECT count() FROM (SELECT 1 FROM merge(currentDatabase(), '^(t_u|d_i)$'));
SELECT count() FROM merge(currentDatabase(), '^(t_u|d_i)$') SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM merge(currentDatabase(), '^(t_u|d_i)$') SETTINGS prefer_localhost_replica = 1;
SELECT count() FROM merge(currentDatabase(), '^(t_u|d_i)$') SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM merge(currentDatabase(), '^(t_u|d_i)$') SETTINGS serialize_query_plan = 1;

SELECT count() FROM merge(currentDatabase(), '^(t_u|d_i2)$');
SELECT count() FROM m_out;

SELECT arraySort(groupArray(id)) FROM merge(currentDatabase(), '^(t_l|v_alias)$') ARRAY JOIN [1] AS x;
SELECT count() FROM merge(currentDatabase(), '^(t_u|t_l|v_alias)$');

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM m);

SELECT arraySort(groupArray(id)) FROM m;
SELECT id FROM m ORDER BY id;
SELECT t, count() FROM m GROUP BY t ORDER BY t;

-- { echoOff }

DROP TABLE m_out;
DROP TABLE m_in;
DROP TABLE m;
DROP TABLE v_alias;
DROP TABLE d_i2;
DROP TABLE d_i;
DROP TABLE t_l;
DROP TABLE t_i;
DROP TABLE t_u;
