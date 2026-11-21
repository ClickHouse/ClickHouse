-- Tags: shard

-- A `Merge` table reads its children at `FetchColumns` under `ARRAY JOIN` or `JOIN`. A `Distributed` child that has
-- none of the requested columns must count its rows by a column the initiator and the shard both read, and that
-- column must not be taken for a requested one.

DROP TABLE IF EXISTS m_c;
DROP TABLE IF EXISTS m_out;
DROP TABLE IF EXISTS m_in;
DROP TABLE IF EXISTS m_r;
DROP TABLE IF EXISTS m_a;
DROP TABLE IF EXISTS m_alias;
DROP TABLE IF EXISTS m_2;
DROP TABLE IF EXISTS m_k;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS v_alias;
DROP TABLE IF EXISTS dc;
DROP TABLE IF EXISTS da;
DROP TABLE IF EXISTS dk;
DROP TABLE IF EXISTS d2;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS la;
DROP TABLE IF EXISTS lk;
DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS lc;
DROP TABLE IF EXISTS u;

CREATE TABLE u (id Int64, t String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE l (id Int64) ENGINE = MergeTree ORDER BY id;
-- The cheapest column of `lk` is `k`, which the `Merge` tables do not have.
CREATE TABLE lk (id Int64, k Int8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE la (id Int64, a Int64 ALIAS id + 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO u VALUES (1, 'a'), (2, 'b');
INSERT INTO l VALUES (11), (12), (13);
INSERT INTO lk VALUES (21, 1), (22, 2);
INSERT INTO la VALUES (31), (32);

CREATE TABLE d AS l ENGINE = Distributed(test_shard_localhost, currentDatabase(), l);
CREATE TABLE d2 AS l ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), l);
CREATE TABLE dk AS lk ENGINE = Distributed(test_shard_localhost, currentDatabase(), lk);
CREATE TABLE da AS la ENGINE = Distributed(test_shard_localhost, currentDatabase(), la);
CREATE TABLE v_alias ENGINE = Alias(currentDatabase(), 'd');
CREATE TABLE r AS remote('127.0.0.1', currentDatabase(), l);

CREATE TABLE m (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(u|d)$');
CREATE TABLE m_k (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(u|dk)$');
CREATE TABLE m_2 (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(u|d2)$');
CREATE TABLE m_alias (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(u|v_alias)$');
CREATE TABLE m_a (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(u|da)$');
CREATE TABLE m_r (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(u|r)$');
CREATE TABLE m_in (id Int64) ENGINE = Merge(currentDatabase(), '^d$');
CREATE TABLE m_out (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(u|m_in)$');

-- A requested column that the `Distributed` table declares as an `ALIAS` over a constant reads no column either.
CREATE TABLE lc (id Int64, t String ALIAS 'z') ENGINE = MergeTree ORDER BY id;
INSERT INTO lc VALUES (41), (42);
CREATE TABLE dc AS lc ENGINE = Distributed(test_shard_localhost, currentDatabase(), lc);
CREATE TABLE m_c (id Int64, t String) ENGINE = Merge(currentDatabase(), '^(u|dc)$');

-- { echoOn }

SELECT count(t) FROM m ARRAY JOIN [1] AS x;
SELECT arraySort(groupArray(t)) FROM m ARRAY JOIN [1] AS x;
SELECT t, count() FROM m ARRAY JOIN [1] AS x GROUP BY t ORDER BY t;
SELECT count() FROM m ARRAY JOIN [1] AS x WHERE t = '';
SELECT countIf(t = '') FROM m ARRAY JOIN [1] AS x;
SELECT arraySort(groupArray(t)) FROM m AS a CROSS JOIN (SELECT 1 AS z) AS b;
SELECT arraySort(groupArray(t)) FROM merge(currentDatabase(), '^(u|d)$') ARRAY JOIN [1] AS x;

SELECT arraySort(groupArray(t)) FROM m ARRAY JOIN [1] AS x SETTINGS prefer_localhost_replica = 0;
SELECT arraySort(groupArray(t)) FROM m ARRAY JOIN [1] AS x SETTINGS prefer_localhost_replica = 1;
SELECT arraySort(groupArray(t)) FROM m ARRAY JOIN [1] AS x SETTINGS serialize_query_plan = 1;

SELECT arraySort(groupArray(t)), count() FROM m_k ARRAY JOIN [1] AS x;
SELECT arraySort(groupArray(t)), count(t) FROM m_2 ARRAY JOIN [1] AS x;
SELECT arraySort(groupArray(t)), count(t) FROM m_alias ARRAY JOIN [1] AS x;
SELECT arraySort(groupArray(t)), count(t) FROM m_a ARRAY JOIN [1] AS x;
SELECT arraySort(groupArray(t)), count(t) FROM m_out ARRAY JOIN [1] AS x;
SELECT arraySort(groupArray(t)), count(t) FROM m_r ARRAY JOIN [1] AS x;
SELECT count() FROM m_r ARRAY JOIN [1] AS x WHERE t = '';
SELECT arraySort(groupArray(t)), count(t) FROM m_c ARRAY JOIN [1] AS x;
SELECT count() FROM m_c ARRAY JOIN [1] AS x WHERE t = 'z';

-- The child reads a requested column.
SELECT arraySort(groupArray(id)) FROM m ARRAY JOIN [1] AS x;
SELECT count() FROM m ARRAY JOIN [1] AS x;

-- { echoOff }

DROP TABLE m_c;
DROP TABLE m_out;
DROP TABLE m_in;
DROP TABLE m_r;
DROP TABLE m_a;
DROP TABLE m_alias;
DROP TABLE m_2;
DROP TABLE m_k;
DROP TABLE m;
DROP TABLE r;
DROP TABLE v_alias;
DROP TABLE dc;
DROP TABLE da;
DROP TABLE dk;
DROP TABLE d2;
DROP TABLE d;
DROP TABLE la;
DROP TABLE lk;
DROP TABLE l;
DROP TABLE lc;
DROP TABLE u;
