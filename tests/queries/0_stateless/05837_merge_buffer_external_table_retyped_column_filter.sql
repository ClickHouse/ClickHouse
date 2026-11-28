-- Tags: no-fasttest

SET send_logs_level = 'fatal'; -- failed connection tries are ok, if it succeeded after retry.

CREATE TABLE t_l (id Int64, v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_l VALUES (0, 'n0'), (5, 'n5'), (256, 'n256');

CREATE TABLE t_my (id Int64, v String) ENGINE = MySQL('127.0.0.1:9004', currentDatabase(), 't_l', 'default', '') SETTINGS connect_timeout = 100, connection_wait_timeout = 100;
CREATE TABLE t_mya (id Int64, v String, a Int64 ALIAS id, w String ALIAS v) ENGINE = MySQL('127.0.0.1:9004', currentDatabase(), 't_l', 'default', '') SETTINGS connect_timeout = 100, connection_wait_timeout = 100;

CREATE TABLE m_n (id UInt8, v String) ENGINE = Merge(currentDatabase(), '^t_my$');
CREATE TABLE b_n (id UInt8, v String) ENGINE = Buffer(currentDatabase(), t_my, 1, 1000000, 1000000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE m_a (a UInt8, v String) ENGINE = Merge(currentDatabase(), '^t_mya$');
CREATE TABLE m_nl (id Nullable(Int64), v String) ENGINE = Merge(currentDatabase(), '^t_my$');
CREATE TABLE m_lc (id Int64, v LowCardinality(String)) ENGINE = Merge(currentDatabase(), '^t_my$');
CREATE TABLE m_as (a Nullable(Int64), w LowCardinality(String)) ENGINE = Merge(currentDatabase(), '^t_mya$');

CREATE TABLE t_la (id Int64, v String, a Int64 ALIAS id) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_la VALUES (0, 'n0'), (5, 'n5'), (256, 'n256');
CREATE TABLE m_la (a UInt8, v String) ENGINE = Merge(currentDatabase(), '^t_la$');

-- The stored 256 is read as the UInt8 0.
SELECT v FROM m_n WHERE id = 0 ORDER BY v;
SELECT v FROM b_n WHERE id = 0 ORDER BY v;
SELECT v FROM m_a WHERE a = 0 ORDER BY v;
SELECT v FROM m_n WHERE id = 0 SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
SELECT v FROM b_n WHERE id = 0 SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
SELECT v FROM m_a WHERE a = 0 SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }

-- LowCardinality and an added Nullable keep every stored value, so the filter is still pushed down.
SELECT v FROM m_nl WHERE id = 5 SETTINGS external_table_strict_query = 1;
SELECT v FROM m_lc WHERE v = 'n5' SETTINGS external_table_strict_query = 1;
SELECT w FROM m_as WHERE a = 5 SETTINGS external_table_strict_query = 1;
SELECT a FROM m_as WHERE w = 'n5' SETTINGS external_table_strict_query = 1;

SELECT v FROM m_la WHERE a = 0 ORDER BY v;

-- The stored UTC instant is read under another time zone.
CREATE TABLE t_dt (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dt VALUES ('2026-01-01 00:00:00');
CREATE TABLE t_mydt (dt DateTime('UTC')) ENGINE = MySQL('127.0.0.1:9004', currentDatabase(), 't_dt', 'default', '') SETTINGS connect_timeout = 100, connection_wait_timeout = 100;
CREATE TABLE m_dt (dt DateTime('Asia/Tokyo')) ENGINE = Merge(currentDatabase(), '^t_mydt$');
CREATE TABLE b_dt (dt DateTime('Asia/Tokyo')) ENGINE = Buffer(currentDatabase(), t_mydt, 1, 1000000, 1000000, 1000000000, 1000000000, 1000000000000, 1000000000000);
SELECT count() FROM m_dt WHERE dt = toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo');
SELECT count() FROM b_dt WHERE dt = toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo');

-- A retyped ALIAS of a `Merge` child that re-analyzes the query.
CREATE TABLE t_leaf (id UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_leaf VALUES (256);
CREATE TABLE d_leaf (id UInt32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_leaf);
CREATE TABLE m_inner (id UInt32, a UInt32 ALIAS id) ENGINE = Merge(currentDatabase(), '^d_leaf$');
CREATE TABLE m_outer (id UInt32, a UInt8) ENGINE = Merge(currentDatabase(), '^m_inner$');
SELECT id, a FROM m_outer;
SELECT id, a FROM m_outer WHERE a = 0;
