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
