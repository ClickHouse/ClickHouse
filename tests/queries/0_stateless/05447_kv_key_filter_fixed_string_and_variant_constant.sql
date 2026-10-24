-- Tags: zookeeper, no-ordinary-database, no-fasttest, use-rocksdb
-- no-fasttest: rocksdb is not enabled in fasttest.

-- A key lookup narrows the candidates, so the keys it looks up must include every stored key `=` matches.
-- `String` compares with `FixedString` zero-padded, and a `Variant` or `Dynamic` constant converts as the
-- value it holds.

DROP TABLE IF EXISTS t_kv_str;
CREATE TABLE t_kv_str (key String, id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_str VALUES ('V0', 0), ('V0\0', 1), ('V0\0\0', 2), ('V0X', 3), ('X', 4);

SELECT count() FROM t_kv_str WHERE key = toFixedString('V0', 3);
SELECT count() FROM t_kv_str WHERE key = toFixedString('V0', 2);
SELECT count() FROM t_kv_str WHERE key = CAST(toFixedString('V0', 3) AS LowCardinality(Nullable(FixedString(3))));
SELECT count() FROM t_kv_str WHERE key = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64));
SELECT count() FROM t_kv_str WHERE key = CAST(toFixedString('V0', 3) AS Dynamic);
SELECT count() FROM t_kv_str WHERE key = CAST(toFixedString('V0', 3) AS Dynamic(max_types = 0));

-- A `FixedString` constant is not a lookup; a `Dynamic` holding a `String` still is.
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM t_kv_str WHERE key = toFixedString('V0', 3)) WHERE explain ILIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM t_kv_str WHERE key = CAST('V0' AS Dynamic)) WHERE explain ILIKE '%ReadType%';

DROP TABLE t_kv_str;

-- A `Variant` or `Dynamic` constant converts with the type of the value it holds.
DROP TABLE IF EXISTS t_kv_enum;
CREATE TABLE t_kv_enum (key String, id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_enum VALUES ('a', 0), ('1', 1);

SELECT count() FROM t_kv_enum WHERE key = CAST(CAST('a' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic);
SELECT count() FROM t_kv_enum WHERE key = CAST(CAST('a' AS Enum8('a' = 1, 'b' = 2)) AS Variant(Enum8('a' = 1, 'b' = 2), UInt64));

DROP TABLE t_kv_enum;

DROP TABLE IF EXISTS t_kv_date;
CREATE TABLE t_kv_date (key DateTime('UTC'), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_date VALUES ('2020-01-01 00:00:00', 0), ('2020-01-02 00:00:00', 1);

SELECT count() FROM t_kv_date WHERE key = CAST(toDate('2020-01-01') AS Dynamic);
SELECT count() FROM t_kv_date WHERE key = CAST(toDate('2020-01-01') AS Variant(Date, String));
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM t_kv_date WHERE key = CAST(toDate('2020-01-01') AS Dynamic)) WHERE explain ILIKE '%ReadType%';

DROP TABLE t_kv_date;

-- A composite primary key.
DROP TABLE IF EXISTS t_kv_pair;
CREATE TABLE t_kv_pair (a String, b String, id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY (a, b);
INSERT INTO t_kv_pair VALUES ('x', 'V0', 0), ('x', 'V0\0', 1), ('x', 'V0X', 2);

SELECT count() FROM t_kv_pair WHERE a = 'x' AND b = toFixedString('V0', 3);
SELECT count() FROM t_kv_pair WHERE (a, b) = (SELECT ('x', toFixedString('V0', 3)));

DROP TABLE t_kv_pair;

-- A key column of a composite type.
DROP TABLE IF EXISTS t_kv_tuple;
CREATE TABLE t_kv_tuple (key Tuple(String, UInt32), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_tuple VALUES (('V0', 1), 0), (('V0\0', 1), 1), (('V0X', 1), 2);

SELECT count() FROM t_kv_tuple WHERE key = (toFixedString('V0', 3), 1);
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM t_kv_tuple WHERE key = ('V0', 1)) WHERE explain ILIKE '%ReadType%';

DROP TABLE t_kv_tuple;

DROP TABLE IF EXISTS t_kv_tuple_date;
CREATE TABLE t_kv_tuple_date (key Tuple(DateTime('UTC'), UInt32), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_tuple_date VALUES (('2020-01-01 00:00:00', 1), 0);

SELECT count() FROM t_kv_tuple_date WHERE key = (toDate('2020-01-01'), 1);

DROP TABLE t_kv_tuple_date;

DROP TABLE IF EXISTS t_kv_array;
CREATE TABLE t_kv_array (key Array(String), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_array VALUES (['V0'], 0), (['V0\0'], 1);

SELECT count() FROM t_kv_array WHERE key = [toFixedString('V0', 3)];

DROP TABLE t_kv_array;

DROP TABLE IF EXISTS t_kv_map;
CREATE TABLE t_kv_map (key Map(String, DateTime('UTC')), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_map VALUES (map('a', '2020-01-01 00:00:00'), 0);

SELECT count() FROM t_kv_map WHERE key = map('a', toDate('2020-01-01'));
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM t_kv_map WHERE key = map('a', toDateTime('2020-01-01 00:00:00', 'UTC'))) WHERE explain ILIKE '%ReadType%';

DROP TABLE t_kv_map;

-- The shape the AST fuzzer found.
DROP TABLE IF EXISTS t_keeper_map;
CREATE TABLE t_keeper_map (id UInt64, v LowCardinality(String)) ENGINE = KeeperMap('/' || currentDatabase() || '/05447_kv_key_filter_fixed_string_and_variant_constant') PRIMARY KEY v;
INSERT INTO t_keeper_map VALUES (0, 'V0'), (1, 'V0\0'), (2, 'V0\0\0'), (3, 'V0X'), (4, 'X');

SELECT count() FROM t_keeper_map WHERE v = CAST(toFixedString('V0', 3) AS Dynamic);

DROP TABLE t_keeper_map;
