-- dictGetOrNull with a tuple of attributes whose first attribute is Nullable.

DROP DICTIONARY IF EXISTS d_tuple_nullable;
DROP TABLE IF EXISTS t_tuple_nullable;

CREATE TABLE t_tuple_nullable (id UInt64, a Nullable(String), b UInt64) ENGINE = Memory;
INSERT INTO t_tuple_nullable VALUES (1, 'x', 10), (2, NULL, 20);

CREATE DICTIONARY d_tuple_nullable (id UInt64, a Nullable(String), b UInt64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_tuple_nullable' DB currentDatabase()))
LAYOUT(HASHED()) LIFETIME(0);

SELECT number, dictGetOrNull('d_tuple_nullable', ('a', 'b'), number) AS t, toTypeName(t) FROM numbers(4) ORDER BY number;
SELECT number, dictGetOrNull('d_tuple_nullable', ('b', 'a'), number) FROM numbers(4) ORDER BY number;

DROP DICTIONARY d_tuple_nullable;
DROP TABLE t_tuple_nullable;
