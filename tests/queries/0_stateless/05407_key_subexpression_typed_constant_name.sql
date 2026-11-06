-- A folded typed constant in a key expression must not be named like a constant of another type,
-- otherwise the primary key is matched against a predicate over a different expression.

DROP TABLE IF EXISTS t_key_uint128;
DROP TABLE IF EXISTS t_key_uuid;

CREATE TABLE t_key_uint128 (x UInt64) ENGINE = MergeTree
ORDER BY (cityHash64(x, toUInt128(1)) + 1)
SETTINGS index_granularity = 1;
INSERT INTO t_key_uint128 SELECT number FROM numbers(1000);

SELECT x FROM t_key_uint128 WHERE cityHash64(x, '1') = cityHash64(toUInt64(1), '1');
SELECT number FROM numbers(1000) WHERE cityHash64(number, '1') = cityHash64(toUInt64(1), '1');
SELECT x FROM t_key_uint128 WHERE cityHash64(x, toUInt128(1)) = cityHash64(toUInt64(1), toUInt128(1));

CREATE TABLE t_key_uuid (x UInt64) ENGINE = MergeTree
ORDER BY (cityHash64(x, toUUID('00000000-0000-0000-0000-000000000001')) + 1)
SETTINGS index_granularity = 1;
INSERT INTO t_key_uuid SELECT number FROM numbers(1000);

SELECT x FROM t_key_uuid
WHERE cityHash64(x, '00000000-0000-0000-0000-000000000001') = cityHash64(toUInt64(1), '00000000-0000-0000-0000-000000000001');

DROP TABLE t_key_uint128;
DROP TABLE t_key_uuid;
