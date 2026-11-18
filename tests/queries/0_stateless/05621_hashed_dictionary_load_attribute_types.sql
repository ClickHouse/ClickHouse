-- Loading a hashed dictionary reads the attribute values directly from the columns of the source block.
-- Check every kind of attribute column against the source table, including NULL values and duplicate keys.

DROP TABLE IF EXISTS src_05621;
CREATE TABLE src_05621
(
    id UInt64,
    u8 UInt8,
    i64 Int64,
    u256 UInt256,
    f32 Float32,
    d32 Decimal32(3),
    d128 Decimal128(10),
    dt DateTime('UTC'),
    dt64 DateTime64(6, 'UTC'),
    uuid UUID,
    ip4 IPv4,
    ip6 IPv6,
    s String,
    n_u64 Nullable(UInt64),
    n_s Nullable(String),
    n_d64 Nullable(Decimal64(4)),
    arr Array(UInt32)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO src_05621 SELECT
    number,
    number % 256,
    toInt64(number) - 500,
    toUInt256(number) * toUInt256('1000000000000000000000'),
    number / 7,
    toDecimal32(number / 3, 3),
    toDecimal128(number / 11, 10),
    toDateTime('2026-01-01 00:00:00', 'UTC') + number,
    toDateTime64('2026-01-01 00:00:00', 6, 'UTC') + toIntervalMicrosecond(number * 1001),
    generateUUIDv4(number),
    toIPv4(number * 16777259),
    toIPv6(toIPv4(number)),
    repeat('x', number % 17) || toString(number),
    if(number % 3 = 0, NULL, number * 2),
    if(number % 5 = 0, NULL, 'v' || toString(number)),
    if(number % 4 = 0, NULL, toDecimal64(number / 9, 4)),
    range(number % 5)
FROM numbers(1000);

-- Duplicate keys: the first value of a key must be kept.
INSERT INTO src_05621 (id, u8, s, n_u64) SELECT number, 255, 'duplicate', 1 FROM numbers(10);

-- A `QUERY` source runs in the default database of the session, so read through a view with a `TABLE` source,
-- which takes the current database. The view puts the duplicates first.
DROP VIEW IF EXISTS ordered_05621;
CREATE VIEW ordered_05621 AS SELECT * FROM src_05621 ORDER BY u8 = 255 DESC, id;

DROP DICTIONARY IF EXISTS dict_hashed_05621;
DROP DICTIONARY IF EXISTS dict_sparse_05621;
DROP DICTIONARY IF EXISTS dict_sharded_05621;
DROP DICTIONARY IF EXISTS dict_complex_05621;

CREATE DICTIONARY dict_hashed_05621
(
    id UInt64, u8 UInt8, i64 Int64, u256 UInt256, f32 Float32, d32 Decimal32(3), d128 Decimal128(10),
    dt DateTime('UTC'), dt64 DateTime64(6, 'UTC'), uuid UUID, ip4 IPv4, ip6 IPv6, s String,
    n_u64 Nullable(UInt64), n_s Nullable(String), n_d64 Nullable(Decimal64(4)), arr Array(UInt32)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ordered_05621'))
LAYOUT(HASHED())
LIFETIME(0);

CREATE DICTIONARY dict_sparse_05621
(
    id UInt64, u8 UInt8, i64 Int64, u256 UInt256, f32 Float32, d32 Decimal32(3), d128 Decimal128(10),
    dt DateTime('UTC'), dt64 DateTime64(6, 'UTC'), uuid UUID, ip4 IPv4, ip6 IPv6, s String,
    n_u64 Nullable(UInt64), n_s Nullable(String), n_d64 Nullable(Decimal64(4)), arr Array(UInt32)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ordered_05621'))
LAYOUT(SPARSE_HASHED())
LIFETIME(0);

CREATE DICTIONARY dict_sharded_05621
(
    id UInt64, u8 UInt8, i64 Int64, u256 UInt256, f32 Float32, d32 Decimal32(3), d128 Decimal128(10),
    dt DateTime('UTC'), dt64 DateTime64(6, 'UTC'), uuid UUID, ip4 IPv4, ip6 IPv6, s String,
    n_u64 Nullable(UInt64), n_s Nullable(String), n_d64 Nullable(Decimal64(4)), arr Array(UInt32)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ordered_05621'))
LAYOUT(HASHED(SHARDS 4))
LIFETIME(0);

CREATE DICTIONARY dict_complex_05621
(
    id UInt64, s String, u8 UInt8, d128 Decimal128(10), n_u64 Nullable(UInt64), n_s Nullable(String), arr Array(UInt32)
)
PRIMARY KEY id, s
SOURCE(CLICKHOUSE(TABLE 'src_05621'))
LAYOUT(COMPLEX_KEY_SPARSE_HASHED(SHARDS 2))
LIFETIME(0);

-- The dictionaries keep the rows that come first in the source query: the duplicates.
SELECT 'duplicates', dictGet('dict_hashed_05621', ('u8', 's', 'n_u64', 'i64'), toUInt64(3)), dictGet('dict_sparse_05621', 's', toUInt64(3)), dictGet('dict_sharded_05621', 's', toUInt64(3));

SELECT 'nulls', dictGet('dict_sparse_05621', ('n_u64', 'n_s', 'n_d64'), toUInt64(60)), dictGet('dict_hashed_05621', ('n_u64', 'n_s', 'n_d64'), toUInt64(61));

-- Compare every attribute of the keys that are not duplicated with the source table.
SELECT
    name,
    count(),
    countIf(toString(dict_row) = toString(src_row))
FROM
(
    SELECT
        arrayJoin([
            ('hashed', dictGet('dict_hashed_05621', ('u8', 'i64', 'u256', 'f32', 'd32', 'd128', 'dt', 'dt64', 'uuid', 'ip4', 'ip6', 's', 'n_u64', 'n_s', 'n_d64', 'arr'), id)),
            ('sparse', dictGet('dict_sparse_05621', ('u8', 'i64', 'u256', 'f32', 'd32', 'd128', 'dt', 'dt64', 'uuid', 'ip4', 'ip6', 's', 'n_u64', 'n_s', 'n_d64', 'arr'), id)),
            ('sharded', dictGet('dict_sharded_05621', ('u8', 'i64', 'u256', 'f32', 'd32', 'd128', 'dt', 'dt64', 'uuid', 'ip4', 'ip6', 's', 'n_u64', 'n_s', 'n_d64', 'arr'), id))
        ]) AS name_and_row,
        name_and_row.1 AS name,
        name_and_row.2 AS dict_row,
        (u8, i64, u256, f32, d32, d128, dt, dt64, uuid, ip4, ip6, s, n_u64, n_s, n_d64, arr) AS src_row
    FROM src_05621
    WHERE id >= 10
)
GROUP BY name
ORDER BY name;

SELECT
    'complex',
    count(),
    countIf(toString(dictGet('dict_complex_05621', ('u8', 'd128', 'n_u64', 'n_s', 'arr'), (id, s))) = toString((u8, d128, n_u64, n_s, arr))),
    countIf(dictHas('dict_complex_05621', (id, s)))
FROM src_05621;

SELECT 'element_count', name, element_count FROM system.dictionaries WHERE database = currentDatabase() ORDER BY name;

DROP DICTIONARY dict_hashed_05621;
DROP DICTIONARY dict_sparse_05621;
DROP DICTIONARY dict_sharded_05621;
DROP DICTIONARY dict_complex_05621;
DROP VIEW ordered_05621;
DROP TABLE src_05621;
