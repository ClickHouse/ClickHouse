-- A `bloom_filter` index must not drop rows that the same filter selects without the index.
-- Each query is run with `use_skip_indexes = 0` (the oracle) and then with the index.

SET optimize_functions_to_subcolumns = 1;

SELECT 'DateTime64 with a Float64 constant';
-- `equals` converts both sides to `Float64`, so both ticks equal the constant.

DROP TABLE IF EXISTS t_bf_dt64_map;
CREATE TABLE t_bf_dt64_map
(
    m Map(String, DateTime64(9, 'UTC')),
    INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_dt64_map SELECT map('k', fromUnixTimestamp64Nano(1767225600000000000 + number)) FROM numbers(2);

SELECT count() FROM t_bf_dt64_map WHERE m['k'] = toFloat64(1767225600) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_dt64_map WHERE m['k'] = toFloat64(1767225600);
SELECT count() FROM t_bf_dt64_map WHERE m['k'] = toFloat64(1767225600) SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_bf_dt64_map;

DROP TABLE IF EXISTS t_bf_dt64;
CREATE TABLE t_bf_dt64
(
    d DateTime64(9, 'UTC'),
    INDEX bf d TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_dt64 SELECT fromUnixTimestamp64Nano(1767225600000000000 + number) FROM numbers(2);

SELECT count() FROM t_bf_dt64 WHERE d = toFloat64(1767225600) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_dt64 WHERE d = toFloat64(1767225600);

DROP TABLE t_bf_dt64;

SELECT 'Bool set over integer map values';
-- `Set` converts the left-hand side to the `Bool` type of the set, so every non-zero number is `true`.

DROP TABLE IF EXISTS t_bf_uint8_map;
CREATE TABLE t_bf_uint8_map
(
    m Map(String, UInt8),
    INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_uint8_map VALUES (map('k', 1)), (map('k', 2));

SELECT count() FROM t_bf_uint8_map WHERE m['k'] IN (SELECT true) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_uint8_map WHERE m['k'] IN (SELECT true);
SELECT count() FROM t_bf_uint8_map WHERE m['k'] IN (SELECT true) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bf_uint8_map WHERE m['k'] GLOBAL IN (SELECT true);

DROP TABLE t_bf_uint8_map;

SELECT 'Bool set over an integer column';

DROP TABLE IF EXISTS t_bf_uint8;
CREATE TABLE t_bf_uint8
(
    v UInt8,
    INDEX bf v TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_uint8 VALUES (1), (2);

SELECT count() FROM t_bf_uint8 WHERE v IN (SELECT true) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_uint8 WHERE v IN (SELECT true);
SELECT count() FROM t_bf_uint8 WHERE v GLOBAL IN (SELECT true);

DROP TABLE t_bf_uint8;

SELECT 'Bool set over a Bool column holding the byte 2';

DROP TABLE IF EXISTS t_bf_bool;
CREATE TABLE t_bf_bool
(
    b Bool,
    INDEX bf b TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_bool SELECT reinterpret(toUInt8(number + 1), 'Bool') FROM numbers(2);

SELECT count() FROM t_bf_bool WHERE b IN (SELECT true) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool WHERE b IN (SELECT true);
SELECT count() FROM t_bf_bool WHERE b IN (SELECT toUInt8(2)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool WHERE b IN (SELECT toUInt8(2));

DROP TABLE t_bf_bool;

SELECT 'IPv4 constant with integer map values';
-- `equals` compares the address numerically; the index must not turn it into `TYPE_MISMATCH`.

DROP TABLE IF EXISTS t_bf_uint32_map;
CREATE TABLE t_bf_uint32_map
(
    m Map(String, UInt32),
    INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_uint32_map SELECT map('k', toUInt32(toIPv4('127.0.0.1')) + number) FROM numbers(2);

SELECT count() FROM t_bf_uint32_map WHERE m['k'] = toIPv4('127.0.0.1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_uint32_map WHERE m['k'] = toIPv4('127.0.0.1');
SELECT count() FROM t_bf_uint32_map WHERE m['k'] = toIPv4('127.0.0.1') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT DISTINCT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_uint32_map WHERE m['k'] = toIPv4('127.0.0.1')) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_bf_uint32_map;

SELECT 'IPv4 constant with integer columns';

DROP TABLE IF EXISTS t_bf_uint32;
CREATE TABLE t_bf_uint32
(
    v UInt32,
    w UInt8,
    INDEX bf_v v TYPE bloom_filter GRANULARITY 1,
    INDEX bf_w w TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_uint32 SELECT toUInt32(toIPv4('127.0.0.1')) + number, number + 1 FROM numbers(2);

SELECT count() FROM t_bf_uint32 WHERE v = toIPv4('127.0.0.1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_uint32 WHERE v = toIPv4('127.0.0.1');
SELECT count() FROM t_bf_uint32 WHERE w = toIPv4('0.0.0.1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_uint32 WHERE w = toIPv4('0.0.0.1');
SELECT count() FROM t_bf_uint32 WHERE w = toIPv4('127.0.0.1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_uint32 WHERE w = toIPv4('127.0.0.1');

DROP TABLE t_bf_uint32;
