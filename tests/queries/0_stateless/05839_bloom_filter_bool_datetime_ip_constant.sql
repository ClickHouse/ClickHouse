-- A `bloom_filter` index hashes the constant of `equals`, so the constant has to be brought to the value
-- that the comparison uses. Each query is checked against the `use_skip_indexes = 0` oracle.

-- `Bool` is stored as `UInt8` and can hold a byte other than 0 and 1. `equals` compares it with an
-- integer numerically, so the constant 2 must not be clamped to `true` before it is hashed.
DROP TABLE IF EXISTS t_bf_bool_map;
CREATE TABLE t_bf_bool_map (m Map(String, Bool), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_bool_map SELECT map('k', reinterpret(toUInt8(2), 'Bool'));
INSERT INTO t_bf_bool_map SELECT map('k', true);

SELECT 'Bool map values';
SELECT count() FROM t_bf_bool_map WHERE m['k'] = toInt64(2) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool_map WHERE m['k'] = toInt64(2);
SELECT count() FROM t_bf_bool_map WHERE m['k'] = toInt64(2) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bf_bool_map WHERE m['k'] = toUInt8(2);
SELECT count() FROM t_bf_bool_map WHERE m['k'] = 2.;
SELECT count() FROM t_bf_bool_map WHERE m['k'] = 256 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool_map WHERE m['k'] = 256;
SELECT count() FROM t_bf_bool_map WHERE m['k'] = -1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool_map WHERE m['k'] = -1;
SELECT count() FROM t_bf_bool_map WHERE m['k'] = true SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool_map WHERE m['k'] = true;
-- The index is still used: only the granule with the byte 2 is read.
SELECT DISTINCT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_bool_map WHERE m['k'] = toInt64(2)) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_bf_bool_map;

DROP TABLE IF EXISTS t_bf_bool;
CREATE TABLE t_bf_bool (b Bool, INDEX bf b TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_bool SELECT reinterpret(toUInt8(2), 'Bool');
INSERT INTO t_bf_bool SELECT true;

SELECT 'Bool column';
SELECT count() FROM t_bf_bool WHERE b = toInt64(2) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool WHERE b = toInt64(2);
SELECT count() FROM t_bf_bool WHERE b = 256 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool WHERE b = 256;
SELECT DISTINCT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_bool WHERE b = toInt64(2)) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_bf_bool;

-- `equals` compares a `DateTime` with a `Float64` or a wide integer numerically; the index must not
-- raise `TYPE_MISMATCH` for a query that succeeds without it.
DROP TABLE IF EXISTS t_bf_datetime_map;
CREATE TABLE t_bf_datetime_map (m Map(String, DateTime), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_datetime_map SELECT map('k', toDateTime(1));
INSERT INTO t_bf_datetime_map SELECT map('k', toDateTime(5));

SELECT 'DateTime map values';
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = toFloat64(1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = toFloat64(1);
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = 1.5 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = 1.5;
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = -1. SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = -1.;
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = toUInt128(1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = toUInt128(1);
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = toInt128(1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime_map WHERE m['k'] = toInt128(1);
SELECT DISTINCT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_datetime_map WHERE m['k'] = toFloat64(1)) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_bf_datetime_map;

DROP TABLE IF EXISTS t_bf_datetime;
CREATE TABLE t_bf_datetime (d DateTime, INDEX bf d TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_datetime SELECT toDateTime(1);
INSERT INTO t_bf_datetime SELECT toDateTime(5);

SELECT 'DateTime column';
SELECT count() FROM t_bf_datetime WHERE d = toFloat64(1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime WHERE d = toFloat64(1);
SELECT count() FROM t_bf_datetime WHERE d = toUInt128(1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime WHERE d = toUInt128(1);
SELECT DISTINCT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_datetime WHERE d = toFloat64(1)) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_bf_datetime;

-- `equals` compares an `IPv4` with an `IPv6` as the IPv4-mapped address.
DROP TABLE IF EXISTS t_bf_ipv6_map;
CREATE TABLE t_bf_ipv6_map (m Map(String, IPv6), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_ipv6_map SELECT map('k', toIPv6('::ffff:127.0.0.1'));
INSERT INTO t_bf_ipv6_map SELECT map('k', toIPv6('::1'));

SELECT 'IPv6 map values';
SELECT count() FROM t_bf_ipv6_map WHERE m['k'] = toIPv4('127.0.0.1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_ipv6_map WHERE m['k'] = toIPv4('127.0.0.1');
SELECT count() FROM t_bf_ipv6_map WHERE m['k'] = toIPv6('::ffff:127.0.0.1');
SELECT DISTINCT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_ipv6_map WHERE m['k'] = toIPv6('::ffff:127.0.0.1')) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_bf_ipv6_map;

DROP TABLE IF EXISTS t_bf_ipv4_map;
CREATE TABLE t_bf_ipv4_map (m Map(String, IPv4), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_ipv4_map SELECT map('k', toIPv4('127.0.0.1'));
INSERT INTO t_bf_ipv4_map SELECT map('k', toIPv4('127.0.0.2'));

SELECT 'IPv4 map values';
SELECT count() FROM t_bf_ipv4_map WHERE m['k'] = toIPv6('::ffff:127.0.0.1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_ipv4_map WHERE m['k'] = toIPv6('::ffff:127.0.0.1');
SELECT count() FROM t_bf_ipv4_map WHERE m['k'] = toIPv4('127.0.0.1');
SELECT DISTINCT extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_ipv4_map WHERE m['k'] = toIPv4('127.0.0.1')) WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_bf_ipv4_map;

DROP TABLE IF EXISTS t_bf_ip;
CREATE TABLE t_bf_ip (a4 IPv4, a6 IPv6, INDEX bf4 a4 TYPE bloom_filter GRANULARITY 1, INDEX bf6 a6 TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_ip SELECT toIPv4('127.0.0.1'), toIPv6('::ffff:127.0.0.1');
INSERT INTO t_bf_ip SELECT toIPv4('127.0.0.2'), toIPv6('::1');

SELECT 'IP columns';
SELECT count() FROM t_bf_ip WHERE a4 = toIPv6('::ffff:127.0.0.1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_ip WHERE a4 = toIPv6('::ffff:127.0.0.1');
SELECT count() FROM t_bf_ip WHERE a6 = toIPv4('127.0.0.1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_ip WHERE a6 = toIPv4('127.0.0.1');

DROP TABLE t_bf_ip;
