-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not built in the fast-test image.

-- `CAST` has no conversion from a wide integer into a `Decimal`, an `Enum` or `IPv4` (it is the other way
-- round that `IN` uses), and `accurateCastOrNull` answers NULL for every value then. Index analysis read
-- each such NULL as an element no key can match, built an empty set and skipped every granule, row group
-- or partition, so the queries below returned no rows.

SET engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_d64;
CREATE TABLE t_d64 (k Decimal64(4)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_d64 VALUES (1), (7);

SELECT 'Int128', k FROM t_d64 WHERE k IN (SELECT CAST('1', 'Int128')) ORDER BY k;
SELECT 'UInt128', k FROM t_d64 WHERE k IN (SELECT toUInt128(7)) ORDER BY k;
SELECT 'Int256', k FROM t_d64 WHERE k IN (SELECT toInt256(1)) ORDER BY k;
SELECT 'UInt256', k FROM t_d64 WHERE k IN (SELECT toUInt256(7)) ORDER BY k;
SELECT 'Nullable(Int128)', k FROM t_d64 WHERE k IN (SELECT CAST(1, 'Nullable(Int128)')) ORDER BY k;

DROP TABLE IF EXISTS t_d128_0;
CREATE TABLE t_d128_0 (k Decimal128(0)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_d128_0 VALUES (1), (7);

SELECT 'Decimal128(0)', k FROM t_d128_0 WHERE k IN (SELECT toInt128(1)) ORDER BY k;

DROP TABLE IF EXISTS t_part;
CREATE TABLE t_part (d Decimal32(2), v UInt8) ENGINE = MergeTree PARTITION BY d ORDER BY v;
INSERT INTO t_part VALUES (1, 1), (7, 7);

SELECT 'partition key', v FROM t_part WHERE d IN (SELECT toInt128(7)) ORDER BY v;

DROP TABLE IF EXISTS t_skip;
CREATE TABLE t_skip (id UInt64, d Decimal64(4), INDEX i_mm d TYPE minmax GRANULARITY 1, INDEX i_set d TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_skip VALUES (1, 1), (2, 7);

SELECT 'minmax index', id FROM t_skip WHERE d IN (SELECT toInt128(1)) ORDER BY id SETTINGS ignore_data_skipping_indices = 'i_set';
SELECT 'set index', id FROM t_skip WHERE d IN (SELECT toInt128(1)) ORDER BY id SETTINGS ignore_data_skipping_indices = 'i_mm';

DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (d Decimal32(0), s String) ENGINE = MergeTree ORDER BY (d, s) SETTINGS index_granularity = 1;
INSERT INTO t_tuple VALUES (1, 'a'), (7, 'b');

SELECT 'tuple key', s FROM t_tuple WHERE (d, s) IN (SELECT toInt128(1), 'a') ORDER BY s;

DROP TABLE IF EXISTS t_enum;
CREATE TABLE t_enum (k Enum8('a' = 1, 'b' = 7)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_enum VALUES ('a'), ('b');

SELECT 'Enum8 Int128', k FROM t_enum WHERE k IN (SELECT toInt128(1)) ORDER BY k;
SELECT 'Enum8 Date', k FROM t_enum WHERE k IN (SELECT toDate(7)) ORDER BY k;

DROP TABLE IF EXISTS t_ipv4;
CREATE TABLE t_ipv4 (k IPv4) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_ipv4 VALUES (toIPv4(1)), (toIPv4(7));

SELECT 'IPv4 UInt128', k FROM t_ipv4 WHERE k IN (SELECT toUInt128(1)) ORDER BY k;

INSERT INTO FUNCTION file(currentDatabase() || '_05615.parquet', Parquet, 'k Decimal64(4)')
SELECT toDecimal64(arrayJoin([1, 7]), 4) SETTINGS output_format_parquet_row_group_size = 1, output_format_parquet_write_bloom_filter = 1;

SELECT 'Parquet', k FROM file(currentDatabase() || '_05615.parquet', Parquet, 'k Decimal64(4)') WHERE k IN (SELECT toInt128(1)) ORDER BY k;

DROP TABLE t_d64;
DROP TABLE t_d128_0;
DROP TABLE t_part;
DROP TABLE t_skip;
DROP TABLE t_tuple;
DROP TABLE t_enum;
DROP TABLE t_ipv4;
