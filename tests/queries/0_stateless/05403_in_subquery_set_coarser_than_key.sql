-- Tags: no-parallel-replicas
-- no-parallel-replicas: the EXPLAIN checks read the local key analysis.

-- `IN (subquery)` casts the key into the subquery's type, while index analysis casts the subquery values into
-- the key type; when the first cast merges distinct keys the index must not prune (#124823).

CREATE TABLE t_dt (d DateTime('UTC')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dt VALUES ('2023-02-01 12:00:01'), ('2023-02-03 13:00:00');
SELECT count() FROM t_dt WHERE d IN (SELECT toDate('2023-02-01'));
SELECT count() FROM t_dt WHERE d NOT IN (SELECT toDate('2023-02-01'));

CREATE TABLE t_dt64 (d DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dt64 VALUES ('2023-02-01 12:00:00.000001'), ('2023-02-01 13:00:00');
SELECT count() FROM t_dt64 WHERE d IN (SELECT toDateTime64('2023-02-01 12:00:00', 3, 'UTC'));

CREATE TABLE t_dec (x Decimal64(3)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_dec VALUES (1.234), (2);
SELECT count() FROM t_dec WHERE x IN (SELECT toDecimal32(1.23, 2));

-- The AST fuzzer shape: `01891_not_in_partition_prune` with `t1.date` turned into a `DateTime`.
CREATE TABLE t1 (date DateTime('Asia/Tokyo'), a Float64, b String) ENGINE = MergeTree ORDER BY date SETTINGS index_granularity = 1;
CREATE TABLE t2 (date Date, a Float64, b String) ENGINE = MergeTree ORDER BY date;
INSERT INTO t1 (a, b) VALUES (1, 'one'), (2, 'two');
INSERT INTO t2 (a, b) VALUES (2, 'two'), (3, 'three');
SELECT a, b FROM t1 WHERE (date, a, b) IN (SELECT date, a, b FROM t2) ORDER BY a;
SELECT a, b FROM t1 WHERE NOT ((date, a, b) NOT IN (SELECT date, a, b FROM t2)) ORDER BY a;

CREATE TABLE t_part (d DateTime('UTC')) ENGINE = MergeTree PARTITION BY d ORDER BY tuple();
INSERT INTO t_part VALUES ('2023-02-01 12:00:01'), ('2023-02-03 13:00:00');
SELECT count() FROM t_part WHERE d IN (SELECT toDate('2023-02-01'));

CREATE TABLE t_hash (x Decimal(10, 4)) ENGINE = MergeTree PARTITION BY cityHash64(x) ORDER BY tuple();
INSERT INTO t_hash VALUES (1.2300), (1.2345), (2.0000);
SELECT x FROM t_hash WHERE x IN (SELECT toDecimal32(1.23, 2)) ORDER BY x;

CREATE TABLE t_skip (d DateTime('UTC'), INDEX i d TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_skip VALUES ('2023-02-01 12:00:01'), ('2023-02-03 13:00:00');
SELECT count() FROM t_skip WHERE d IN (SELECT toDate('2023-02-01')) SETTINGS use_skip_indexes_on_data_read = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_skip WHERE d IN (SELECT toDateTime('2023-02-01 12:00:01', 'UTC')) SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Granules: 1/2%';

CREATE TABLE t_stat (d DateTime('Asia/Tokyo'), b String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_string_columns = 1, auto_statistics_types = 'minmax';
INSERT INTO t_stat SETTINGS materialize_statistics_on_insert = 1 VALUES (0, 'two');
SELECT count() FROM t_stat WHERE (d, b) IN (SELECT toDate('1970-01-01'), 'two') SETTINGS use_statistics_for_part_pruning = 1, use_skip_indexes_on_data_read = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT b FROM t_stat WHERE (d, b) IN (SELECT toDateTime(1, 'Asia/Tokyo'), 'two') SETTINGS use_statistics_for_part_pruning = 1, use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Parts: 0/1%';

CREATE TABLE t_bool (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_bool VALUES (0), (2);
SELECT count() FROM t_bool WHERE x IN (SELECT true);

CREATE TABLE t_float (x Float64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_float VALUES (1.234), (2);
SELECT count() FROM t_float WHERE x IN (SELECT toDecimal32(1.23, 2));

CREATE TABLE t_u32 (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_u32 VALUES (1), (19389), (1675252801);
SELECT count() FROM t_u32 WHERE x IN (SELECT toDate('2023-02-01')) SETTINGS session_timezone = 'UTC';

CREATE TABLE t_d8 (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_d8 VALUES (1), (2), (257);
SELECT count() FROM t_d8 WHERE d IN (SELECT toUInt8(1));

CREATE TABLE t_f32 (d DateTime('UTC')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_f32 VALUES (1), (16777216), (16777217);
SELECT count() FROM t_f32 WHERE d IN (SELECT toFloat32(16777216));

CREATE TABLE t_dst (d DateTime('Europe/Berlin')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dst VALUES (1698539400), (1698543000), (1698546600);
SELECT count() FROM t_dst WHERE d IN (SELECT '2023-10-29 02:30:00');

CREATE TABLE t_arr (a Array(UInt16)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_arr VALUES ([0]), ([2]);
SELECT count() FROM t_arr WHERE a IN (SELECT [true]);

CREATE TABLE t_i64e (x Int64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_i64e VALUES (1), (257);
SELECT count() FROM t_i64e WHERE x IN (SELECT CAST(1, 'Enum8(''a'' = 1)'));

CREATE TABLE t_apia (d Date) ENGINE = MergeTree PARTITION BY d ORDER BY tuple();
INSERT INTO t_apia VALUES ('2011-12-29'), ('2011-12-30'), ('2011-12-31');
SELECT count() FROM t_apia WHERE d IN (SELECT toDateTime('2011-12-31 00:00:00', 'Pacific/Apia'));

CREATE TABLE t_n64 (d Nullable(DateTime64(3, 'UTC')), x UInt8, INDEX i d TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_n64 VALUES ('2023-02-01 12:00:00.500', 1), ('2023-02-03 13:00:00', 1);
SELECT count() FROM t_n64 WHERE (d, x) IN (SELECT toDateTime('2023-02-01 12:00:00', 'UTC'), 1) SETTINGS use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_n64 WHERE d IN (SELECT toDateTime('2023-02-01 12:00:00', 'UTC')) SETTINGS transform_null_in = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_n64 WHERE d IN (SELECT toDateTime('2023-02-01 12:00:00', 'UTC')) SETTINGS use_skip_indexes_on_data_read = 0;

CREATE TABLE t_tup64 (d DateTime64(3, 'UTC'), x UInt8) ENGINE = MergeTree ORDER BY (d, x) SETTINGS index_granularity = 1;
INSERT INTO t_tup64 VALUES ('2023-02-01 12:00:00.500', 1), ('2023-02-03 13:00:00', 1);
SELECT count() FROM t_tup64 WHERE (d, x) IN (SELECT tuple(toDateTime('2023-02-01 12:00:00', 'UTC'), toUInt8(1)));

-- A numeric `CAST` stores values beyond the declared precision.
CREATE TABLE t_dec6 (d Decimal(6, 0)) ENGINE = MergeTree PARTITION BY d ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dec6 SELECT CAST(16777216 + number, 'Decimal(6, 0)') FROM numbers(2);
SELECT count() FROM t_dec6 WHERE d IN (SELECT toFloat32(16777216));
CREATE TABLE t_dec10 (d Decimal(10, 0)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dec10 SELECT CAST(9007199254740992 + number, 'Decimal(10, 0)') FROM numbers(2);
SELECT count() FROM t_dec10 WHERE d IN (SELECT toFloat64(9007199254740992));

-- Formatting saturates an out-of-range `Date32` or `Time` and prints every nonzero `Bool` byte as `true`; `RowBinary` stores them.
CREATE TABLE t_d32s (d Date32) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_d32s SELECT * FROM format(RowBinary, 'd Date32', unhex('A0C02C00A1C02C00A2C02C00'));
SELECT count() FROM t_d32s WHERE d IN (SELECT '9999-12-31');
CREATE TABLE t_time (t Time) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO t_time SELECT * FROM format(RowBinary, 't Time', unhex('7FEE360080EE360081EE3600'));
SELECT count() FROM t_time WHERE t IN (SELECT '999:59:59');
CREATE TABLE t_boolb (b Bool) ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 1;
INSERT INTO t_boolb SELECT * FROM format(RowBinary, 'b Bool', unhex('000102'));
SELECT count() FROM t_boolb WHERE b IN (SELECT 'true');

-- A set whose cast merges nothing keeps pruning.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dt WHERE d IN (SELECT toDateTime('2023-02-01 12:00:01', 'UTC'))) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dt WHERE d IN (SELECT toDateTime64('2023-02-01 12:00:01', 3, 'UTC'))) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dt WHERE toDate(d) IN (SELECT toDate('2023-02-01'))) WHERE explain ILIKE '%Granules: 1/2%';
CREATE TABLE t_date (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_date VALUES ('2023-02-01'), ('2023-02-03');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_date WHERE d IN (SELECT toDate32('2023-02-01'))) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_date WHERE d IN (SELECT '2023-02-01')) WHERE explain ILIKE '%Granules: 1/2%';
CREATE TABLE t_dec9 (d Decimal(9, 0)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dec9 VALUES (1), (2);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dec9 WHERE d IN (SELECT toFloat64(1))) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT a FROM t_arr WHERE a IN (SELECT [toUInt8(0)])) WHERE explain ILIKE '%Granules: 1/2%';
CREATE TABLE t_enum (e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY e SETTINGS index_granularity = 1;
INSERT INTO t_enum VALUES ('a'), ('b');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT e FROM t_enum WHERE e IN (SELECT CAST('a', 'Enum8(''a'' = 1, ''b'' = 2, ''c'' = 3)'))) WHERE explain ILIKE '%Granules: 1/2%';
CREATE TABLE t_dt64s (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dt64s VALUES ('2023-02-01 12:00:00'), ('2023-02-01 13:00:00.500');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dt64s WHERE d IN (SELECT toDateTime('2023-02-01 12:00:00', 'UTC'))) WHERE explain ILIKE '%Granules: 1/2%';
CREATE TABLE t_wrap (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_wrap VALUES (1), (5);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_wrap WHERE k IN (SELECT CAST(toInt64(1), 'Dynamic'))) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_wrap WHERE k IN (SELECT CAST(toInt64(1), 'Variant(Int64, String)'))) WHERE explain ILIKE '%Granules: 1/2%';

DROP TABLE t_dt;
DROP TABLE t_dt64;
DROP TABLE t_dec;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t_part;
DROP TABLE t_hash;
DROP TABLE t_skip;
DROP TABLE t_stat;
DROP TABLE t_date;
DROP TABLE t_bool;
DROP TABLE t_float;
DROP TABLE t_dec9;
DROP TABLE t_u32;
DROP TABLE t_d8;
DROP TABLE t_f32;
DROP TABLE t_dst;
DROP TABLE t_arr;
DROP TABLE t_i64e;
DROP TABLE t_enum;
DROP TABLE t_apia;
DROP TABLE t_dt64s;
DROP TABLE t_wrap;
DROP TABLE t_n64;
DROP TABLE t_tup64;
DROP TABLE t_dec6;
DROP TABLE t_dec10;
DROP TABLE t_d32s;
DROP TABLE t_time;
DROP TABLE t_boolb;
