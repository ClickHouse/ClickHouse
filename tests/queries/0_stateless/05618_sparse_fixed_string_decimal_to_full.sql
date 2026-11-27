-- Reading sparse `FixedString`, `Decimal` and `DateTime64` columns (also inside `Nullable`) as full ones, and functions
-- over them, give the same result as for columns that are not sparse: values in the first and last rows, runs of
-- adjacent values, a part without values, and functions whose result for the default value is not the default.

DROP TABLE IF EXISTS t_sparse_fd;
DROP TABLE IF EXISTS t_full_fd;

CREATE TABLE t_sparse_fd (id UInt64, fs FixedString(5), d32 Decimal32(2), d64 Decimal64(4), d128 Decimal128(10), d256 Decimal256(20), dt DateTime64(3, 'UTC'), nfs Nullable(FixedString(5)), nd Nullable(Decimal64(4)))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, serialization_info_version = 'with_types', nullable_serialization_version = 'allow_sparse';

CREATE TABLE t_full_fd (id UInt64, fs FixedString(5), d32 Decimal32(2), d64 Decimal64(4), d128 Decimal128(10), d256 Decimal256(20), dt DateTime64(3, 'UTC'), nfs Nullable(FixedString(5)), nd Nullable(Decimal64(4)))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_sparse_fd;

INSERT INTO t_sparse_fd SELECT
    number,
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toFixedString(toString(number), 5), ''),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toDecimal32(number, 2) + toDecimal32('0.25', 2), 0),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toDecimal64(number, 4) - toDecimal64('0.1234', 4), 0),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toDecimal128(number, 10) * 1000000, 0),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toDecimal256(number, 20) + toDecimal256('0.00000000000000000001', 20), 0),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, fromUnixTimestamp64Milli(toInt64(number * 1000 + 123), 'UTC'), fromUnixTimestamp64Milli(toInt64(0), 'UTC')),
    multiIf(number % 11 = 0 OR number = 999, toFixedString(toString(number), 5), number % 13 = 0, toFixedString('', 5), NULL),
    multiIf(number % 11 = 0 OR number = 999, toDecimal64(number, 4), number % 13 = 0, toDecimal64(0, 4), NULL)
FROM numbers(1000);
INSERT INTO t_sparse_fd (id) SELECT number FROM numbers(1000, 500);

INSERT INTO t_full_fd SELECT
    number,
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toFixedString(toString(number), 5), ''),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toDecimal32(number, 2) + toDecimal32('0.25', 2), 0),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toDecimal64(number, 4) - toDecimal64('0.1234', 4), 0),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toDecimal128(number, 10) * 1000000, 0),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, toDecimal256(number, 20) + toDecimal256('0.00000000000000000001', 20), 0),
    if(number % 7 = 0 OR number = 999 OR number BETWEEN 500 AND 520, fromUnixTimestamp64Milli(toInt64(number * 1000 + 123), 'UTC'), fromUnixTimestamp64Milli(toInt64(0), 'UTC')),
    multiIf(number % 11 = 0 OR number = 999, toFixedString(toString(number), 5), number % 13 = 0, toFixedString('', 5), NULL),
    multiIf(number % 11 = 0 OR number = 999, toDecimal64(number, 4), number % 13 = 0, toDecimal64(0, 4), NULL)
FROM numbers(1000);
INSERT INTO t_full_fd (id) SELECT number FROM numbers(1000, 500);

SELECT table, column, groupUniqArray(serialization_kind)
FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_sparse_fd', 't_full_fd') AND column != 'id' AND active
GROUP BY table, column
ORDER BY table, column;

SELECT id, hex(fs), d32, d64, d128, d256, dt, hex(nfs), nd FROM t_sparse_fd WHERE id IN (0, 1, 7, 499, 500, 510, 520, 521, 998, 999, 1000, 1499) ORDER BY id;

SELECT 'read', (SELECT sum(cityHash64(id, fs, d32, d64, d128, d256, dt)) FROM t_sparse_fd) = (SELECT sum(cityHash64(id, fs, d32, d64, d128, d256, dt)) FROM t_full_fd);
SELECT 'nullable', (SELECT (sum(cityHash64(id, nfs)), sum(cityHash64(id, nd))) FROM t_sparse_fd) = (SELECT (sum(cityHash64(id, nfs)), sum(cityHash64(id, nd))) FROM t_full_fd);
SELECT 'fixed_string_not', (SELECT sum(cityHash64(id, bitNot(fs))) FROM t_sparse_fd) = (SELECT sum(cityHash64(id, bitNot(fs))) FROM t_full_fd);
SELECT 'decimal_plus', (SELECT sum(cityHash64(id, d32 - 1, d64 + 1, d128 * 2 + 1, d256 + 1)) FROM t_sparse_fd) = (SELECT sum(cityHash64(id, d32 - 1, d64 + 1, d128 * 2 + 1, d256 + 1)) FROM t_full_fd);
SELECT 'datetime64_plus', (SELECT sum(cityHash64(id, dt + toIntervalSecond(1))) FROM t_sparse_fd) = (SELECT sum(cityHash64(id, dt + toIntervalSecond(1))) FROM t_full_fd);
SELECT 'nullable_ifnull', (SELECT sum(cityHash64(id, ifNull(nd, 1), ifNull(nfs, toFixedString('x', 5)))) FROM t_sparse_fd) = (SELECT sum(cityHash64(id, ifNull(nd, 1), ifNull(nfs, toFixedString('x', 5)))) FROM t_full_fd);
SELECT 'to_nullable', (SELECT sum(cityHash64(id, assumeNotNull(toNullable(d64)), assumeNotNull(toNullable(fs)))) FROM t_sparse_fd) = (SELECT sum(cityHash64(id, assumeNotNull(toNullable(d64)), assumeNotNull(toNullable(fs)))) FROM t_full_fd);
SELECT 'group_by', (SELECT sum(cityHash64(fs, d64, dt, isNull(nfs), assumeNotNull(nfs), isNull(nd), assumeNotNull(nd), c)) FROM (SELECT fs, d64, dt, nfs, nd, count() AS c FROM t_sparse_fd GROUP BY fs, d64, dt, nfs, nd)) = (SELECT sum(cityHash64(fs, d64, dt, isNull(nfs), assumeNotNull(nfs), isNull(nd), assumeNotNull(nd), c)) FROM (SELECT fs, d64, dt, nfs, nd, count() AS c FROM t_full_fd GROUP BY fs, d64, dt, nfs, nd));
SELECT 'join', count() FROM t_sparse_fd AS a INNER JOIN t_full_fd AS b ON a.id = b.id AND a.fs = b.fs AND a.d64 = b.d64 AND a.d256 = b.d256 AND a.dt = b.dt;

DROP TABLE t_sparse_fd;
DROP TABLE t_full_fd;
