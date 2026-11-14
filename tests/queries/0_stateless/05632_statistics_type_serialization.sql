-- The bounds of `minmax` and `basic` statistics are written with the serialization of the column type.
-- The estimates of a part are kept in memory when it is written, so `DETACH` / `ATTACH` makes them be read
-- from the statistics file, and a merge always reads the statistics of the parts it merges.

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_stats;

CREATE TABLE t_stats
(
    i8 Int8 STATISTICS(minmax, basic),
    u64 UInt64 STATISTICS(minmax, basic),
    i128 Int128 STATISTICS(minmax, basic),
    f32 Float32 STATISTICS(minmax, basic),
    f64 Float64 STATISTICS(minmax, basic),
    dec Decimal(18, 4) STATISTICS(minmax, basic),
    dec256 Decimal(60, 10) STATISTICS(minmax, basic),
    d Date STATISTICS(minmax, basic),
    d32 Date32 STATISTICS(minmax, basic),
    dt DateTime('UTC') STATISTICS(minmax, basic),
    dt64 DateTime64(3, 'UTC') STATISTICS(minmax, basic),
    e8 Enum8('a' = -1, 'b' = 5) STATISTICS(minmax, basic),
    e16 Enum16('x' = 1000, 'y' = 2000) STATISTICS(minmax, basic),
    b Bool STATISTICS(minmax, basic),
    ip4 IPv4 STATISTICS(minmax, basic),
    ip6 IPv6 STATISTICS(basic),
    lc LowCardinality(UInt32) STATISTICS(minmax, basic),
    n Nullable(Int32) STATISTICS(minmax, basic),
    n_all_null Nullable(Int32) STATISTICS(minmax, basic),
    lcn LowCardinality(Nullable(UInt16)) STATISTICS(minmax, basic)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '', materialize_statistics_on_merge = 1;

INSERT INTO t_stats SELECT
    toInt8(number) - 50,
    number * 1000000000000,
    toInt128(number) * toInt128('-1000000000000000000000'),
    number / 4,
    if(number = 3, nan, number / 8),
    toDecimal64(number, 4) / 3,
    toDecimal256(number, 10) / 7,
    toDate('2020-01-01') + number,
    toDate32('1950-01-01') + number,
    toDateTime('2020-01-01 00:00:00', 'UTC') + number,
    toDateTime64('2020-01-01 00:00:00.123', 3, 'UTC') + number,
    if(number % 2 = 0, 'a', 'b'),
    if(number % 2 = 0, 'x', 'y'),
    number % 3 = 0,
    toIPv4(number + 1),
    toIPv6(concat('::', toString(number + 1))),
    number + 7,
    if(number % 2 = 0, NULL, toInt32(number) - 10),
    NULL,
    if(number % 2 = 0, NULL, toUInt16(number + 100))
FROM numbers(100);

SELECT 'written';
SELECT column, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats' AND active ORDER BY column;

DETACH TABLE t_stats;
ATTACH TABLE t_stats;

SELECT 'read';
SELECT column, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats' AND active ORDER BY column;

DROP TABLE t_stats;

-- A merge combines the statistics read from one part with the statistics built from the rows of another one.
CREATE TABLE t_stats_merge
(
    b Bool STATISTICS(basic),
    e Enum8('a' = 1, 'b' = 2, 'c' = 3) STATISTICS(basic),
    dt64 DateTime64(3, 'UTC') STATISTICS(basic)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '', materialize_statistics_on_merge = 1;

INSERT INTO t_stats_merge SETTINGS materialize_statistics_on_insert = 0 VALUES (true, 'c', '2020-01-02 00:00:00.500');
INSERT INTO t_stats_merge VALUES (false, 'b', '2020-01-01 00:00:00.250'), (false, 'b', '2020-01-03 00:00:00.750');
OPTIMIZE TABLE t_stats_merge FINAL;
DETACH TABLE t_stats_merge;
ATTACH TABLE t_stats_merge;

SELECT 'merged';
SELECT column, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_merge' AND active ORDER BY column;

DROP TABLE t_stats_merge;

-- `ALTER` rewrites the statistics together with the column: a type change that rewrites the data rebuilds them,
-- and one that keeps the data keeps them in the part with its old type until a merge rewrites the part.
CREATE TABLE t_stats_alter
(
    i Int32 STATISTICS(basic),
    e Enum8('a' = 1, 'b' = 2) STATISTICS(basic),
    d Date STATISTICS(basic)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '', materialize_statistics_on_merge = 1;

INSERT INTO t_stats_alter SELECT number - 5, if(number % 2 = 0, 'a', 'b'), toDate('2020-01-01') + number FROM numbers(10);

ALTER TABLE t_stats_alter MODIFY COLUMN i Int64;
ALTER TABLE t_stats_alter MODIFY COLUMN e Enum8('a' = 1, 'b' = 2, 'c' = 3);
ALTER TABLE t_stats_alter MODIFY COLUMN d UInt16;
DETACH TABLE t_stats_alter;
ATTACH TABLE t_stats_alter;

SELECT 'altered';
SELECT column, type, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_alter' AND active ORDER BY column;

OPTIMIZE TABLE t_stats_alter FINAL;
DETACH TABLE t_stats_alter;
ATTACH TABLE t_stats_alter;

SELECT 'altered and merged';
SELECT column, type, `estimates.min`, `estimates.max` FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_alter' AND active ORDER BY column;

DROP TABLE t_stats_alter;
