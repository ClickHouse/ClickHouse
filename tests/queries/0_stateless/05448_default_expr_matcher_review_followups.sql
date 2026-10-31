-- Regression tests for column matchers in default expressions.

DROP TABLE IF EXISTS t_05448_partition;
DROP TABLE IF EXISTS t_05448_readd;
DROP TABLE IF EXISTS t_05448_lambda;
DROP TABLE IF EXISTS ts_05448;
DROP TABLE IF EXISTS ext_tags_05448;

SELECT '-- partition key reads a subcolumn of a rematerialized column';
CREATE TABLE t_05448_partition
(
    a UInt64,
    m Tuple(x UInt64) MATERIALIZED tuple(greatest(a, * EXCEPT m))
) ENGINE = MergeTree PARTITION BY m.x % 2 ORDER BY a;
INSERT INTO t_05448_partition (a) VALUES (0);
ALTER TABLE t_05448_partition ADD COLUMN b UInt64 DEFAULT a + 1 SETTINGS alter_sync = 2, mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT a, m.x FROM t_05448_partition;

SELECT '-- a re-added column is not converted together with another column';
CREATE TABLE t_05448_readd (id UInt64, val UInt64, c UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_05448_readd VALUES (1, 2, 3);
SYSTEM STOP MERGES t_05448_readd;
ALTER TABLE t_05448_readd DROP COLUMN c SETTINGS alter_sync = 0;
ALTER TABLE t_05448_readd ADD COLUMN c UInt32 SETTINGS alter_sync = 0;
ALTER TABLE t_05448_readd MODIFY COLUMN val UInt32 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT val, c FROM t_05448_readd;

SELECT '-- a field access of a lambda parameter is not a dotted ALIAS column';
CREATE TABLE t_05448_lambda
(
    x UInt8,
    `x.id` UInt8 ALIAS x + 1,
    arr Array(Tuple(id UInt8)),
    m Array(UInt8) MATERIALIZED arrayMap(x -> x.id, arr)
) ENGINE = Memory;
INSERT INTO t_05448_lambda (x, arr) VALUES (10, [(1), (2)]);
SELECT m, `x.id` FROM t_05448_lambda;

SELECT '-- a matcher in the DEFAULT of `id` of an external tags table';
SET allow_experimental_time_series_table = 1;
CREATE TABLE ext_tags_05448
(
    id UInt64 DEFAULT toUInt64(length(COLUMNS('^tags$'))),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = ReplacingMergeTree ORDER BY (metric_name, id);
-- External target tables are supported up to version 9.
CREATE TABLE ts_05448 ENGINE = TimeSeries SETTINGS version = 8 TAGS ext_tags_05448;
INSERT INTO ts_05448 (metric_name, tags, samples) VALUES ('m1', {'job': 'j1'}, [(toDateTime64(1000, 3), 1.5)]);
SELECT id, metric_name, tags FROM ext_tags_05448;

DROP TABLE ts_05448;
DROP TABLE ext_tags_05448;
DROP TABLE t_05448_lambda;
DROP TABLE t_05448_readd;
DROP TABLE t_05448_partition;
