-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- Data parts in Parquet format: inserts, reads, merges, and the primary key.

DROP TABLE IF EXISTS t_parquet;
DROP TABLE IF EXISTS t_reference;

CREATE TABLE t_parquet
(
    key UInt64,
    s String,
    arr Array(UInt32),
    n Nullable(Int32),
    d DateTime64(3, 'UTC'),
    lc LowCardinality(String),
    m Map(String, UInt64),
    t Tuple(a Int32, b String)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 1000;

CREATE TABLE t_reference AS t_parquet ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_parquet SELECT number, toString(number), range(number % 5), if(number % 3 = 0, NULL, number), toDateTime64(number, 3, 'UTC'), toString(number % 7), map(toString(number % 3), number), (number, toString(number * 2)) FROM numbers(10000);
INSERT INTO t_reference SELECT * FROM t_parquet;

SELECT 'parts';
SELECT part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet' AND active;

SELECT 'same data';
SELECT count() FROM (SELECT * FROM t_parquet EXCEPT SELECT * FROM t_reference);
SELECT count() FROM (SELECT * FROM t_reference EXCEPT SELECT * FROM t_parquet);
SELECT count(), sum(key), sum(length(s)), sum(length(arr)), countIf(n IS NULL), uniqExact(lc) FROM t_parquet;

SELECT 'point query';
SELECT * FROM t_parquet WHERE key = 5555;
SELECT key, s FROM t_parquet WHERE key BETWEEN 4998 AND 5002 ORDER BY key;

SELECT 'subcolumns';
SELECT arr.size0, n.null, m.keys, m.values, t.a, t.b FROM t_parquet WHERE key IN (6, 7) ORDER BY key;

SELECT 'primary key';
SELECT count() FROM t_parquet WHERE key < 500 SETTINGS max_rows_to_read = 1000;
SELECT count() FROM t_parquet WHERE key >= 9500 SETTINGS max_rows_to_read = 1000;

SELECT 'small blocks';
SELECT sum(key), count() FROM t_parquet WHERE key % 10 = 3 SETTINGS max_block_size = 77, max_threads = 3;

SELECT 'merge';
INSERT INTO t_parquet SELECT number + 10000, toString(number), [], NULL, toDateTime64(number, 3, 'UTC'), '', map(), (0, '') FROM numbers(2500);
OPTIMIZE TABLE t_parquet FINAL;
SELECT part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet' AND active;
SELECT count(), sum(key) FROM t_parquet;

DROP TABLE t_parquet;
DROP TABLE t_reference;
