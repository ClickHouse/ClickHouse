-- Tags: no-fasttest
-- The condition of a partial index is evaluated on the values before compression. With a lossy codec on a column
-- the condition reads, the stored values can fall on the other side of it, and the index would drop rows the query
-- matches: such a condition is rejected when the index is defined and when the codec is added.

SET allow_experimental_partial_skip_indexes = 1;
SET enable_sz3_codec = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_lossy;
DROP TABLE IF EXISTS t_lossy_alter;
DROP TABLE IF EXISTS t_lossless;

-- A new table.
CREATE TABLE t_lossy (id UInt64, p Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.1)), INDEX i id TYPE minmax GRANULARITY 1 WHERE p > 0.5)
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- A subcolumn is read through the stream of its column.
CREATE TABLE t_lossy (id UInt64, p Array(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.1)), INDEX i id TYPE minmax GRANULARITY 1 WHERE p.size0 > 1)
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- Adding the index to a table that has the codec.
CREATE TABLE t_lossy (id UInt64, p Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.1))) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_lossy ADD INDEX i id TYPE minmax GRANULARITY 1 WHERE p > 0.5; -- { serverError BAD_ARGUMENTS }
-- The index does not read the column: the codec is fine.
ALTER TABLE t_lossy ADD INDEX j id TYPE minmax GRANULARITY 1 WHERE id > 5;

-- Adding the codec to a column the condition reads.
CREATE TABLE t_lossy_alter (id UInt64, p Float64, INDEX i id TYPE minmax GRANULARITY 1 WHERE p > 0.5) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_lossy_alter MODIFY COLUMN p Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.1)); -- { serverError BAD_ARGUMENTS }

-- Without the codec the combination works: every read mode returns what the unindexed query does.
CREATE TABLE t_lossless (id UInt64, p Float64, INDEX i id TYPE minmax GRANULARITY 1 WHERE p > 0.5)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_lossless SELECT number, sin(number * 0.1) FROM numbers(1000);
SELECT count() FROM t_lossless WHERE p > 0.5 AND id < 1000 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_lossless WHERE p > 0.5 AND id < 1000 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_lossless WHERE p > 0.5 AND id < 1000 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

DROP TABLE t_lossy;
DROP TABLE t_lossy_alter;
DROP TABLE t_lossless;
