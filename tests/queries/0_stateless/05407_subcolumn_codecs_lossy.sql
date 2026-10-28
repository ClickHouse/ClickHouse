-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library.

SET enable_subcolumn_codecs = 1;
SET enable_sz3_codec = 1;

DROP TABLE IF EXISTS t_subcolumn_codecs_lossy;

-- A lossy codec is not allowed for a Map or for its keys, which it would corrupt; the values of a Map may be lossy.
CREATE TABLE t_subcolumn_codecs_lossy (x Tuple(k Float64, m Map(Float64, Float64)), CODEC x.m (SZ3('ALGO_INTERP', 'REL', 0.01))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_lossy (m Map(Float64, Float64), CODEC m.keys (SZ3('ALGO_INTERP', 'REL', 0.01))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_lossy (x Tuple(k Float64, m Map(Float64, Float64)), CODEC x.m.values (SZ3('ALGO_INTERP', 'REL', 0.01))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_subcolumn_codecs_lossy SELECT (number, map(number, number / 7)) FROM numbers(100);
SELECT count(), sum(x.k), sum(length(x.m)) FROM t_subcolumn_codecs_lossy;
DROP TABLE t_subcolumn_codecs_lossy;

-- A lossy codec is not allowed on a stream of the sorting key.
CREATE TABLE t_subcolumn_codecs_lossy (x Tuple(k Float64, v Float64), CODEC x.k (SZ3('ALGO_INTERP', 'REL', 0.01))) ENGINE = MergeTree ORDER BY x.k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_lossy (x Tuple(k Float64, v Float64), CODEC x (SZ3('ALGO_INTERP', 'REL', 0.01))) ENGINE = MergeTree ORDER BY x.k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_lossy (x Tuple(t Tuple(a Float64, b Float64), v Float64), CODEC x.t.a (SZ3('ALGO_INTERP', 'REL', 0.01))) ENGINE = MergeTree ORDER BY x.t; -- { serverError BAD_ARGUMENTS }

-- A sibling of a key subcolumn may be lossy.
CREATE TABLE t_subcolumn_codecs_lossy (x Tuple(k Float64, v Float64), CODEC x.v (SZ3('ALGO_INTERP', 'REL', 0.01))) ENGINE = MergeTree ORDER BY x.k;
ALTER TABLE t_subcolumn_codecs_lossy MODIFY COLUMN x.k CODEC(SZ3('ALGO_INTERP', 'REL', 0.01)); -- { serverError BAD_ARGUMENTS }
INSERT INTO t_subcolumn_codecs_lossy SELECT (number, number / 7) FROM numbers(1000);
-- The error of the lossy codec is relative to the range of the values (about 143), so it stays below 2.
SELECT count(), sum(x.k), min(abs(x.v - x.k / 7) <= 2) FROM t_subcolumn_codecs_lossy;

DROP TABLE t_subcolumn_codecs_lossy;
