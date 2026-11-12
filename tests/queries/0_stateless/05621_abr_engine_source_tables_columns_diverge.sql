-- The source tables of an `ABR` table must have the same columns. This is checked on every query, not only when the table
-- is created: a source table altered since is rejected, instead of having its result converted to the columns of the finest one.

SET allow_experimental_abr = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;
DROP TABLE IF EXISTS src_10;

CREATE TABLE src_1 (ts DateTime, value Float64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE src_10 (ts DateTime, value Float64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 60, 0.5 FROM numbers(1000);
INSERT INTO src_10 SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 600, 5.7 FROM numbers(100);

CREATE TABLE abr ENGINE = ABR('src', [1, 10], ('ts'));

-- The finest table exceeds its share of the limit, so the query is retried on the coarse one.
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00', 'UTC') SETTINGS max_rows_to_read = 1000;

-- A different type: the result of the coarse table would be converted from `Float64` to `UInt64`, giving 5 instead of 5.7.
ALTER TABLE src_1 MODIFY COLUMN value UInt64;
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00', 'UTC') SETTINGS max_rows_to_read = 1000; -- { serverError BAD_ARGUMENTS }
-- Rejected even if the query would not be retried on the table that differs.
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00', 'UTC'); -- { serverError BAD_ARGUMENTS }
-- A table outside the range of the candidates is not checked.
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00', 'UTC') SETTINGS abr_last_table_suffix = 1;

ALTER TABLE src_1 MODIFY COLUMN value Float64;
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00', 'UTC') SETTINGS max_rows_to_read = 1000;

-- A column that only one source table has.
ALTER TABLE src_10 ADD COLUMN extra String;
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00', 'UTC'); -- { serverError BAD_ARGUMENTS }

ALTER TABLE src_1 ADD COLUMN extra String;
SELECT max(value), any(extra) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00', 'UTC');

DROP TABLE abr;
DROP TABLE src_1;
DROP TABLE src_10;
