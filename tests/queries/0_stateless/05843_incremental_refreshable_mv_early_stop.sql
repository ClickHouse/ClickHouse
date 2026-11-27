-- Tags: atomic-database
-- An incremental refresh resumes from the cursor that the bounded read of the previous refresh reports.
-- A query can stop the read before its end when the rest of the rows cannot change the result (a `DISTINCT` by
-- constants, a `LIMIT BY` without keys): the read still reports the cursor after all the rows, so the next refresh
-- does not get them again. A query that returns a partial result (the overflow mode `break`) is rejected, because
-- the rows that were not read would be lost.
-- The source is larger than one chunk of the read, otherwise the read ends before it is stopped.

DROP TABLE IF EXISTS es_src;
DROP TABLE IF EXISTS es_tgt;
DROP TABLE IF EXISTS es_mv;

CREATE TABLE es_src (k UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE es_tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO es_src SELECT number FROM numbers(100000);

SELECT '-- LIMIT BY without keys';
CREATE MATERIALIZED VIEW es_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO es_tgt EMPTY
    AS SELECT k FROM es_src LIMIT 1 BY tuple()
    SETTINGS max_block_size = 1, max_threads = 1, exact_rows_before_limit = 0;
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv;
SELECT count() FROM es_tgt;
-- No new rows in the source: nothing is appended.
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv;
SELECT count() FROM es_tgt;
-- Only the new rows are read.
INSERT INTO es_src SELECT number FROM numbers(100000, 100000);
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv;
SELECT count(), countIf(k >= 100000) FROM es_tgt;
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv;
SELECT count(), countIf(k >= 100000) FROM es_tgt;
DROP TABLE es_mv;
TRUNCATE TABLE es_tgt;

SELECT '-- DISTINCT by constants';
CREATE MATERIALIZED VIEW es_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO es_tgt EMPTY
    AS SELECT DISTINCT toUInt64(1) AS k FROM es_src
    SETTINGS max_block_size = 1, max_threads = 1;
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv;
SELECT count() FROM es_tgt;
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv;
SELECT count() FROM es_tgt;
INSERT INTO es_src SELECT number FROM numbers(200000, 100000);
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv;
SELECT count() FROM es_tgt;
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv;
SELECT count() FROM es_tgt;
DROP TABLE es_mv;
TRUNCATE TABLE es_tgt;

SELECT '-- The overflow mode break';
CREATE MATERIALIZED VIEW es_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO es_tgt EMPTY
    AS SELECT k FROM es_src
    SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break', max_block_size = 1, max_threads = 1; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW es_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO es_tgt EMPTY
    AS SELECT DISTINCT k FROM es_src
    SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'break', max_block_size = 1, max_threads = 1; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW es_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO es_tgt EMPTY
    AS SELECT k FROM es_src
    SETTINGS max_result_rows = 1, result_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW es_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO es_tgt EMPTY
    AS SELECT k FROM es_src
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
SET read_overflow_mode = 'break';
CREATE MATERIALIZED VIEW es_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO es_tgt EMPTY
    AS SELECT k FROM es_src; -- { serverError NOT_IMPLEMENTED }
SET read_overflow_mode = 'throw';

SELECT '-- The overflow mode throw';
-- The refresh fails and nothing is appended.
CREATE MATERIALIZED VIEW es_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO es_tgt EMPTY
    AS SELECT k FROM es_src SETTINGS max_rows_to_read = 1;
SYSTEM REFRESH VIEW es_mv;
SYSTEM WAIT VIEW es_mv; -- { serverError REFRESH_FAILED }
SELECT count() FROM es_tgt;

DROP TABLE es_mv;
DROP TABLE es_tgt;
DROP TABLE es_src;
