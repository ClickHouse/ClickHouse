-- Tags: atomic-database
-- An incremental refresh resumes from the cursor that the bounded read reports at its end. A query that stops the read
-- earlier and still succeeds would leave a cursor that does not match the appended rows: such definitions are rejected.

DROP TABLE IF EXISTS lim_src;
DROP TABLE IF EXISTS lim_tgt;
DROP TABLE IF EXISTS lim_mv;

CREATE TABLE lim_src (k UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE lim_tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1, 10; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src OFFSET 1; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS limit = 1; -- { serverError NOT_IMPLEMENTED }

-- `LIMIT BY` stops the read once no more rows can pass it, which is the case when all its keys are constants.
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1 BY 1 + 1; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1 BY k; -- { serverError NOT_IMPLEMENTED }

-- A limit with the overflow mode `break` finishes the query successfully with a partial result, while the cursor
-- of a `MergeTree` read advances to the end of the snapshot: the rows that were not read would be skipped forever.
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_rows_to_read = 100, read_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_bytes_to_read = 100, read_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_result_rows = 100, result_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_execution_time = 100, timeout_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT DISTINCT k FROM lim_src SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src GROUP BY k SETTINGS max_rows_to_group_by = 100, group_by_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }

-- The settings of the session that creates the view are checked as well.
SET max_rows_to_read = 100, read_overflow_mode = 'break';
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src; -- { serverError NOT_IMPLEMENTED }
SET max_rows_to_read = 0, read_overflow_mode = 'throw';

-- A limit that throws fails the refresh, which does not advance the cursor, so it remains allowed.
INSERT INTO lim_src SELECT number FROM numbers(1000);
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_rows_to_read = 100000, read_overflow_mode = 'throw';
SYSTEM REFRESH VIEW lim_mv;
SYSTEM WAIT VIEW lim_mv;
INSERT INTO lim_src VALUES (1000);
SYSTEM REFRESH VIEW lim_mv;
SYSTEM WAIT VIEW lim_mv;
SELECT count(), uniqExact(k), max(k) FROM lim_tgt;

DROP TABLE lim_mv;
DROP TABLE lim_tgt;
DROP TABLE lim_src;
