-- Tags: atomic-database
-- An incremental refresh persists the cursor that the bounded read of the source reports when it finishes.
-- The refresh must not succeed if the read was cut short: otherwise the rows that were not read are skipped
-- forever, or the rows that were read are appended again by every next refresh.

DROP TABLE IF EXISTS incr_incomplete_mv;
DROP TABLE IF EXISTS incr_incomplete_src;
DROP TABLE IF EXISTS incr_incomplete_tgt;

CREATE TABLE incr_incomplete_src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 1;
CREATE TABLE incr_incomplete_tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO incr_incomplete_src SELECT number FROM numbers(100);

-- A limit on the amount of data to read with the `break` overflow mode would make a truncated read look like
-- a completed one, and the cursor would move past the rows that were not read. The limit throws instead.
CREATE MATERIALIZED VIEW incr_incomplete_mv
    REFRESH EVERY 10 YEAR SETTINGS refresh_retries = 0 APPEND INCREMENTAL TO incr_incomplete_tgt EMPTY
    AS SELECT k FROM incr_incomplete_src
    SETTINGS max_block_size = 1, max_threads = 1, max_rows_to_read = 10, read_overflow_mode = 'break';
SYSTEM REFRESH VIEW incr_incomplete_mv;
SYSTEM WAIT VIEW incr_incomplete_mv; -- { serverError REFRESH_FAILED }
SELECT 'read limit', exception LIKE '%TOO_MANY_ROWS%' FROM system.view_refreshes
WHERE database = currentDatabase() AND view = 'incr_incomplete_mv';
DROP TABLE incr_incomplete_mv;

-- The query stops reading the source before the end of the bounded read (here the aggregation does not need
-- more data), so the read does not report the cursor. The refresh fails instead of keeping the previous cursor.
CREATE MATERIALIZED VIEW incr_incomplete_mv
    REFRESH EVERY 10 YEAR SETTINGS refresh_retries = 0 APPEND INCREMENTAL TO incr_incomplete_tgt EMPTY
    AS SELECT k FROM incr_incomplete_src GROUP BY k
    SETTINGS max_block_size = 1, max_threads = 1, max_rows_to_group_by = 1, group_by_overflow_mode = 'break';
SYSTEM REFRESH VIEW incr_incomplete_mv;
SYSTEM WAIT VIEW incr_incomplete_mv; -- { serverError REFRESH_FAILED }
SELECT 'closed source', exception LIKE '%stopped reading the source table%' FROM system.view_refreshes
WHERE database = currentDatabase() AND view = 'incr_incomplete_mv';

DROP TABLE incr_incomplete_mv;
DROP TABLE incr_incomplete_tgt;
DROP TABLE incr_incomplete_src;
