-- Tags: atomic-database
-- A query can stop reading its source before the bounded read of an incremental refresh finishes
-- (`LIMIT BY` and `DISTINCT` with constant keys, `LIMIT UNTIL`). The read then reports the end of its snapshot
-- as the cursor, so the next refresh does not read and append the same rows again.

DROP TABLE IF EXISTS incr_stop_src;
DROP TABLE IF EXISTS incr_stop_tgt_limit_by;
DROP TABLE IF EXISTS incr_stop_tgt_limit_until;
DROP TABLE IF EXISTS incr_stop_tgt_distinct;

CREATE TABLE incr_stop_src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';
CREATE TABLE incr_stop_tgt_limit_by (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE incr_stop_tgt_limit_until (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE incr_stop_tgt_distinct (k UInt64) ENGINE = MergeTree ORDER BY k;

-- Many blocks of the source, so that the query stops long before the source is read to the end.
INSERT INTO incr_stop_src SELECT number FROM numbers(1000000);

CREATE MATERIALIZED VIEW incr_stop_mv_limit_by
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_stop_tgt_limit_by EMPTY
    AS SELECT k FROM incr_stop_src LIMIT 1 BY tuple()
    SETTINGS max_block_size = 1, max_threads = 1;

CREATE MATERIALIZED VIEW incr_stop_mv_limit_until
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_stop_tgt_limit_until EMPTY
    AS SELECT k FROM incr_stop_src LIMIT UNTIL k = 1
    SETTINGS max_block_size = 1, max_threads = 1;

CREATE MATERIALIZED VIEW incr_stop_mv_distinct
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_stop_tgt_distinct EMPTY
    AS SELECT DISTINCT toUInt64(0) AS k FROM incr_stop_src
    SETTINGS max_block_size = 1, max_threads = 1;

-- Two refreshes without inserts in between append the rows once.
SYSTEM REFRESH VIEW incr_stop_mv_limit_by;
SYSTEM WAIT VIEW incr_stop_mv_limit_by;
SYSTEM REFRESH VIEW incr_stop_mv_limit_by;
SYSTEM WAIT VIEW incr_stop_mv_limit_by;
SELECT 'limit by', count(), min(k), max(k) FROM incr_stop_tgt_limit_by;

SYSTEM REFRESH VIEW incr_stop_mv_limit_until;
SYSTEM WAIT VIEW incr_stop_mv_limit_until;
SYSTEM REFRESH VIEW incr_stop_mv_limit_until;
SYSTEM WAIT VIEW incr_stop_mv_limit_until;
SELECT 'limit until', count(), min(k), max(k) FROM incr_stop_tgt_limit_until;

SYSTEM REFRESH VIEW incr_stop_mv_distinct;
SYSTEM WAIT VIEW incr_stop_mv_distinct;
SYSTEM REFRESH VIEW incr_stop_mv_distinct;
SYSTEM WAIT VIEW incr_stop_mv_distinct;
SELECT 'distinct', count(), min(k), max(k) FROM incr_stop_tgt_distinct;

-- The rows inserted after the refresh are read by the next one.
INSERT INTO incr_stop_src SELECT number FROM numbers(1000000, 10);
SYSTEM REFRESH VIEW incr_stop_mv_limit_by;
SYSTEM WAIT VIEW incr_stop_mv_limit_by;
SELECT 'limit by', count(), min(k), max(k) FROM incr_stop_tgt_limit_by;

DROP TABLE incr_stop_mv_limit_by;
DROP TABLE incr_stop_mv_limit_until;
DROP TABLE incr_stop_mv_distinct;
DROP TABLE incr_stop_tgt_limit_by;
DROP TABLE incr_stop_tgt_limit_until;
DROP TABLE incr_stop_tgt_distinct;
DROP TABLE incr_stop_src;
