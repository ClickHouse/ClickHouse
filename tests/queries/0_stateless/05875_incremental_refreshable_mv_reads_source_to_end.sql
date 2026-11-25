-- Tags: atomic-database
-- An incremental refresh resumes from the cursor that the read reports at the end of the snapshot.
-- A query that does not need the whole source for its result (DISTINCT of a constant, GROUP BY with the overflow mode 'break')
-- closes the source early. The source is read to the end anyway, so that the cursor advances
-- and a refresh without new data in the source appends nothing.

DROP TABLE IF EXISTS end_src;
DROP TABLE IF EXISTS end_tgt_distinct;
DROP TABLE IF EXISTS end_tgt_group_by;

CREATE TABLE end_src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE end_tgt_distinct (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE end_tgt_group_by (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW end_mv_distinct REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO end_tgt_distinct EMPTY
    AS SELECT DISTINCT 1 AS k FROM end_src SETTINGS max_block_size = 1, max_threads = 1;
CREATE MATERIALIZED VIEW end_mv_group_by REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO end_tgt_group_by EMPTY
    AS SELECT k FROM end_src GROUP BY k
    SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'break', max_block_size = 1, max_threads = 1;

-- Several parts and several blocks in every part.
INSERT INTO end_src SELECT number FROM numbers(10);
INSERT INTO end_src SELECT number FROM numbers(10, 10);
INSERT INTO end_src SELECT number FROM numbers(20, 10);

SYSTEM REFRESH VIEW end_mv_distinct;
SYSTEM WAIT VIEW end_mv_distinct;
SYSTEM REFRESH VIEW end_mv_group_by;
SYSTEM WAIT VIEW end_mv_group_by;
SELECT 'round1', (SELECT count() FROM end_tgt_distinct), (SELECT count() > 0 FROM end_tgt_group_by);

-- Nothing new in the source: nothing is appended.
CREATE TEMPORARY TABLE end_counts AS SELECT count() AS c FROM end_tgt_group_by;
SYSTEM REFRESH VIEW end_mv_distinct;
SYSTEM WAIT VIEW end_mv_distinct;
SYSTEM REFRESH VIEW end_mv_group_by;
SYSTEM WAIT VIEW end_mv_group_by;
SELECT 'round2', (SELECT count() FROM end_tgt_distinct), (SELECT count() FROM end_tgt_group_by) = (SELECT c FROM end_counts);

-- New data: the refresh reads only it.
INSERT INTO end_src SELECT number FROM numbers(100, 10);
SYSTEM REFRESH VIEW end_mv_distinct;
SYSTEM WAIT VIEW end_mv_distinct;
SYSTEM REFRESH VIEW end_mv_group_by;
SYSTEM WAIT VIEW end_mv_group_by;
SELECT 'round3', (SELECT count() FROM end_tgt_distinct), (SELECT count() FROM end_tgt_group_by WHERE k < 100) = (SELECT c FROM end_counts),
    (SELECT count() > 0 FROM end_tgt_group_by WHERE k >= 100);

DROP TABLE end_mv_distinct;
DROP TABLE end_mv_group_by;
DROP TABLE end_tgt_distinct;
DROP TABLE end_tgt_group_by;
DROP TABLE end_src;
