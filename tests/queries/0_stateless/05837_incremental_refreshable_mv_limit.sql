-- Tags: atomic-database
-- A `LIMIT` can close the source of an incremental refresh before its bounded read finishes, so the read
-- would not report the advanced cursor, and every refresh would append the same rows again. It is rejected.

DROP TABLE IF EXISTS incr_limit_src;
DROP TABLE IF EXISTS incr_limit_tgt;

CREATE TABLE incr_limit_src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE incr_limit_tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW incr_limit_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_limit_tgt EMPTY
    AS SELECT k FROM incr_limit_src LIMIT 1; -- { serverError NOT_IMPLEMENTED }

CREATE MATERIALIZED VIEW incr_limit_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_limit_tgt EMPTY
    AS SELECT k FROM incr_limit_src LIMIT 1, 10; -- { serverError NOT_IMPLEMENTED }

CREATE MATERIALIZED VIEW incr_limit_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_limit_tgt EMPTY
    AS SELECT k FROM incr_limit_src SETTINGS limit = 1; -- { serverError NOT_IMPLEMENTED }

-- `LIMIT ... UNTIL`, and `LIMIT BY` and `DISTINCT` (executed with a limit when their keys are constants) close the source too.
CREATE MATERIALIZED VIEW incr_limit_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_limit_tgt EMPTY
    AS SELECT k FROM incr_limit_src LIMIT UNTIL k >= 3; -- { serverError NOT_IMPLEMENTED }

CREATE MATERIALIZED VIEW incr_limit_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_limit_tgt EMPTY
    AS SELECT k FROM incr_limit_src LIMIT 1 BY toUInt8(1); -- { serverError NOT_IMPLEMENTED }

CREATE MATERIALIZED VIEW incr_limit_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_limit_tgt EMPTY
    AS SELECT DISTINCT toUInt64(1) AS k FROM incr_limit_src; -- { serverError NOT_IMPLEMENTED }

-- Without a limit, two refreshes without inserts in between append the rows once.
INSERT INTO incr_limit_src SELECT number FROM numbers(3);
CREATE MATERIALIZED VIEW incr_limit_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO incr_limit_tgt EMPTY
    AS SELECT k FROM incr_limit_src;
SYSTEM REFRESH VIEW incr_limit_mv;
SYSTEM WAIT VIEW incr_limit_mv;
SYSTEM REFRESH VIEW incr_limit_mv;
SYSTEM WAIT VIEW incr_limit_mv;
SELECT arraySort(groupArray(k)) FROM incr_limit_tgt;

DROP TABLE incr_limit_mv;
DROP TABLE incr_limit_tgt;
DROP TABLE incr_limit_src;
