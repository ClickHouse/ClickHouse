-- Tags: atomic-database
-- An incremental refresh resumes from the cursor that the read reports at the end of the snapshot.
-- A LIMIT (or a LIMIT BY, or a limit with the overflow mode 'break') stops the read before that, so it is rejected (otherwise every refresh would append the same rows).

DROP TABLE IF EXISTS lim_src;
DROP TABLE IF EXISTS lim_tgt;

CREATE TABLE lim_src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE lim_tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1 OFFSET 1; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src OFFSET 1; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS limit = 1; -- { serverError NOT_IMPLEMENTED }

-- LIMIT BY stops the read as well, once every group that can appear has reached its limit.
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1 BY tuple(); -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1 BY k; -- { serverError NOT_IMPLEMENTED }

-- The limits that stop the read without an error.
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_result_rows = 1, result_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_execution_time = 100, timeout_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }

-- The same limits with an error are allowed.
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src SETTINGS max_result_rows = 1000000, result_overflow_mode = 'throw';
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'lim_mv';

DROP TABLE lim_mv;
DROP TABLE lim_tgt;
DROP TABLE lim_src;
