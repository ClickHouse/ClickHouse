-- Tags: atomic-database
-- An incremental refresh resumes from the cursor that the bounded read reports at its end. A `LIMIT` stops the read
-- earlier, so the cursor would not match the appended rows: such definitions are rejected.

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

-- `LIMIT BY` does not stop the read early, so it remains allowed.
CREATE MATERIALIZED VIEW lim_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO lim_tgt EMPTY
    AS SELECT k FROM lim_src LIMIT 1 BY k;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'lim_mv';

DROP TABLE lim_mv;
DROP TABLE lim_tgt;
DROP TABLE lim_src;
