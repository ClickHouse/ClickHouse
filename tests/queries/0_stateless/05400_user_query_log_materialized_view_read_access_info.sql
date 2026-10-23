-- Tags: no-parallel
-- Tag no-parallel: copies share a global user with a fixed name.

-- Reading a DEFINER/NONE materialized view hides its access metadata from the reader's own query log.
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_START';
SET use_query_cache = 0;

DROP USER IF EXISTS user_05400_mv_reader;
DROP TABLE IF EXISTS mv_05400_definer;
DROP TABLE IF EXISTS mv_05400_none;
DROP TABLE IF EXISTS t_05400_definer_source;
DROP TABLE IF EXISTS t_05400_none_source;
DROP TABLE IF EXISTS t_05400_run_marker;

CREATE TABLE t_05400_definer_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05400_none_source (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv_05400_definer
ENGINE = MergeTree ORDER BY id
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT id FROM t_05400_definer_source;

CREATE MATERIALIZED VIEW mv_05400_none
ENGINE = MergeTree ORDER BY id
DEFINER = CURRENT_USER SQL SECURITY NONE
AS SELECT id FROM t_05400_none_source;

INSERT INTO t_05400_definer_source VALUES (1);
INSERT INTO t_05400_none_source VALUES (1);

CREATE USER user_05400_mv_reader;
GRANT SELECT ON mv_05400_definer TO user_05400_mv_reader;
GRANT SELECT ON mv_05400_none TO user_05400_mv_reader;

CREATE TABLE t_05400_run_marker (started_at DateTime64(6, 'UTC')) ENGINE = Memory;
INSERT INTO t_05400_run_marker SELECT now64(6, 'UTC');
GRANT SELECT ON t_05400_run_marker TO user_05400_mv_reader;

EXECUTE AS user_05400_mv_reader SELECT count() FROM mv_05400_definer
FORMAT Null SETTINGS log_comment = '05400_mv_definer_read';
EXECUTE AS user_05400_mv_reader SELECT count() FROM mv_05400_none
FORMAT Null SETTINGS log_comment = '05400_mv_none_read';
SYSTEM FLUSH LOGS query_log;

-- The operator log retains both query rows and marks their access metadata hidden.
SELECT
    countIf(log_comment = '05400_mv_definer_read' AND query_access_info_is_hidden = 1),
    countIf(log_comment = '05400_mv_none_read' AND query_access_info_is_hidden = 1)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05400_run_marker)
    AND initial_user = 'user_05400_mv_reader'
    AND log_comment IN ('05400_mv_definer_read', '05400_mv_none_read');

-- The reader cannot observe either marked query in their own log.
EXECUTE AS user_05400_mv_reader SELECT
    countIf(log_comment = '05400_mv_definer_read'),
    countIf(log_comment = '05400_mv_none_read')
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND initial_query_id = query_id
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05400_run_marker)
    AND initial_user = 'user_05400_mv_reader'
    AND log_comment IN ('05400_mv_definer_read', '05400_mv_none_read');

DROP USER user_05400_mv_reader;
DROP TABLE mv_05400_definer;
DROP TABLE mv_05400_none;
DROP TABLE t_05400_definer_source;
DROP TABLE t_05400_none_source;
DROP TABLE t_05400_run_marker;
