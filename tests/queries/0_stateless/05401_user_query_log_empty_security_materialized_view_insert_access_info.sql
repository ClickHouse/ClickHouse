-- Tags: no-parallel
-- Tag no-parallel: copies share a global user with a fixed name.

-- Omitted SQL SECURITY leaves the MV security type empty; inserts through the MV must still hide access metadata.
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_START';

DROP USER IF EXISTS user_05401_empty_security_caller;
DROP TABLE IF EXISTS mv_05401_source_trigger;
DROP TABLE IF EXISTS mv_05401_root;
DROP TABLE IF EXISTS t_05401_source_sink;
DROP TABLE IF EXISTS t_05401_root_sink;
DROP TABLE IF EXISTS t_05401_source;
DROP TABLE IF EXISTS t_05401_root_source;
DROP TABLE IF EXISTS t_05401_run_marker;

CREATE TABLE t_05401_source (id UInt64) ENGINE = Null;
CREATE TABLE t_05401_source_sink (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_05401_source_trigger TO t_05401_source_sink AS
SELECT id FROM t_05401_source;

CREATE TABLE t_05401_root_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05401_root_sink (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_05401_root TO t_05401_root_sink AS
SELECT id FROM t_05401_root_source;

-- Confirm these definitions remain empty-security under the default server setting.
SELECT count() = 2,
    countIf(positionCaseInsensitive(create_table_query, 'SQL SECURITY') = 0) = 2
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('mv_05401_source_trigger', 'mv_05401_root');

CREATE USER user_05401_empty_security_caller;
-- The caller can insert through each MV, but cannot insert into either sink.
GRANT INSERT ON t_05401_source TO user_05401_empty_security_caller;
GRANT SELECT ON t_05401_source TO user_05401_empty_security_caller;
GRANT INSERT ON mv_05401_root TO user_05401_empty_security_caller;
GRANT INSERT ON t_05401_root_source TO user_05401_empty_security_caller;

-- Direct inserts into either target remain denied to the restricted caller.
EXECUTE AS user_05401_empty_security_caller INSERT INTO t_05401_source_sink
SETTINGS log_comment = '05401_direct_source_sink_insert' SELECT toUInt64(1); -- { serverError ACCESS_DENIED }
EXECUTE AS user_05401_empty_security_caller INSERT INTO t_05401_root_sink
SETTINGS log_comment = '05401_direct_root_sink_insert' SELECT toUInt64(2); -- { serverError ACCESS_DENIED }

CREATE TABLE t_05401_run_marker (started_at DateTime64(6, 'UTC')) ENGINE = Memory;
INSERT INTO t_05401_run_marker SELECT now64(6, 'UTC');
GRANT SELECT ON t_05401_run_marker TO user_05401_empty_security_caller;

EXECUTE AS user_05401_empty_security_caller INSERT INTO t_05401_source
SETTINGS log_comment = '05401_source_trigger_insert' SELECT toUInt64(1);
EXECUTE AS user_05401_empty_security_caller INSERT INTO mv_05401_root
SETTINGS log_comment = '05401_root_view_insert' SELECT toUInt64(2);
SYSTEM FLUSH LOGS query_log;

-- The operator log keeps both start and finish rows and marks each insert's access metadata hidden.
SELECT
    countIf(log_comment = '05401_source_trigger_insert' AND type = 'QueryStart') = 1,
    countIf(log_comment = '05401_source_trigger_insert' AND type = 'QueryStart' AND query_access_info_is_hidden = 1) = 1,
    countIf(log_comment = '05401_source_trigger_insert' AND type = 'QueryFinish') = 1,
    countIf(log_comment = '05401_source_trigger_insert' AND type = 'QueryFinish' AND query_access_info_is_hidden = 1) = 1,
    countIf(log_comment = '05401_source_trigger_insert' AND type = 'QueryFinish'
        AND has(tables, concat(currentDatabase(), '.t_05401_source_sink'))) = 1,
    countIf(log_comment = '05401_root_view_insert' AND type = 'QueryStart') = 1,
    countIf(log_comment = '05401_root_view_insert' AND type = 'QueryStart' AND query_access_info_is_hidden = 1) = 1,
    countIf(log_comment = '05401_root_view_insert' AND type = 'QueryFinish') = 1,
    countIf(log_comment = '05401_root_view_insert' AND type = 'QueryFinish' AND query_access_info_is_hidden = 1) = 1,
    countIf(log_comment = '05401_root_view_insert' AND type = 'QueryFinish'
        AND has(tables, concat(currentDatabase(), '.t_05401_root_sink'))) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND initial_query_id = query_id
    AND initial_user = 'user_05401_empty_security_caller'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05401_run_marker)
    AND type IN ('QueryStart', 'QueryFinish')
    AND log_comment IN ('05401_source_trigger_insert', '05401_root_view_insert');

-- The restricted caller sees neither start nor finish rows, so none of the insert metadata leaks.
EXECUTE AS user_05401_empty_security_caller SELECT
    countIf(log_comment = '05401_source_trigger_insert' AND type = 'QueryStart') = 0,
    countIf(log_comment = '05401_source_trigger_insert' AND type = 'QueryFinish') = 0,
    countIf(log_comment = '05401_root_view_insert' AND type = 'QueryStart') = 0,
    countIf(log_comment = '05401_root_view_insert' AND type = 'QueryFinish') = 0,
    countIf(notEmpty(databases) OR notEmpty(tables) OR notEmpty(columns) OR notEmpty(partitions)
        OR notEmpty(projections) OR notEmpty(views) OR notEmpty(used_row_policies)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND initial_query_id = query_id
    AND initial_user = 'user_05401_empty_security_caller'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05401_run_marker)
    AND type IN ('QueryStart', 'QueryFinish')
    AND log_comment IN ('05401_source_trigger_insert', '05401_root_view_insert');

SELECT count() FROM t_05401_source_sink;
SELECT count() FROM t_05401_root_sink;

DROP USER user_05401_empty_security_caller;
DROP TABLE mv_05401_source_trigger;
DROP TABLE mv_05401_root;
DROP TABLE t_05401_source_sink;
DROP TABLE t_05401_root_sink;
DROP TABLE t_05401_source;
DROP TABLE t_05401_root_source;
DROP TABLE t_05401_run_marker;
