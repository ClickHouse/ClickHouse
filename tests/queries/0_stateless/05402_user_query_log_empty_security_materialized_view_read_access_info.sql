-- Tags: no-parallel
-- Tag no-parallel: copies share a global user with a fixed name.

-- An empty-security MV read of a private TO target must hide access metadata from the caller's own query log.
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_START';
SET use_query_cache = 0;

DROP USER IF EXISTS user_05402_empty_security_reader;
DROP TABLE IF EXISTS mv_05402_legacy_read;
DROP TABLE IF EXISTS t_05402_legacy_target;
DROP TABLE IF EXISTS t_05402_legacy_source;
DROP TABLE IF EXISTS t_05402_run_marker;

CREATE TABLE t_05402_legacy_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05402_legacy_target (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_05402_legacy_read TO t_05402_legacy_target AS
SELECT id FROM t_05402_legacy_source;
INSERT INTO t_05402_legacy_source VALUES (1);

-- Confirm the test uses the legacy empty-security definition under the default server setting.
SELECT count() = 1,
    countIf(positionCaseInsensitive(create_table_query, 'SQL SECURITY') = 0
        AND positionCaseInsensitive(create_table_query, 'DEFINER') = 0) = 1
FROM system.tables
WHERE database = currentDatabase()
    AND name = 'mv_05402_legacy_read';

CREATE USER user_05402_empty_security_reader;
GRANT SELECT ON t_05402_legacy_source TO user_05402_empty_security_reader;
GRANT SELECT ON mv_05402_legacy_read TO user_05402_empty_security_reader;

-- The target remains private even though the legacy MV path permits reading through the view.
EXECUTE AS user_05402_empty_security_reader SELECT count() FROM t_05402_legacy_target; -- { serverError ACCESS_DENIED }

CREATE TABLE t_05402_run_marker (started_at DateTime64(6, 'UTC')) ENGINE = Memory;
INSERT INTO t_05402_run_marker SELECT now64(6, 'UTC');
GRANT SELECT ON t_05402_run_marker TO user_05402_empty_security_reader;

EXECUTE AS user_05402_empty_security_reader SELECT sum(id) FROM mv_05402_legacy_read
SETTINGS log_comment = '05402_empty_security_mv_read' FORMAT Null;
SYSTEM FLUSH LOGS query_log;

-- Operators retain both events, the hidden marker, and the private target attribution.
SELECT
    countIf(type = 'QueryStart') = 1,
    countIf(type = 'QueryStart' AND query_access_info_is_hidden = 1) = 1,
    countIf(type = 'QueryFinish') = 1,
    countIf(type = 'QueryFinish' AND query_access_info_is_hidden = 1) = 1,
    countIf(type = 'QueryFinish' AND arrayExists(partition -> startsWith(partition,
        concat(currentDatabase(), '.t_05402_legacy_target.')), partitions)) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
    AND initial_query_id = query_id
    AND initial_user = 'user_05402_empty_security_reader'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05402_run_marker)
    AND type IN ('QueryStart', 'QueryFinish')
    AND log_comment = '05402_empty_security_mv_read';

-- The reader sees neither event and cannot discover the target through its own log.
EXECUTE AS user_05402_empty_security_reader SELECT
    countIf(type = 'QueryStart') = 0,
    countIf(type = 'QueryFinish') = 0,
    countIf(type = 'QueryFinish' AND arrayExists(partition -> startsWith(partition,
        concat(currentDatabase(), '.t_05402_legacy_target.')), partitions)) = 0
FROM system.user_query_log
WHERE current_database = currentDatabase()
    AND initial_query_id = query_id
    AND initial_user = 'user_05402_empty_security_reader'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05402_run_marker)
    AND type IN ('QueryStart', 'QueryFinish')
    AND log_comment = '05402_empty_security_mv_read';

DROP USER user_05402_empty_security_reader;
DROP TABLE mv_05402_legacy_read;
DROP TABLE t_05402_legacy_target;
DROP TABLE t_05402_legacy_source;
DROP TABLE t_05402_run_marker;
