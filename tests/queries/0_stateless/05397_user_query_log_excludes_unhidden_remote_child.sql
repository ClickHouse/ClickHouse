-- Tags: no-parallel
-- Tag no-parallel: copies share a global user, and EXECUTE AS does not accept identifier parameters.

-- A marker-zero remote child must remain hidden from its initiator's own query log.
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_START';

DROP USER IF EXISTS user_05397_remote;
DROP TABLE IF EXISTS t_05397_remote_query_id;
DROP TABLE IF EXISTS t_05397_run_marker;
DROP TABLE IF EXISTS t_05397_visible;

CREATE TABLE t_05397_visible (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05397_visible VALUES (1), (2);

CREATE USER user_05397_remote IDENTIFIED WITH no_password;
GRANT SELECT ON t_05397_visible TO user_05397_remote;
GRANT REMOTE ON *.* TO user_05397_remote;
GRANT CREATE TEMPORARY TABLE ON *.* TO user_05397_remote;

-- The test user can read only the fresh marker and its own parent query ID.
CREATE TABLE t_05397_run_marker (started_at DateTime64(6, 'UTC')) ENGINE = Memory;
INSERT INTO t_05397_run_marker SELECT now64(6, 'UTC');
GRANT SELECT ON t_05397_run_marker TO user_05397_remote;

CREATE TABLE t_05397_remote_query_id (query_id String) ENGINE = Memory;
GRANT SELECT ON t_05397_remote_query_id TO user_05397_remote;

-- A plain-table remote read produces an unhidden child linked to this top-level query.
EXECUTE AS user_05397_remote SELECT id
FROM remote('127.0.0.1', currentDatabase(), 't_05397_visible', 'user_05397_remote')
WHERE id > 0 ORDER BY id
FORMAT Null SETTINGS prefer_localhost_replica = 0, log_comment = '05397_remote_visible';

SYSTEM FLUSH LOGS query_log;

INSERT INTO t_05397_remote_query_id
SELECT query_id
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_id = initial_query_id
    AND initial_user = 'user_05397_remote'
    AND query_start_time_microseconds >= (SELECT started_at FROM t_05397_run_marker)
    AND log_comment = '05397_remote_visible';

-- Require exactly one linked child.
-- Do not filter child current_database: remote child rows may record another database context.
SELECT
    (SELECT count() = 1 FROM t_05397_remote_query_id),
    count() = 1
FROM
(
    SELECT query_id
    FROM system.query_log
    WHERE initial_query_id = (SELECT any(query_id) FROM t_05397_remote_query_id)
        AND query_id != (SELECT any(query_id) FROM t_05397_remote_query_id)
    GROUP BY query_id
);

-- Report each child property independently, while retaining the combined hidden-child assertion.
SELECT
    count() = 1 AND countIf(query_starts = 1) = 1,
    count() = 1 AND countIf(query_finishes = 1) = 1,
    count() = 1 AND countIf(non_initial_finishes = 1) = 1,
    count() = 1 AND countIf(unhidden_finishes = 1) = 1,
    count() = 1 AND countIf(user_finishes = 1) = 1,
    count() = 1 AND countIf(table_finishes = 1) = 1,
    count() = 1 AND countIf(marker_zero_finishes = 1) = 1
FROM
(
    SELECT
        query_id,
        countIf(type = 'QueryStart') AS query_starts,
        countIf(type = 'QueryFinish') AS query_finishes,
        countIf(type = 'QueryFinish' AND is_initial_query = 0) AS non_initial_finishes,
        countIf(type = 'QueryFinish' AND query_access_info_is_hidden = 0) AS unhidden_finishes,
        countIf(type = 'QueryFinish' AND initial_user = 'user_05397_remote') AS user_finishes,
        countIf(type = 'QueryFinish'
            AND has(tables, concat(currentDatabase(), '.t_05397_visible'))) AS table_finishes,
        countIf(type = 'QueryFinish'
            AND is_initial_query = 0
            AND query_access_info_is_hidden = 0
            AND initial_user = 'user_05397_remote'
            AND has(tables, concat(currentDatabase(), '.t_05397_visible'))) AS marker_zero_finishes
    FROM system.query_log
    WHERE initial_query_id = (SELECT any(query_id) FROM t_05397_remote_query_id)
        AND query_id != (SELECT any(query_id) FROM t_05397_remote_query_id)
    GROUP BY query_id
);

-- The positive control is visible, while the linked child is absent from the caller's own log.
EXECUTE AS user_05397_remote SELECT
    countIf(type = 'QueryFinish'
        AND query_id = (SELECT any(query_id) FROM t_05397_remote_query_id)
        AND initial_query_id = query_id
        AND log_comment = '05397_remote_visible') = 1,
    countIf(query_id != (SELECT any(query_id) FROM t_05397_remote_query_id)) = 0
FROM system.user_query_log
WHERE initial_query_id = (SELECT any(query_id) FROM t_05397_remote_query_id);

DROP USER user_05397_remote;
DROP TABLE t_05397_remote_query_id;
DROP TABLE t_05397_run_marker;
DROP TABLE t_05397_visible;
