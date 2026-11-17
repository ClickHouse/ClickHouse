-- Tags: no-fasttest
-- no-fasttest: needs S3

-- `S3::URI` puts the query of a URL into the key, and `?` makes the key a glob, so the table is read-only.
-- The errors of `INSERT` and `TRUNCATE` into such a table should not repeat the URL query.

SET log_queries = 1;

DROP TABLE IF EXISTS t_query;
DROP TABLE IF EXISTS t_archive;

CREATE TABLE t_query (x UInt32) ENGINE = S3('http://localhost:11111/test/' || currentDatabase() || '/key?param=url_query_marker', 'clickhouse', 'clickhouse', 'CSV');
-- The archive check runs before the glob check.
CREATE TABLE t_archive (x UInt32) ENGINE = S3('http://localhost:11111/test/' || currentDatabase() || '/a.zip?param=url_query_marker :: inner.csv', 'clickhouse', 'clickhouse', 'CSV');

INSERT INTO t_query VALUES (1); -- { serverError DATABASE_ACCESS_DENIED }
TRUNCATE TABLE t_query; -- { serverError DATABASE_ACCESS_DENIED }
INSERT INTO t_archive VALUES (1); -- { serverError NOT_IMPLEMENTED }
TRUNCATE TABLE t_archive; -- { serverError NOT_IMPLEMENTED }

SYSTEM FLUSH LOGS query_log;

SELECT query_kind, exception_code, position(exception, 'url_query_marker') > 0 AS has_url_query
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type != 'QueryStart' AND exception_code IN (48, 291)
ORDER BY event_time_microseconds;

DROP TABLE t_query;
DROP TABLE t_archive;
