-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- The key of a `key = value` table function argument names a setting of the table function,
-- not a column of the query that happens to have the same name.

DROP TABLE IF EXISTS t_05516;
CREATE TABLE t_05516 (foo String, filename String, format String) ENGINE = MergeTree ORDER BY foo;
INSERT INTO t_05516 VALUES ('a', 'f1', 'fmt1'), ('c', 'f3', 'fmt3');

SET s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = '05516_lookup.csv', format = 'CSV', structure = 'foo String') VALUES ('a'), ('b');

SELECT l.foo, l.filename, l.format
FROM t_05516 AS l
INNER JOIN s3(s3_conn, filename = '05516_lookup.csv', format = 'CSV', structure = 'foo String') AS lk ON l.foo = lk.foo
ORDER BY l.foo;

SELECT foo FROM t_05516
WHERE foo IN (SELECT foo FROM s3(s3_conn, filename = '05516_lookup.csv', format = 'CSV', structure = 'foo String'))
ORDER BY foo;

DROP TABLE t_05516;
