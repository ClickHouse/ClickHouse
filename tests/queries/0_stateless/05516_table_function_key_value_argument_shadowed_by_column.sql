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

-- The key must also not resolve to a `WITH` alias of the same name, which took the
-- alias-to-constant-then-folded route on master.
WITH 'x' AS format
SELECT foo FROM s3(s3_conn, filename = '05516_lookup.csv', format = 'CSV', structure = 'foo String')
ORDER BY foo;

-- Without a named collection, `x = 1` is an ordinary comparison and `x` resolves to the alias.
WITH 1 AS x SELECT count() FROM numbers(x = 1);
WITH 1 AS x SELECT * FROM values('v UInt8', x = 1);
WITH 1 AS x SELECT * FROM (VALUES (x = 1));

DROP TABLE t_05516;
