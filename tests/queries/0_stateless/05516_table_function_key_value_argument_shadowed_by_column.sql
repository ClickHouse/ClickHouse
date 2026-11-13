-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: Depends on S3
-- Tag no-parallel: creates a server-global named collection with a fixed name, which concurrent runs would collide on

-- The key of a `key = value` table function argument names a setting of the table function,
-- not a column of the query that happens to have the same name.

DROP TABLE IF EXISTS t_05516;
CREATE TABLE t_05516 (foo String, filename String, format String) ENGINE = MergeTree ORDER BY foo;
INSERT INTO t_05516 VALUES ('a', 'f1', 'fmt1'), ('c', 'f3', 'fmt3');

SET s3_truncate_on_insert = 1;
-- Analyzing the `azureBlobStorage`/`s3` cases below resolves credentials; on a CI host with a managed
-- identity that falls back to the server's own identity, which is refused unless this is enabled.
SET s3_allow_server_credentials_in_user_queries = 1;
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

-- A table function that does not take `key = value` parameters (numbers) must resolve `x = 1` as an
-- ordinary comparison even when the first positional argument happens to name an existing collection:
-- collection existence must not change the meaning of positional expressions.
DROP NAMED COLLECTION IF EXISTS c_05516;
CREATE NAMED COLLECTION c_05516 AS k = 'v';
WITH 0 AS c_05516, 1 AS x SELECT count() FROM numbers(c_05516, x = 1);
DROP NAMED COLLECTION c_05516;

-- Cluster variants take the cluster name first, so the named collection is the second argument; the
-- `key = value` keys after it must still be treated as parameters, not resolved to a query column/alias.
WITH 'x' AS format
SELECT foo FROM s3Cluster('test_shard_localhost', s3_conn, filename = '05516_lookup.csv', format = 'CSV', structure = 'foo String')
ORDER BY foo;

-- A `key = value` is a named argument only as a trailing argument. A positional `identifier = value`
-- expression followed by a positional argument (here the partition_columns_in_data_file Boolean before
-- the structure) is an ordinary comparison and must resolve to the alias. Analysis only (EXPLAIN): the
-- endpoint is never contacted; the test fails (UNKNOWN_IDENTIFIER) if `keep` is wrongly skipped.
-- The EXPLAIN is wrapped in a subquery so the outer `FORMAT Null` discards the tree: a trailing
-- `FORMAT Null` on `EXPLAIN` itself is the explained query's format and does not suppress the output.
SELECT * FROM (
    EXPLAIN QUERY TREE
    WITH 1 AS keep
    SELECT * FROM azureBlobStorage('http://127.0.0.1:1/account', 'container', 'file.csv', 'CSV', 'none', 'NONE', keep = 1, 'a UInt8')
) FORMAT Null;

-- `extra_credentials(...)` is a wrapper the storage parser removes before it classifies arguments, so a
-- `key = value` named argument can precede it and must still be treated as a parameter, not resolved to a
-- shadowing `WITH` alias. Analysis only (EXPLAIN): the endpoint is never contacted; both orders must analyze
-- (BAD_ARGUMENTS if `partition_strategy` is wrongly folded instead of reaching the storage parser).
SELECT * FROM (
    EXPLAIN QUERY TREE
    WITH 'ignored' AS partition_strategy
    SELECT * FROM s3('http://127.0.0.1:1/bucket/file.csv', 'NOSIGN', 'CSV', 'a UInt8', partition_strategy = 'none', extra_credentials(role_arn = ''))
) FORMAT Null;

SELECT * FROM (
    EXPLAIN QUERY TREE
    WITH 'ignored' AS partition_strategy
    SELECT * FROM s3('http://127.0.0.1:1/bucket/file.csv', 'NOSIGN', 'CSV', 'a UInt8', extra_credentials(role_arn = ''), partition_strategy = 'none')
) FORMAT Null;

DROP TABLE t_05516;
