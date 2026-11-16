-- Tags: no-replicated-database, no-ordinary-database
-- Tag no-ordinary-database: a refreshable materialized view needs an Atomic database.
-- Tag no-replicated-database: the test creates its own database and stops its own views.

DROP DATABASE IF EXISTS test_05579;
CREATE DATABASE test_05579 ENGINE = Atomic;

CREATE TABLE test_05579.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE test_05579.dst_empty (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE test_05579.dst_not_empty (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_05579.src VALUES (1);

-- EMPTY means "skip the initial refresh", so it is not a one-shot action: it has to stay in the
-- stored definition. Otherwise the view refreshes as soon as it is loaded again and replaces its
-- target table. The schedule is far in the future so that nothing fires while the test runs.
CREATE MATERIALIZED VIEW test_05579.mv_empty
REFRESH EVERY 1 YEAR TO test_05579.dst_empty EMPTY
AS SELECT max(id) AS id FROM test_05579.src;

CREATE MATERIALIZED VIEW test_05579.mv_not_empty
REFRESH EVERY 1 YEAR TO test_05579.dst_not_empty
AS SELECT max(id) AS id FROM test_05579.src;

SYSTEM STOP VIEW test_05579.mv_empty;
SYSTEM STOP VIEW test_05579.mv_not_empty;

-- The view that was created EMPTY keeps the keyword, and the one that was not does not.
SELECT countSubstrings(create_table_query, ' EMPTY') AS keeps_empty
FROM system.tables
WHERE database = 'test_05579' AND name = 'mv_empty';

SELECT countSubstrings(create_table_query, ' EMPTY') AS keeps_empty
FROM system.tables
WHERE database = 'test_05579' AND name = 'mv_not_empty';

-- The definition is read again when the view is loaded, so the keyword has to survive a reload.
-- This is the reported symptom: after a DETACH/ATTACH the view lost EMPTY and refreshed its target.
DETACH TABLE test_05579.mv_empty;
ATTACH TABLE test_05579.mv_empty;

SELECT countSubstrings(create_table_query, ' EMPTY') AS keeps_empty_after_reload
FROM system.tables
WHERE database = 'test_05579' AND name = 'mv_empty';

-- A plain table keeps its own EMPTY handling: there it is a one-shot action, so it stays out of the
-- stored definition and the table is still not filled by the INSERT SELECT.
CREATE TABLE test_05579.plain_empty EMPTY ENGINE = MergeTree ORDER BY tuple()
AS SELECT * FROM test_05579.src;
SELECT count() FROM test_05579.plain_empty;

SELECT countSubstrings(create_table_query, ' EMPTY') AS plain_keeps_empty
FROM system.tables
WHERE database = 'test_05579' AND name = 'plain_empty';

-- Drop the views before the database, so no refresh can still be in flight when it is dropped.
DROP TABLE test_05579.mv_empty SYNC;
DROP TABLE test_05579.mv_not_empty SYNC;

DROP DATABASE test_05579;
