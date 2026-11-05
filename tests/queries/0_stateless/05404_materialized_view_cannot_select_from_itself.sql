CREATE TABLE src (c0 Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY tuple() AS SELECT c0 FROM src;
CREATE VIEW v AS SELECT c0 FROM mv;

ALTER TABLE mv MODIFY QUERY SELECT c0 FROM mv; -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM (SELECT c0 FROM mv); -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM (SELECT c0 FROM src JOIN mv USING (c0)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM src JOIN mv USING (c0); -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM src JOIN (SELECT c0 FROM mv) AS t USING (c0); -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM src WHERE c0 IN (SELECT c0 FROM mv); -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM src WHERE c0 > (SELECT count() FROM mv); -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM src UNION ALL SELECT c0 FROM mv; -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM src WHERE c0 IN (SELECT c0 FROM cluster('test_shard_localhost', currentDatabase(), 'mv')); -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM v; -- { serverError INFINITE_LOOP }
-- A string literal on the right side of IN is a set of values, not a table.
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM src WHERE toString(c0) IN ('v');
-- A recursive CTE named like the view is not the view.
ALTER TABLE mv MODIFY QUERY WITH RECURSIVE mv AS (SELECT 1 AS c0 UNION ALL SELECT c0 + 1 FROM mv WHERE c0 < 3) SELECT c0 FROM src WHERE c0 IN (SELECT c0 FROM mv);
SELECT dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'src';
-- A temporary table named like the view is stored unqualified, so outside this session it is the view.
CREATE TEMPORARY TABLE mv (c0 Int32);
ALTER TABLE {CLICKHOUSE_DATABASE:Identifier}.mv MODIFY QUERY SELECT c0 FROM mv; -- { serverError BAD_ARGUMENTS }
ALTER TABLE {CLICKHOUSE_DATABASE:Identifier}.mv MODIFY QUERY WITH RECURSIVE mv AS (SELECT 1 AS c0 UNION ALL SELECT c0 + 1 FROM mv WHERE c0 < 3) SELECT c0 FROM src WHERE c0 IN (SELECT c0 FROM mv);
DROP TEMPORARY TABLE mv;
CREATE TEMPORARY TABLE mv3 (c0 Int32);
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE:Identifier}.mv3 ENGINE = MergeTree ORDER BY tuple() AS SELECT c0 FROM mv3; -- { serverError BAD_ARGUMENTS, UNKNOWN_TABLE }
DROP TEMPORARY TABLE mv3;
DROP TABLE v;
RENAME TABLE mv TO mv2;
ALTER TABLE mv2 MODIFY QUERY SELECT c0 FROM mv2; -- { serverError BAD_ARGUMENTS }
CREATE TEMPORARY TABLE mv2 (c0 Int32);
ALTER TABLE {CLICKHOUSE_DATABASE:Identifier}.mv2 MODIFY QUERY SELECT c0 FROM mv2; -- { serverError BAD_ARGUMENTS }
ALTER TABLE {CLICKHOUSE_DATABASE:Identifier}.mv2 MODIFY QUERY SELECT c0 FROM src WHERE c0 IN mv2; -- { serverError BAD_ARGUMENTS }
DROP TEMPORARY TABLE mv2;
SELECT dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'src';

-- A refreshable view is not triggered by inserts, so it may read itself.
CREATE TABLE dst (c0 Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 YEAR APPEND TO dst EMPTY AS SELECT c0 FROM src;
ALTER TABLE rmv MODIFY QUERY SELECT c0 FROM src WHERE c0 NOT IN (SELECT c0 FROM rmv);

-- A string literal on the right side of IN in a column default is not a loading dependency either.
CREATE TABLE t1 (c0 String, c1 UInt8 DEFAULT c0 IN ('t2')) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (c0 String, c1 UInt8 DEFAULT c0 IN ('t1')) ENGINE = MergeTree ORDER BY tuple();

DROP TABLE t2;
DROP TABLE t1;
DROP TABLE rmv;
DROP TABLE dst;
DROP TABLE mv2;
DROP TABLE src;
