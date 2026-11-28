-- Some optimizations derive a predicate on one join side from a key equality with the other side.
-- They assume that `x = y` in the JOIN is a plain equality of the two values. For `FixedString` x and
-- `String` y it is not: `toFixedString('a', 3)` matches both `'a'` and `'a\0\0'`. Such keys must be skipped.

SET enable_analyzer = 1;
SET join_algorithm = 'hash';
SET query_plan_filter_push_down = 1;

DROP TABLE IF EXISTS fs;
DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS s2;

CREATE TABLE fs (x FixedString(3)) ENGINE = MergeTree ORDER BY x;
CREATE TABLE s (y String) ENGINE = MergeTree ORDER BY y;
CREATE TABLE s2 (z String) ENGINE = MergeTree ORDER BY z;

INSERT INTO fs VALUES ('a'), ('b');
INSERT INTO s VALUES ('a'), ('a\0\0'), ('b');
INSERT INTO s2 VALUES ('a'), ('a\0\0'), ('b');

SELECT '-- filter pushdown through the join: the result must not depend on the setting';
SELECT count() FROM fs JOIN s ON x = y WHERE y = 'a\0\0';
SELECT count() FROM fs JOIN s ON x = y WHERE y = 'a\0\0' SETTINGS query_plan_filter_push_down = 0;
SELECT count() FROM fs JOIN s ON x = y WHERE y = 'a';
SELECT count() FROM fs JOIN s ON x = y WHERE y = 'a' SETTINGS query_plan_filter_push_down = 0;
SELECT count() FROM fs JOIN s ON x = y WHERE x = 'a';
SELECT count() FROM fs JOIN s ON x = y WHERE x = 'a' SETTINGS query_plan_filter_push_down = 0;
SELECT count() FROM fs JOIN s ON x = y WHERE y LIKE 'a%';
SELECT count() FROM fs JOIN s ON x = y WHERE y LIKE 'a%' SETTINGS query_plan_filter_push_down = 0;

SELECT '-- the join is not converted to IN (result, plan has a set)';
SELECT count() FROM fs ANY INNER JOIN s ON x = y SETTINGS query_plan_convert_join_to_in = 1;
SELECT count() FROM fs ANY INNER JOIN s ON x = y SETTINGS query_plan_convert_join_to_in = 0;
SELECT countIf(explain LIKE '%CreatingSet%') > 0 FROM (EXPLAIN SELECT count() FROM fs ANY INNER JOIN s ON x = y SETTINGS query_plan_convert_join_to_in = 1);

SELECT '-- control: a join on String keys is converted (result, plan has a set)';
SELECT count() FROM s ANY INNER JOIN s2 ON y = z SETTINGS query_plan_convert_join_to_in = 1;
SELECT countIf(explain LIKE '%CreatingSet%') > 0 FROM (EXPLAIN SELECT count() FROM s ANY INNER JOIN s2 ON y = z SETTINGS query_plan_convert_join_to_in = 1);

SELECT '-- LEFT ANY JOIN with a GROUP BY on the key is not converted to INNER ALL: `x` matches two groups';
SELECT count() FROM fs LEFT ANY JOIN (SELECT y, count() AS n FROM s GROUP BY y) AS g ON x = y WHERE g.n > 0;
SELECT count() FROM fs LEFT ANY JOIN (SELECT y, count() AS n FROM s GROUP BY y) AS g ON x = y WHERE g.n > 0 SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

DROP TABLE fs;
DROP TABLE s;
DROP TABLE s2;
