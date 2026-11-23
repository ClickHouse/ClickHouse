-- A subquery that selects the join key several times gives the join an input with a repeated name. The join keeps
-- the columns of that name as they are, and prunes its other columns, so lazy materialization applies over it as it
-- does when the key is selected once. Every query is followed by the number of lazy reads in its plan, and the results
-- are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET enable_parallel_replicas = 0;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS t_lazy_dup_l;
DROP TABLE IF EXISTS t_lazy_dup_m;
DROP TABLE IF EXISTS t_lazy_dup_r;

CREATE TABLE t_lazy_dup_l (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
CREATE TABLE t_lazy_dup_m (id UInt64, m UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
CREATE TABLE t_lazy_dup_r (id UInt64, l_id UInt64, v UInt64, payload String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;

INSERT INTO t_lazy_dup_l SELECT number FROM numbers(1000);
INSERT INTO t_lazy_dup_m SELECT number, number * 7 FROM numbers(1000);
INSERT INTO t_lazy_dup_r SELECT number, number % 1000, cityHash64(number), concat('p', toString(number)) FROM numbers(3000);

SELECT '-- the join key selected once';
SELECT s.id, r.payload FROM (SELECT id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT s.id, r.payload FROM (SELECT id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5);

SELECT '-- the join key selected twice';
SELECT s.id, r.payload FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5;
SELECT s.id, r.payload FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT s.id, r.payload FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5);

SELECT '-- the join key selected twice, one of them qualified';
SELECT s.id, r.payload FROM (SELECT s.id, id FROM t_lazy_dup_l AS s) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5;
SELECT s.id, r.payload FROM (SELECT s.id, id FROM t_lazy_dup_l AS s) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT s.id, r.payload FROM (SELECT s.id, id FROM t_lazy_dup_l AS s) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5);

SELECT '-- both occurrences are returned';
SELECT * FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5;
SELECT * FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT * FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5);

SELECT '-- the key of the right side selected twice';
SELECT l.id, s.payload FROM t_lazy_dup_l AS l JOIN (SELECT l_id, l_id, v, payload FROM t_lazy_dup_r) AS s ON s.l_id = l.id ORDER BY s.v LIMIT 5;
SELECT l.id, s.payload FROM t_lazy_dup_l AS l JOIN (SELECT l_id, l_id, v, payload FROM t_lazy_dup_r) AS s ON s.l_id = l.id ORDER BY s.v LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.id, s.payload FROM t_lazy_dup_l AS l JOIN (SELECT l_id, l_id, v, payload FROM t_lazy_dup_r) AS s ON s.l_id = l.id ORDER BY s.v LIMIT 5);

SELECT '-- three tables, the key of the upper join selected twice';
SELECT s.id, m.m, r.payload FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_m AS m ON m.id = s.id JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5;
SELECT s.id, m.m, r.payload FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_m AS m ON m.id = s.id JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT s.id, m.m, r.payload FROM (SELECT id, id FROM t_lazy_dup_l) AS s JOIN t_lazy_dup_m AS m ON m.id = s.id JOIN t_lazy_dup_r AS r ON r.l_id = s.id ORDER BY r.v LIMIT 5);

SELECT '-- the key and the payload of the lazily read side selected twice';
SELECT l.id, s.payload FROM t_lazy_dup_l AS l JOIN (SELECT l_id, l_id, v, payload, payload FROM t_lazy_dup_r) AS s ON s.l_id = l.id ORDER BY s.v LIMIT 5;
SELECT l.id, s.payload FROM t_lazy_dup_l AS l JOIN (SELECT l_id, l_id, v, payload, payload FROM t_lazy_dup_r) AS s ON s.l_id = l.id ORDER BY s.v LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.id, s.payload FROM t_lazy_dup_l AS l JOIN (SELECT l_id, l_id, v, payload, payload FROM t_lazy_dup_r) AS s ON s.l_id = l.id ORDER BY s.v LIMIT 5);

SELECT '-- same-named columns with different values: the join reads the first one, whether or not it is returned';
SELECT * FROM (SELECT 1 AS a, 1 AS a UNION ALL SELECT 2, 3) AS s ORDER BY ALL;
SELECT s.a, r.x FROM (SELECT 1 AS a, 1 AS a UNION ALL SELECT 2, 3) AS s LEFT JOIN (SELECT arrayJoin([2, 3]) AS x) AS r ON r.x = s.a ORDER BY ALL;
SELECT r.x FROM (SELECT 1 AS a, 1 AS a UNION ALL SELECT 2, 3) AS s JOIN (SELECT arrayJoin([2, 3]) AS x) AS r ON r.x = s.a ORDER BY ALL;
SELECT r.x FROM (SELECT arrayJoin([2, 3]) AS x) AS r JOIN (SELECT 1 AS a, 1 AS a UNION ALL SELECT 2, 3) AS s ON r.x = s.a ORDER BY ALL;

DROP TABLE t_lazy_dup_l;
DROP TABLE t_lazy_dup_m;
DROP TABLE t_lazy_dup_r;
