-- A column reference in the SELECT list is named by its bare column name unless another result column has, or would get, that name.
-- https://github.com/ClickHouse/ClickHouse/issues/129643

SELECT '-- case 1: the issue, two and three tables';
SELECT * FROM (SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id) FORMAT TSVWithNames;
SELECT * FROM (SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id) FORMAT TSVWithNames;
SELECT a.id FROM values('id Int32', 1) a, values('id Int32', 1) b, values('id Int32', 1) c FORMAT TSVWithNames;

SELECT '-- case 2: outer references';
SELECT s.id FROM (SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id) s;
WITH cte AS (SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id) SELECT cte.id FROM cte;
SELECT id FROM (SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id);

SELECT '-- case 3: right-side tables';
SELECT b.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id FORMAT TSVWithNames;
SELECT c.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id LEFT JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;

SELECT '-- case 4: names that collide keep their qualifiers';
SELECT a.id, b.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id FORMAT TSVWithNames;
SELECT a.id, b.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;
SELECT a.id, 1 AS id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;
SELECT a.id, a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;

SELECT '-- case 5: matchers';
SELECT a.* FROM values('id Int32, x Int32', (1, 10)) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;
SELECT * FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;
SELECT COLUMNS(a.id, a.x) FROM values('id Int32, x Int32', (1, 10)) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;
SELECT a.COLUMNS('^(id|x)$') FROM values('id Int32, x Int32', (1, 10)) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;

SELECT '-- case 6: explicit alias';
SELECT a.id AS `a.id` FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;

SELECT '-- case 7: expressions are not column references';
SELECT a.id + 1 FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;

SELECT '-- case 8: join_use_nulls and group_by_use_nulls';
SELECT c.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id LEFT JOIN values('id Int32', 2) c ON a.id = c.id FORMAT TSVWithNames SETTINGS join_use_nulls = 1;
SELECT a.id, count() AS cnt FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id GROUP BY a.id WITH ROLLUP ORDER BY a.id NULLS LAST FORMAT TSVWithNames SETTINGS group_by_use_nulls = 1;
SELECT s.id FROM (SELECT a.* APPLY (x -> x) FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id GROUP BY a.id WITH ROLLUP SETTINGS group_by_use_nulls = 1) AS s ORDER BY s.id NULLS LAST;
SELECT s.id FROM (SELECT COLUMNS(a.id) APPLY (x -> x) FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id GROUP BY a.id WITH ROLLUP SETTINGS group_by_use_nulls = 1) AS s ORDER BY s.id NULLS LAST;

SELECT '-- case 9: the compatibility setting restores the qualified names';
SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames SETTINGS analyzer_compatibility_qualify_ambiguous_column_names = 1;
SELECT b.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id FORMAT TSVWithNames SETTINGS analyzer_compatibility_qualify_ambiguous_column_names = 1;
SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames SETTINGS compatibility = '26.10';

SELECT '-- case 10: analyzer_compatibility_multiple_joins_qualify_column_names';
SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames SETTINGS analyzer_compatibility_multiple_joins_qualify_column_names = 1;

SELECT '-- case 11: DESCRIBE, materialized views';
DESCRIBE (SELECT a.id, a.x FROM values('id Int32, x Int32', (1, 10)) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id);
CREATE TABLE t_a (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_b (id Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_c (id Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_dst (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_dst2 (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_b VALUES (1);
INSERT INTO t_c VALUES (1);
CREATE MATERIALIZED VIEW mv TO t_dst AS SELECT a.id, a.x FROM t_a AS a JOIN t_b AS b ON a.id = b.id JOIN t_c AS c ON a.id = c.id;
INSERT INTO t_a VALUES (1, 10);
SELECT * FROM t_dst;
CREATE MATERIALIZED VIEW rmv REFRESH AFTER 1 YEAR SETTINGS all_replicas = 1 APPEND TO t_dst2 EMPTY AS SELECT a.id, a.x FROM t_a AS a JOIN t_b AS b ON a.id = b.id JOIN t_c AS c ON a.id = c.id;
SYSTEM REFRESH VIEW rmv;
SYSTEM WAIT VIEW rmv;
SELECT * FROM t_dst2;

SELECT '-- case 12: remote';
SELECT id FROM remote('127.0.0.{1,2}', view(SELECT a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id)) ORDER BY id;

SELECT '-- case 13: matcher transformers';
SELECT r.* APPLY (x -> x, 'f_') FROM (SELECT 1 AS a) AS l CROSS JOIN (SELECT 2 AS a) AS r CROSS JOIN (SELECT 3 AS a) AS t FORMAT TSVWithNames;
SELECT l.* EXCEPT (b) FROM (SELECT 1 AS a, 2 AS b) AS l CROSS JOIN (SELECT 3 AS a) AS r CROSS JOIN (SELECT 4 AS a) AS t FORMAT TSVWithNames;
SELECT l.* REPLACE (x + 1 AS x) FROM (SELECT 1 AS a, 2 AS x) AS l CROSS JOIN (SELECT 3 AS a) AS r CROSS JOIN (SELECT 4 AS a) AS t FORMAT TSVWithNames;
SELECT a.* APPLY (x -> x) FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;
SELECT t.* APPLY (x -> x, 't.') FROM values('id Int32', 1) AS t FORMAT TSVWithNames;
SELECT t.* APPLY (x -> (x AS `t.id`)) FROM values('id Int32', 1) AS t FORMAT TSVWithNames;
SELECT * EXCEPT (id) REPLACE (id AS `t.id`) FROM values('id Int32, `t.id` Int32', (1, 2)) AS t FORMAT TSVWithNames;
SELECT s.`t.id` FROM (SELECT * EXCEPT (id) REPLACE (id AS `t.id`) FROM values('id Int32, `t.id` Int32', (1, 2)) AS t) AS s;

SELECT '-- case 14: subcolumn';
CREATE TABLE t_s (id Int32, tup Tuple(x Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_s2 (id Int32, tup Tuple(x Int32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_s VALUES (1, tuple(5));
INSERT INTO t_s2 VALUES (1, tuple(6));
SELECT a.tup.x FROM t_s AS a JOIN t_s2 AS b ON a.id = b.id JOIN t_c AS c ON a.id = c.id FORMAT TSVWithNames;

SELECT '-- case 15: later readers of the result column names, two tables then three tables';
SELECT a.id, a.x FROM values('id Int32, x Int32', (1, 10), (1, 20), (1, 30)) a JOIN values('id Int32', 1) b ON a.id = b.id
ORDER BY a.x WITH FILL TO 40 STEP 5 INTERPOLATE (id AS id + 1) FORMAT TSVWithNames;
SELECT a.id, a.x FROM values('id Int32, x Int32', (1, 10), (1, 20), (1, 30)) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id
ORDER BY a.x WITH FILL TO 40 STEP 5 INTERPOLATE (id AS id + 1) FORMAT TSVWithNames;
SELECT a.id, a.x FROM values('id Int32, x Int32', (1, 10), (1, 20), (1, 30)) a JOIN values('id Int32', 1) b ON a.id = b.id
ORDER BY 1 WITH FILL TO 4 INTERPOLATE (id AS id + 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.id, a.x FROM values('id Int32, x Int32', (1, 10), (1, 20), (1, 30)) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id
ORDER BY 1 WITH FILL TO 4 INTERPOLATE (id AS id + 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.* APPLY (x -> x) FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id
ORDER BY 1 WITH FILL TO 5 INTERPOLATE (id AS id + 10); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.id FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id
ORDER BY a.id WITH FILL TO 5 INTERPOLATE (id AS id + 10); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.id FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id
ORDER BY a.id WITH FILL TO 5 INTERPOLATE (id AS id + 10); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT t_a.id FROM t_a JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_a.id = t_c.id
ORDER BY {CLICKHOUSE_DATABASE:Identifier}.t_a.id WITH FILL TO 5 INTERPOLATE (id AS id + 10); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.id FROM t_a AS a JOIN t_b AS b ON a.id = b.id JOIN t_c AS c ON a.id = c.id
ORDER BY t_a.id WITH FILL TO 5 INTERPOLATE (id AS id + 10); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.* FROM t_a AS a JOIN t_b AS b ON a.id = b.id JOIN t_c AS c ON a.id = c.id
ORDER BY {CLICKHOUSE_DATABASE:Identifier}.t_a.id WITH FILL TO 5 INTERPOLATE (id AS id + 10); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.id FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id
GROUP BY a.id WITH ROLLUP ORDER BY 1 WITH FILL TO 5 INTERPOLATE (id AS id + 10) SETTINGS group_by_use_nulls = 1; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.* APPLY (x -> x) FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id
GROUP BY a.id WITH ROLLUP ORDER BY 1 WITH FILL TO 5 INTERPOLATE (id AS id + 10) SETTINGS group_by_use_nulls = 1; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT a.id, a.id AS k FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id
ORDER BY k WITH FILL TO 5 INTERPOLATE (id AS id + 10) FORMAT TSVWithNames;
SELECT a.id, a.id + 100 AS `a.id` FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id
ORDER BY `a.id` WITH FILL TO 105 INTERPOLATE (id AS id + 10) FORMAT TSVWithNames;
SELECT a.id, a.id AS k FROM values('id Int32', 1, 3) a ORDER BY a.id WITH FILL TO 5 INTERPOLATE (k AS k + 10) FORMAT TSVWithNames;
SELECT a.id AS k, a.id FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id
ORDER BY 1 WITH FILL TO 5 INTERPOLATE (id AS id + 10) FORMAT TSVWithNames;
SELECT a.id, b.id FROM values('id Int32', 1, 3) a JOIN values('id Int32', 1, 3) b ON a.id = b.id JOIN values('id Int32', 1, 3) c ON a.id = c.id
ORDER BY a.id WITH FILL TO 5 INTERPOLATE (`b.id` AS `b.id` + 10) FORMAT TSVWithNames;
SELECT a.all FROM (SELECT 1 AS id, 2 AS all) AS a JOIN (SELECT 1 AS id, 3 AS all) AS b ON a.id = b.id ORDER BY ALL; -- { serverError UNEXPECTED_EXPRESSION }
SELECT a.all FROM (SELECT 1 AS id, 2 AS all) AS a JOIN (SELECT 1 AS id, 3 AS all) AS b ON a.id = b.id JOIN (SELECT 1 AS id, 4 AS all) AS c ON a.id = c.id ORDER BY ALL; -- { serverError UNEXPECTED_EXPRESSION }
SELECT a.id AS all, a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id ORDER BY ALL; -- { serverError UNEXPECTED_EXPRESSION }
SELECT a.id AS all, a.id FROM values('id Int32', 1) a JOIN values('id Int32', 1) b ON a.id = b.id JOIN values('id Int32', 1) c ON a.id = c.id FORMAT TSVWithNames;

SELECT '-- case 16: an unqualified outer reference can match the renamed column';
SELECT v FROM (SELECT a.k, a.v FROM values('id Int32, k Int32, v String', (1, 7, 'a')) a JOIN values('id Int32, v String', (1, 'b')) b ON a.id = b.id JOIN values('id Int32, v String', (1, 'c')) c ON a.id = c.id) AS s JOIN values('k Int32, v String', (7, 'd')) d ON s.k = d.k;
SELECT v FROM (SELECT a.k, a.v FROM values('id Int32, k Int32, v String', (1, 7, 'a')) a JOIN values('id Int32, v String', (1, 'b')) b ON a.id = b.id JOIN values('id Int32, v String', (1, 'c')) c ON a.id = c.id) AS s JOIN values('k Int32, v String', (7, 'd')) d ON s.k = d.k SETTINGS analyzer_compatibility_qualify_ambiguous_column_names = 1;
SELECT v FROM (SELECT a.k, a.v FROM values('id Int32, k Int32, v String', (1, 7, 'a')) a JOIN values('id Int32, v String', (1, 'b')) b ON a.id = b.id JOIN values('id Int32, v String', (1, 'c')) c ON a.id = c.id) AS s JOIN values('k Int32, v String', (7, 'd')) d ON s.k = d.k JOIN values('k Int32', 7) e ON s.k = e.k; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE rmv;
DROP TABLE mv;
