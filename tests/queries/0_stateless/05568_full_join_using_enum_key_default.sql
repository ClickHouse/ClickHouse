-- Enum keys whose smallest value is not 0. A join fills a not-matched row with the first enum value,
-- so the `USING` key, `firstNonDefault`, must treat that value as the default.
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;
CREATE TABLE t_l (k Enum8('b' = 2, 'c' = 3), e Enum16('x' = 300, 'y' = 301), t Tuple(Enum8('b' = 2, 'c' = 3))) ENGINE = Memory;
CREATE TABLE t_r (k Enum8('b' = 2, 'c' = 3), e Enum16('x' = 300, 'y' = 301), t Tuple(Enum8('b' = 2, 'c' = 3))) ENGINE = Memory;
INSERT INTO t_l VALUES ('b', 'x', tuple('b'));
INSERT INTO t_r VALUES ('c', 'y', tuple('c'));

SELECT 'hash 0', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;
SELECT 'hash 1', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 1;
SELECT 'parallel_hash 0', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 0;
SELECT 'parallel_hash 1', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 1;
SELECT 'grace_hash 0', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'grace_hash', query_plan_join_swap_table = 0;
SELECT 'grace_hash 1', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'grace_hash', query_plan_join_swap_table = 1;
SELECT 'partial_merge 0', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 0;
SELECT 'partial_merge 1', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 1;
SELECT 'full_sorting_merge 0', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0;
SELECT 'full_sorting_merge 1', arraySort(groupArray((k, t_l.k, t_r.k))) FROM t_l FULL JOIN t_r USING (k) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 1;

SELECT 'where';
SELECT k FROM t_l FULL JOIN t_r USING (k) WHERE k = 'c';
SELECT 'group by';
SELECT k, count() FROM t_l FULL JOIN t_r USING (k) GROUP BY k ORDER BY k;
SELECT 'chain';
SELECT k FROM t_l FULL JOIN t_r USING (k) INNER JOIN (SELECT CAST('c', 'Enum8(\'b\' = 2, \'c\' = 3)') AS k) AS t_3 USING (k);
SELECT 'Enum16, two keys';
SELECT k, e FROM t_l FULL JOIN t_r USING (k, e) ORDER BY k;
SELECT 'Tuple';
SELECT t, t_l.t, t_r.t FROM t_l FULL JOIN t_r USING (t) ORDER BY t SETTINGS query_plan_join_swap_table = 0;
SELECT 'right key of a not-matched row';
SELECT t_l.k, t_r.k FROM t_l LEFT JOIN t_r ON t_l.k = t_r.k SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;
SELECT t_r.k FROM t_l LEFT ANTI JOIN t_r ON t_l.k = t_r.k SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;
SELECT count() FROM t_l LEFT JOIN t_r ON t_l.k = t_r.k WHERE t_r.k = 'b' SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;
SELECT count() FROM t_l LEFT JOIN t_r ON t_l.k = t_r.k WHERE t_r.k = 'b' SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'lazily indexed probe side';
-- The inner join on the probe side hands the left join its key lazily indexed; the settings keep that plan.
SELECT l.x, m.y, r.k FROM (SELECT CAST(if(number % 2 = 0, 'b', 'c'), 'Enum8(\'b\' = 2, \'c\' = 3)') AS k, number AS x FROM numbers(10)) AS l
INNER JOIN (SELECT number AS x, number * 10 AS y FROM numbers(10) WHERE number % 3 != 1) AS m ON l.x = m.x
LEFT JOIN t_r AS r ON l.k = r.k ORDER BY l.x
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 1,
    enable_join_runtime_filters = 0, query_plan_min_columns_for_join_lazy_indexing = 1;

DROP TABLE t_l;
DROP TABLE t_r;

SELECT 'zero is not the first value';
SELECT k FROM (SELECT CAST('b', 'Enum8(\'a\' = -1, \'b\' = 0, \'c\' = 1)') AS k) AS l FULL JOIN (SELECT CAST('c', 'Enum8(\'a\' = -1, \'b\' = 0, \'c\' = 1)') AS k) AS r USING (k) ORDER BY k;
SELECT 'zero is the first value';
SELECT k FROM (SELECT CAST('a', 'Enum8(\'a\' = 0, \'b\' = 1)') AS k) AS l FULL JOIN (SELECT CAST('b', 'Enum8(\'a\' = 0, \'b\' = 1)') AS k) AS r USING (k) ORDER BY k;

SELECT 'firstNonDefault';
SELECT firstNonDefault(CAST('b', 'Enum8(\'b\' = 2, \'c\' = 3)'), CAST('c', 'Enum8(\'b\' = 2, \'c\' = 3)'));
SELECT firstNonDefault(CAST('b', 'Enum8(\'b\' = 2, \'c\' = 3)'), CAST('b', 'Enum8(\'b\' = 2, \'c\' = 3)'));
SELECT firstNonDefault(CAST('b', 'Enum8(\'a\' = -1, \'b\' = 0, \'c\' = 1)'), CAST('c', 'Enum8(\'a\' = -1, \'b\' = 0, \'c\' = 1)'));
SELECT firstNonDefault(tuple(CAST('b', 'Enum8(\'b\' = 2, \'c\' = 3)')), tuple(CAST('c', 'Enum8(\'b\' = 2, \'c\' = 3)')));
SELECT firstNonDefault(materialize(CAST('b', 'Enum8(\'b\' = 2, \'c\' = 3)')), materialize(CAST('c', 'Enum8(\'b\' = 2, \'c\' = 3)')));
SELECT firstNonDefault(l, r) AS v FROM (SELECT CAST(arrayJoin(['b', 'c']), 'Enum8(\'b\' = 2, \'c\' = 3)') AS l, materialize(CAST('b', 'Enum8(\'b\' = 2, \'c\' = 3)')) AS r) ORDER BY v;
SELECT reinterpretAsUInt64(tupleElement(firstNonDefault(tuple(-0., CAST('b', 'Enum8(\'b\' = 2, \'c\' = 3)')), tuple(1., CAST('c', 'Enum8(\'b\' = 2, \'c\' = 3)'))), 1));

SELECT 'JSON';
SELECT toString(j) AS s FROM (SELECT '{"a":"b"}'::JSON(a Enum8('b' = 2, 'c' = 3)) AS j) AS l FULL JOIN (SELECT '{"a":"c"}'::JSON(a Enum8('b' = 2, 'c' = 3)) AS j) AS r USING (j) ORDER BY s;
SELECT toString(r.j) FROM (SELECT '{"a":"b"}'::JSON(a Enum8('b' = 2, 'c' = 3)) AS j) AS l LEFT JOIN (SELECT '{"a":"c"}'::JSON(a Enum8('b' = 2, 'c' = 3)) AS j) AS r ON l.j = r.j SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;
