-- Correlated `EXISTS` and a right-side-filtered `ANY` join under `ie_join`, which executes only inequality joins,
-- and the `ANY` to `SEMI` rewrite under `auto` and `default`.

DROP TABLE IF EXISTS t_ie_join_semi_l;
DROP TABLE IF EXISTS t_ie_join_semi_r;
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
-- CI may inject False; pin it so the conversion pass always runs.
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;

DROP TABLE IF EXISTS t_ie_join_semi_l;
DROP TABLE IF EXISTS t_ie_join_semi_r;
CREATE TABLE t_ie_join_semi_l (k UInt32, a UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_ie_join_semi_r (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_ie_join_semi_l SELECT number, number % 7 FROM numbers(100);
INSERT INTO t_ie_join_semi_r SELECT number * 2, number % 3 FROM numbers(50);

SELECT 'a correlated EXISTS under ie_join';
SELECT count() FROM t_ie_join_semi_l AS o WHERE EXISTS (SELECT 1 FROM t_ie_join_semi_l AS i WHERE i.a = o.a AND i.k = 5) SETTINGS join_algorithm = 'ie_join';
SELECT count() FROM t_ie_join_semi_l AS o WHERE NOT EXISTS (SELECT 1 FROM t_ie_join_semi_l AS i WHERE i.a = o.a AND i.k = 5) SETTINGS join_algorithm = 'ie_join';
SELECT count() FROM t_ie_join_semi_l AS x INNER JOIN t_ie_join_semi_l AS y ON x.k < y.k AND x.a > y.a
    WHERE EXISTS (SELECT 1 FROM t_ie_join_semi_l AS i WHERE i.a = x.a AND i.k = 5) SETTINGS join_algorithm = 'ie_join';
SELECT count() FROM t_ie_join_semi_l AS x INNER JOIN t_ie_join_semi_l AS y ON x.k < y.k AND x.a > y.a
    WHERE EXISTS (SELECT 1 FROM t_ie_join_semi_l AS i WHERE i.a = x.a AND i.k = 5) SETTINGS join_algorithm = 'hash';

SELECT 'an ANY LEFT JOIN filtered on the right side under ie_join,full_sorting_merge';
SELECT count() FROM t_ie_join_semi_l ANY LEFT JOIN t_ie_join_semi_r USING (k) WHERE t_ie_join_semi_r.v > 0 SETTINGS join_algorithm = 'ie_join,full_sorting_merge';

SELECT 'the strictness under auto and default';
SELECT extract(explain, 'Strictness: \\w+') FROM (EXPLAIN keep_logical_steps = 1, description = 1
    SELECT count() FROM t_ie_join_semi_l ANY LEFT JOIN t_ie_join_semi_r USING (k) WHERE t_ie_join_semi_r.v > 0
    SETTINGS join_algorithm = 'auto') WHERE explain LIKE '%Strictness%';
SELECT extract(explain, 'Strictness: \\w+') FROM (EXPLAIN keep_logical_steps = 1, description = 1
    SELECT count() FROM t_ie_join_semi_l ANY LEFT JOIN t_ie_join_semi_r USING (k) WHERE t_ie_join_semi_r.v > 0
    SETTINGS join_algorithm = 'default') WHERE explain LIKE '%Strictness%';

DROP TABLE t_ie_join_semi_l;
DROP TABLE t_ie_join_semi_r;
