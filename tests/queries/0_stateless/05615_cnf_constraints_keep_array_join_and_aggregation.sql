-- The constraint optimizations of `convert_query_to_cnf` must keep the `arrayJoin` and the aggregation of a filter.

SET convert_query_to_cnf = 1, optimize_using_constraints = 1;
SET optimize_extract_common_expressions = 0; -- it rewrites the same filters

DROP TABLE IF EXISTS t_cnf_aj;
DROP TABLE IF EXISTS t_cnf_aj_subst;

CREATE TABLE t_cnf_aj (a UInt64, b UInt64, s String, arr Array(UInt8), CONSTRAINT c_ab ASSUME a < b, CONSTRAINT c_s ASSUME startsWith(s, 'x'))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_cnf_aj SELECT number, number + 1, 'xy', [1, 2] FROM numbers(3);

SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE (a > 0 AND arrayJoin(arr) > 0) OR a > 0);
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE (a > 0 AND unnest(arr) > 0) OR a > 0) SETTINGS normalize_function_names = 0;
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE a > 0 AND (a > 0 OR arrayJoin(arr) > 0));
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE (a > 0 OR arrayJoin(arr) > 0) AND (a > 0 OR NOT arrayJoin(arr) > 0));
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE arrayJoin(arr) > 0 OR NOT arrayJoin(arr) > 0);
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE arrayJoin(CAST([], 'Array(UInt8)')) > 0 OR NOT arrayJoin(CAST([], 'Array(UInt8)')) > 0);
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE a < b OR arrayJoin(arr) > 5);
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE startsWith(s, 'x') OR arrayJoin(arr) > 5);
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE (a > b AND arrayJoin(arr) > 0) OR a >= 0);
SELECT count() FROM (SELECT a FROM t_cnf_aj WHERE arrayJoin(arr) > 0 AND ((a > 0 AND arrayJoin([5, 6, 7]) > 0) OR a > 0));
SELECT count() FROM (SELECT a FROM t_cnf_aj GROUP BY a HAVING (a > 0 AND arrayJoin([1, 2]) > 0) OR a > 0);
SELECT count() FROM (SELECT 1 FROM t_cnf_aj HAVING sum(a) > 0 OR NOT sum(a) > 0);
SELECT count() FROM (SELECT 1 FROM t_cnf_aj WHERE a > 100 HAVING sum(a) > 0 OR NOT sum(a) > 0);
SELECT (SELECT 1 FROM t_cnf_aj HAVING sum(a) > 0 OR NOT sum(a) > 0);
SELECT count() FROM (SELECT 1 FROM t_cnf_aj HAVING count() = 6 AND (sum(arrayJoin([1, 2])) > 0 OR NOT sum(arrayJoin([1, 2])) > 0) AND arrayJoin([1, 2]) > 0);

CREATE TABLE t_cnf_aj_subst (k UInt64, a UInt64, b UInt64, CONSTRAINT c_eq ASSUME a = b) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_cnf_aj_subst SELECT number, 2, 2 FROM numbers(3);

SELECT count() FROM (SELECT k FROM t_cnf_aj_subst WHERE arrayJoin(range(a)) >= 0 AND arrayJoin(range(b)) >= 0) SETTINGS optimize_substitute_columns = 1;
SELECT count() FROM (SELECT arrayJoin(range(a)), arrayJoin(range(b)) FROM t_cnf_aj_subst WHERE k >= 0) SETTINGS optimize_substitute_columns = 1;
SELECT count(), sum(length(r)) FROM (WITH range(a) AS r SELECT r, arrayJoin(r), arrayJoin(range(b)) FROM t_cnf_aj_subst WHERE k >= 0) SETTINGS optimize_substitute_columns = 1;

-- The reductions still apply when the `arrayJoin` or another aggregate function stays, and so does the substitution outside of `arrayJoin`.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT a FROM t_cnf_aj WHERE arrayJoin(arr) > 0 AND ((a > 0 AND arrayJoin(arr) > 0) OR a > 0)) WHERE explain ILIKE '%function_name: or,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM t_cnf_aj HAVING max(a) > 0 AND (count() > 0 OR NOT count() > 0)) WHERE explain ILIKE '%function_name: count,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT 1 FROM t_cnf_aj HAVING max(arrayJoin(arr)) > 0 AND (sum(arrayJoin(arr)) > 0 OR NOT sum(arrayJoin(arr)) > 0)) WHERE explain ILIKE '%function_name: sum,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT a, arrayJoin(range(a)) FROM t_cnf_aj_subst WHERE k >= 0) WHERE explain ILIKE '%column_name: b,%' SETTINGS optimize_substitute_columns = 1;

DROP TABLE t_cnf_aj;
DROP TABLE t_cnf_aj_subst;
