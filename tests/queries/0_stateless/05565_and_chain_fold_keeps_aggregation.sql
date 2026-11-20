-- Folding an AND chain must not remove the query's aggregate functions or arrayJoin, https://github.com/ClickHouse/ClickHouse/issues/129858

SET optimize_redundant_comparisons = 1; -- the folds below are done by this optimization

DROP TABLE IF EXISTS t_and_fold_agg;
DROP TABLE IF EXISTS t_and_fold_agg_empty;
CREATE TABLE t_and_fold_agg (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_and_fold_agg SELECT number, number FROM numbers(10);
CREATE TABLE t_and_fold_agg_empty (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();

SELECT count() FROM (SELECT max(a) >= 0 AND max(b) >= 0 FROM t_and_fold_agg);
SELECT count() FROM (SELECT max(a) >= 0 AND 1 FROM t_and_fold_agg);
SELECT count() FROM (SELECT max(a) >= 0 AND 0 FROM t_and_fold_agg);
SELECT count() FROM (SELECT count() >= 0 AND count() >= 0 FROM t_and_fold_agg);
SELECT count() FROM (SELECT max(toLowCardinality(a)) >= 0 AND 1 FROM t_and_fold_agg);
SELECT count() FROM (SELECT max(a) > 5 AND max(a) < 3 FROM t_and_fold_agg);
SELECT count() FROM (SELECT max(a) = 1 AND max(a) = 2 FROM t_and_fold_agg);
SELECT count() FROM (SELECT max(a) > 0 AND materialize(5) < 1 AND materialize(5) > 2 FROM t_and_fold_agg);
SELECT count() FROM (SELECT arrayJoin([1, 2, 3]) > 0 AND number < 1 AND number > 2 FROM numbers(2));
SELECT count() FROM (SELECT 1 FROM t_and_fold_agg HAVING max(a) >= 0 AND max(b) >= 0);
SELECT count() FROM (SELECT 1 FROM t_and_fold_agg ORDER BY max(a) >= 0 AND 1);
SELECT count() FROM (SELECT sum(max(a)) OVER () >= 0 AND 1 FROM t_and_fold_agg);
SELECT count() FROM (SELECT max(a) >= 0 AND max(b) >= 0 FROM t_and_fold_agg_empty);
SELECT (SELECT max(a) >= 0 AND max(b) >= 0 FROM t_and_fold_agg);
SELECT count() FROM (SELECT max(a) >= 0 AND max(b) >= 0 FROM remote('127.0.0.{1,2}', currentDatabase(), t_and_fold_agg));
SELECT max(a) >= 0 AND max(b) >= 0, max(a) > 5 AND max(a) < 3 FROM t_and_fold_agg;

-- The optimization still folds comparisons over columns.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT a FROM t_and_fold_agg WHERE a >= 0 AND a < 5) WHERE explain ILIKE '%function_name: greaterOrEquals%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT a FROM t_and_fold_agg WHERE a > 5 AND a < 3) WHERE explain ILIKE '%function_name: less%';

DROP TABLE t_and_fold_agg;
DROP TABLE t_and_fold_agg_empty;
