-- Random settings limits: execute_exists_as_scalar_subquery=(1, None)
-- The bug is on the path that executes a non-correlated EXISTS as a scalar subquery (execute_exists_as_scalar_subquery = 1).

DROP TABLE IF EXISTS t_exists_filter;

SELECT number FROM numbers(3) WHERE exists(SELECT 1) ORDER BY number SETTINGS additional_result_filter = 'number > 0';
SELECT number FROM numbers(3) ORDER BY number SETTINGS additional_result_filter = 'exists(SELECT 1)';
SELECT number FROM numbers(3) WHERE NOT exists(SELECT 1 WHERE 0) ORDER BY number SETTINGS additional_result_filter = 'number > 0';
SELECT number, exists(SELECT 1 UNION ALL SELECT 2) FROM numbers(3) ORDER BY number SETTINGS additional_result_filter = 'number > 0';
SELECT number FROM numbers(2) WHERE exists(SELECT 1) UNION ALL SELECT number FROM numbers(2) WHERE exists(SELECT 1) SETTINGS additional_result_filter = 'number > 0';

CREATE TABLE t_exists_filter (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_exists_filter VALUES (0), (1), (2);
SELECT x FROM t_exists_filter ORDER BY x SETTINGS additional_table_filters = {'t_exists_filter': 'exists(SELECT 1)'}, additional_result_filter = 'x > 0';

SELECT number FROM numbers(exists(SELECT 1) + 2) ORDER BY number SETTINGS additional_result_filter = 'number > 0';

-- Each limit is the smallest max_subquery_depth that accepts the query.
SELECT exists(SELECT 1) SETTINGS max_subquery_depth = 2;
SELECT exists(SELECT 1 WHERE exists(SELECT 1)) SETTINGS max_subquery_depth = 4;

SELECT number FROM numbers(3) WHERE exists(SELECT 1) ORDER BY number SETTINGS execute_exists_as_scalar_subquery = 0, additional_result_filter = 'number > 0';

DROP TABLE t_exists_filter;
