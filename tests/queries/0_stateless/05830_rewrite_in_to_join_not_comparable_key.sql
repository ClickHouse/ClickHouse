-- `IN (subquery)` with a key that is not comparable for equality (an `AggregateFunction` state)
-- must not be rewritten by `rewrite_in_to_join` into an `EXISTS` with `equals`, which rejects it.

SET allow_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_in_aggregate_state;
CREATE TABLE t_in_aggregate_state (k UInt64, s AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO t_in_aggregate_state SELECT number, initializeAggregation('sumState', number) FROM numbers(2);

SELECT 'rewrite_in_to_join = 0';
SET rewrite_in_to_join = 0;
SELECT groupArray(k) FROM t_in_aggregate_state WHERE s IN (SELECT s FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE s NOT IN (SELECT s FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE (s, k) IN (SELECT s, k FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE (toString(s), k) IN (SELECT s, k FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE (toString(s), k) NOT IN (SELECT s, k FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE [s] IN (SELECT [s] FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE map('a', s) IN (SELECT map('a', s) FROM t_in_aggregate_state WHERE k = 1);
SELECT a IN (SELECT initializeAggregation('sumState', toUInt64(0))) FROM (SELECT initializeAggregation('sumState', number) AS a FROM numbers(2));

SELECT 'rewrite_in_to_join = 1';
SET rewrite_in_to_join = 1;
SELECT groupArray(k) FROM t_in_aggregate_state WHERE s IN (SELECT s FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE s NOT IN (SELECT s FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE (s, k) IN (SELECT s, k FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE (toString(s), k) IN (SELECT s, k FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE (toString(s), k) NOT IN (SELECT s, k FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE [s] IN (SELECT [s] FROM t_in_aggregate_state WHERE k = 1);
SELECT groupArray(k) FROM t_in_aggregate_state WHERE map('a', s) IN (SELECT map('a', s) FROM t_in_aggregate_state WHERE k = 1);
SELECT a IN (SELECT initializeAggregation('sumState', toUInt64(0))) FROM (SELECT initializeAggregation('sumState', number) AS a FROM numbers(2));

DROP TABLE t_in_aggregate_state;
