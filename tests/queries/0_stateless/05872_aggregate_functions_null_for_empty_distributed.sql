-- Tags: shard

-- The `-OrNull` rewrite of aggregate function names must be idempotent: the shards receive already rewritten names
-- (e.g. `sumOrNullIf`) and must not rewrite them again (e.g. to `sumOrNullOrNullIf`).

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_null_for_empty_states;
CREATE TABLE t_null_for_empty_states (n AggregateFunction(sumOrNull, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_for_empty_states SELECT sumOrNullState(number) FROM numbers(10);

SET aggregate_functions_null_for_empty = 1, prefer_localhost_replica = 0;

SELECT sum(number) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT sumIf(number, number > 5) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT sumIf(number, number > 100) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT sumOrNullIf(number, number > 5) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT finalizeAggregation(sumState(number)) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT sumMerge(n) FROM remote('127.0.0.{1,2}', currentDatabase(), t_null_for_empty_states);

DROP TABLE t_null_for_empty_states;
