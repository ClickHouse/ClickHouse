-- `FINAL` of `SummingMergeTree` merges only the columns it reads, in the order of the query. The merge takes the first
-- column of a `...Map` structure as its key, so a value column read before the key or without it was taken as the key,
-- and a value column read alone was not summed: https://github.com/ClickHouse/ClickHouse/issues/125256

DROP TABLE IF EXISTS t_summing_map;
DROP TABLE IF EXISTS t_summing_map_not_summable;

CREATE TABLE t_summing_map (id UInt32, statMap Nested(k UInt8, v UInt64, w UInt64)) ENGINE = SummingMergeTree ORDER BY id;
SYSTEM STOP MERGES t_summing_map;
INSERT INTO t_summing_map VALUES (1, [1], [10], [100]);
INSERT INTO t_summing_map VALUES (1, [1, 2], [5, 7], [50, 70]);

SELECT statMap.k, statMap.v, statMap.w FROM t_summing_map FINAL;
SELECT statMap.v, statMap.k FROM t_summing_map FINAL;
SELECT statMap.v, statMap.w FROM t_summing_map FINAL;
SELECT statMap.w FROM t_summing_map FINAL;
SELECT count() FROM t_summing_map FINAL WHERE has(statMap.v, 15);
SELECT statMap.v FROM t_summing_map FINAL
SETTINGS make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 0, enable_parallel_replicas = 0;

-- The merge does not sum a structure with a value column that cannot be summed.
CREATE TABLE t_summing_map_not_summable (id UInt32, sMap Nested(k UInt8, v UInt64, s String)) ENGINE = SummingMergeTree ORDER BY id;
SYSTEM STOP MERGES t_summing_map_not_summable;
INSERT INTO t_summing_map_not_summable VALUES (1, [1], [10], ['a']);
INSERT INTO t_summing_map_not_summable VALUES (1, [1], [5], ['b']);

SELECT sMap.k, sMap.v, sMap.s FROM t_summing_map_not_summable FINAL;
SELECT sMap.k, sMap.v FROM t_summing_map_not_summable FINAL;

DROP TABLE t_summing_map;
DROP TABLE t_summing_map_not_summable;
