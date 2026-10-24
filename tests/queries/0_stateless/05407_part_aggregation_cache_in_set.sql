-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: the part aggregation cache is global and the test checks its size
-- Tag no-parallel-replicas: the part aggregation cache is intentionally not used for parallel-replica reads

-- `ActionsDAG::updateHash` does not hash the contents of an `IN` set, so `g IN (1)` and `g IN (2)`
-- would share a cache key, and a set built from a subquery changes with the data of another table
-- while the query text stays the same. The optimization must not be applied to such queries.

SET allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache_in;
DROP TABLE IF EXISTS t_part_agg_cache_in_keys;

CREATE TABLE t_part_agg_cache_in (k UInt32, g UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_part_agg_cache_in_keys (g UInt32) ENGINE = MergeTree ORDER BY g;

INSERT INTO t_part_agg_cache_in SELECT number, number % 4, number FROM numbers(16);
INSERT INTO t_part_agg_cache_in_keys VALUES (1);

-- Sanity check: a query without `IN` is cached.
SELECT k % 2 AS p, sum(v) FROM t_part_agg_cache_in GROUP BY p ORDER BY p;
SELECT count() FROM system.part_aggregation_cache;
SYSTEM DROP PART AGGREGATION CACHE;

-- Tuple-literal sets: 1+5+9+13=28 and 2+6+10+14=32.
SELECT k % 2 AS p, sum(v) FROM t_part_agg_cache_in WHERE g IN (1) GROUP BY p ORDER BY p;
SELECT k % 2 AS p, sum(v) FROM t_part_agg_cache_in WHERE g IN (2) GROUP BY p ORDER BY p;
SELECT k % 2 AS p, sum(v) FROM t_part_agg_cache_in WHERE g NOT IN (0, 1, 3) GROUP BY p ORDER BY p;

-- Subquery sets: the same query text over different contents of the other table.
SELECT k % 2 AS p, sum(v) FROM t_part_agg_cache_in WHERE g IN (SELECT g FROM t_part_agg_cache_in_keys) GROUP BY p ORDER BY p;
TRUNCATE TABLE t_part_agg_cache_in_keys;
INSERT INTO t_part_agg_cache_in_keys VALUES (2);
SELECT k % 2 AS p, sum(v) FROM t_part_agg_cache_in WHERE g IN (SELECT g FROM t_part_agg_cache_in_keys) GROUP BY p ORDER BY p;

-- `IN` inside a key expression.
SELECT g IN (1, 2) AS p, sum(v) FROM t_part_agg_cache_in GROUP BY p ORDER BY p;

-- None of the queries with `IN` populated the cache.
SELECT count() FROM system.part_aggregation_cache;

DROP TABLE t_part_agg_cache_in;
DROP TABLE t_part_agg_cache_in_keys;
SYSTEM DROP PART AGGREGATION CACHE;
