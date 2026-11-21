-- Random settings limits: index_granularity=(100, None); index_granularity_bytes=(100000, None)
-- A filter on the key of a normal projection still selects the projection when optimize_aggregation_in_order is enabled,
-- instead of reading the whole table in the order of the GROUP BY key.

DROP TABLE IF EXISTS t_aggregation_in_order_projection;
CREATE TABLE t_aggregation_in_order_projection (x UInt64, y UInt64, PROJECTION p (SELECT * ORDER BY y))
ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 128;
INSERT INTO t_aggregation_in_order_projection SELECT number, (number * 7919) % 100000 FROM numbers(100000);
OPTIMIZE TABLE t_aggregation_in_order_projection FINAL;

SET optimize_aggregation_in_order = 1, optimize_use_projections = 1, force_optimize_projection = 1, enable_parallel_replicas = 0, prefer_optimize_projection = 0;

SELECT x, count() FROM t_aggregation_in_order_projection WHERE y = 43700 GROUP BY x;

-- Without a selective filter, the table is still read in the order of the GROUP BY key.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT x, count() FROM t_aggregation_in_order_projection GROUP BY x SETTINGS force_optimize_projection = 0)
WHERE explain LIKE '%AggregatingInOrderTransform%';

DROP TABLE t_aggregation_in_order_projection;
