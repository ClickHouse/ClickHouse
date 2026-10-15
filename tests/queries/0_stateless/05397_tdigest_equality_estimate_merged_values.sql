-- `y` has 274 values of 365 rows each. Its `tdigest` statistics merge the rows of neighbor values into one centroid
-- although there are fewer values than centroids allowed, so it has no centroid at `2001` and must not be used for
-- `y = 2001`: the estimate comes from `uniq` instead, not from an empty centroid. `s` keeps one centroid per value, so
-- its skew (90% of rows have `s = 1`) still comes from `tdigest`.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_tdigest_equality;
CREATE TABLE t_tdigest_equality (k UInt32, y Int64, s UInt8) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'uniq,tdigest';
INSERT INTO t_tdigest_equality SELECT number, 1900 + intDiv(number, 365), if(number < 90000, 1, 2 + number % 4) FROM numbers(100000);

SELECT 'y = 2001', toFloat64(regexpExtract(explain, 'rows: ~([0-9.]+)', 1)) BETWEEN 300 AND 450
FROM (EXPLAIN estimates = 1 SELECT a.k FROM t_tdigest_equality AS a JOIN t_tdigest_equality AS b ON a.k = b.k WHERE a.y = 2001)
WHERE explain LIKE 'Join (%';

SELECT 's = 1', toFloat64(regexpExtract(explain, 'rows: ~([0-9.]+)', 1)) BETWEEN 80000 AND 100000
FROM (EXPLAIN estimates = 1 SELECT a.k FROM t_tdigest_equality AS a JOIN t_tdigest_equality AS b ON a.k = b.k WHERE a.s = 1)
WHERE explain LIKE 'Join (%';

DROP TABLE t_tdigest_equality;
