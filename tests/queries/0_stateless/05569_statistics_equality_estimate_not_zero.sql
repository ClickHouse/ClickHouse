-- Statistics must not estimate a non-empty relation as 0 rows: the join order optimizer takes 0 for an empty relation,
-- every join with it then looks free, and it picks the order of the other joins by ties. With DPsub, the Join Order
-- Benchmark queries 6f, 19-20 and 30-31 got slower by orders of magnitude.
-- Random settings limits: query_plan_optimize_join_order_limit=(10, None); use_statistics=(1, 1); materialize_statistics_on_insert=(1, 1); query_plan_optimize_join_order_randomize=(0, 0)
SET enable_parallel_replicas = 0;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS dim;
DROP TABLE IF EXISTS fact;
CREATE TABLE dim (id Int32, info String) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE fact (id Int32, dim_id Int32) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO dim SELECT number, concat('info ', toString(number)) FROM numbers(500);
INSERT INTO fact SELECT number, number % 500 FROM numbers(10000);

SELECT '-- an equality on a column with as many distinct values as rows: a fraction of a row is rounded up to one';
SELECT extract(explain, 'd\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM fact AS f, dim AS d WHERE f.dim_id = d.id AND d.info = 'info 7')
WHERE explain LIKE '%⋈%' LIMIT 1;
SELECT count() FROM fact AS f, dim AS d WHERE f.dim_id = d.id AND d.info = 'info 7';

SELECT '-- a constant conjunct, here `IS NOT NULL` of a non-Nullable column, does not change the estimate';
WITH
    (SELECT extract(explain, 'd\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM fact AS f, dim AS d WHERE f.dim_id = d.id AND d.id < 250) WHERE explain LIKE '%⋈%' LIMIT 1) AS alone,
    (SELECT extract(explain, 'd\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM fact AS f, dim AS d WHERE f.dim_id = d.id AND d.id < 250 AND f.id IS NOT NULL) WHERE explain LIKE '%⋈%' LIMIT 1) AS with_constant
SELECT alone != '' AND alone = with_constant;
SELECT extract(explain, 'd\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM fact AS f, dim AS d WHERE f.dim_id = d.id AND d.info = 'info 7' AND f.id IS NOT NULL)
WHERE explain LIKE '%⋈%' LIMIT 1;

DROP TABLE dim;
DROP TABLE fact;
