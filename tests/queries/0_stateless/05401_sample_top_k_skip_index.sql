-- Random settings limits: use_skip_indexes_for_top_k=(1, 1); query_plan_max_limit_for_top_k_optimization=(10, None)

-- `SAMPLE` drops rows after the read, so the skip-index top-K must not narrow the read to the granules holding the
-- extreme `k`: here none of their rows is sampled.

DROP TABLE IF EXISTS t_sample_top_k;

CREATE TABLE t_sample_top_k (id UInt32, h UInt32, k UInt32, INDEX mm_k k TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY (id, h) SAMPLE BY h SETTINGS index_granularity = 8;

-- Only the middle 100 rows (`k` 100..199) are sampled by `SAMPLE 1/2` and by `SAMPLE 1/4 OFFSET 1/4`.
INSERT INTO t_sample_top_k SELECT number, if(number BETWEEN 100 AND 199, 1073741824, 4294967295), number FROM numbers(300);

SELECT k FROM t_sample_top_k SAMPLE 1/2 ORDER BY k LIMIT 3;
SELECT k FROM t_sample_top_k SAMPLE 1/2 ORDER BY k DESC LIMIT 3;
SELECT k FROM t_sample_top_k SAMPLE 1/2 ORDER BY k, id LIMIT 3;
SELECT k FROM t_sample_top_k SAMPLE 150 ORDER BY k LIMIT 3;
SELECT k FROM t_sample_top_k SAMPLE 1/4 OFFSET 1/4 ORDER BY k LIMIT 3;

DROP TABLE t_sample_top_k;
