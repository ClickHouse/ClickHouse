-- A `join_algorithm` list without `sorted_merge` / `parallel_sorted_merge` keeps the runtime-filter policy
-- it had before these algorithms existed: `partial_merge,hash` still gets the join runtime filter, which
-- removes `partial_merge` from the list, so the join runs as `hash`. The keys are not the sorting key here,
-- so the inputs are not read in the order of the join keys.

DROP TABLE IF EXISTS smj_lpm_left;
DROP TABLE IF EXISTS smj_lpm_right;

CREATE TABLE smj_lpm_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE smj_lpm_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY b;

INSERT INTO smj_lpm_left SELECT number, 100000 - number FROM numbers(100000);
INSERT INTO smj_lpm_right SELECT number, 1000 - number FROM numbers(1000);

SET enable_analyzer = 1;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0,
    query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
    query_plan_optimize_join_order_limit = 1, explain_query_plan_default = 'legacy';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SET join_algorithm = 'partial_merge,hash';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_lpm_left AS l INNER JOIN smj_lpm_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%BuildRuntimeFilter%';

SELECT count() FROM smj_lpm_left AS l INNER JOIN smj_lpm_right AS r ON l.id = r.id;

DROP TABLE smj_lpm_left;
DROP TABLE smj_lpm_right;
