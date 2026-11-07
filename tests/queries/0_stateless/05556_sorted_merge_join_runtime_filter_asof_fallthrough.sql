-- An `ASOF` join with `sorted_merge` / `parallel_sorted_merge` listed before `hash` keeps its runtime filter
-- when the inputs cannot be read in the join key order: the sorted-merge algorithm is not selectable then,
-- the selection falls through to `hash`, and the filter must stay as with plain `hash`.
-- When the inputs are sorted by the join keys, the merge join wins and there is no runtime filter.

DROP TABLE IF EXISTS smj_asof_a;
DROP TABLE IF EXISTS smj_asof_b;
DROP TABLE IF EXISTS smj_asof_sorted_a;
DROP TABLE IF EXISTS smj_asof_sorted_b;

CREATE TABLE smj_asof_a (id UInt64, ts UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE smj_asof_b (id UInt64, ts UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE smj_asof_sorted_a (id UInt64, ts UInt64, v UInt64) ENGINE = MergeTree ORDER BY (id, ts);
CREATE TABLE smj_asof_sorted_b (id UInt64, ts UInt64, v UInt64) ENGINE = MergeTree ORDER BY (id, ts);

INSERT INTO smj_asof_a SELECT number % 100, number, number FROM numbers(10000);
INSERT INTO smj_asof_b SELECT number % 10, number, number FROM numbers(1000);
INSERT INTO smj_asof_sorted_a SELECT * FROM smj_asof_a;
INSERT INTO smj_asof_sorted_b SELECT * FROM smj_asof_b;

SET enable_analyzer = 1;
-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0,
    query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
    query_plan_optimize_join_order_limit = 1, explain_query_plan_default = 'legacy';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT '--- hash ---';
SELECT * FROM (
EXPLAIN actions = 1
SELECT sum(a.v + b.v) FROM smj_asof_a AS a ASOF INNER JOIN smj_asof_b AS b ON a.id = b.id AND a.ts >= b.ts
SETTINGS join_algorithm = 'hash'
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%BuildRuntimeFilter%';

SELECT '--- sorted_merge,hash: unsorted inputs, hash with a runtime filter ---';
SELECT * FROM (
EXPLAIN actions = 1
SELECT sum(a.v + b.v) FROM smj_asof_a AS a ASOF INNER JOIN smj_asof_b AS b ON a.id = b.id AND a.ts >= b.ts
SETTINGS join_algorithm = 'sorted_merge,hash'
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%BuildRuntimeFilter%';

SELECT '--- parallel_sorted_merge,hash: unsorted inputs, hash with a runtime filter ---';
SELECT * FROM (
EXPLAIN actions = 1
SELECT sum(a.v + b.v) FROM smj_asof_a AS a ASOF INNER JOIN smj_asof_b AS b ON a.id = b.id AND a.ts >= b.ts
SETTINGS join_algorithm = 'parallel_sorted_merge,hash'
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%BuildRuntimeFilter%';

SELECT '--- sorted_merge,hash: sorted inputs, merge join without a runtime filter ---';
SELECT * FROM (
EXPLAIN actions = 1
SELECT sum(a.v + b.v) FROM smj_asof_sorted_a AS a ASOF INNER JOIN smj_asof_sorted_b AS b ON a.id = b.id AND a.ts >= b.ts
SETTINGS join_algorithm = 'sorted_merge,hash'
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%BuildRuntimeFilter%';

SELECT '--- Results are the same ---';
SELECT sum(a.v + b.v) FROM smj_asof_a AS a ASOF INNER JOIN smj_asof_b AS b ON a.id = b.id AND a.ts >= b.ts SETTINGS join_algorithm = 'hash';
SELECT sum(a.v + b.v) FROM smj_asof_a AS a ASOF INNER JOIN smj_asof_b AS b ON a.id = b.id AND a.ts >= b.ts SETTINGS join_algorithm = 'sorted_merge,hash';
SELECT sum(a.v + b.v) FROM smj_asof_a AS a ASOF INNER JOIN smj_asof_b AS b ON a.id = b.id AND a.ts >= b.ts SETTINGS join_algorithm = 'parallel_sorted_merge,hash';
SELECT sum(a.v + b.v) FROM smj_asof_sorted_a AS a ASOF INNER JOIN smj_asof_sorted_b AS b ON a.id = b.id AND a.ts >= b.ts SETTINGS join_algorithm = 'sorted_merge,hash';

DROP TABLE smj_asof_a;
DROP TABLE smj_asof_b;
DROP TABLE smj_asof_sorted_a;
DROP TABLE smj_asof_sorted_b;
