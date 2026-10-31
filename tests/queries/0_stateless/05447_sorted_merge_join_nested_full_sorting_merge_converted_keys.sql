-- A nested `full_sorting_merge` join whose keys have different types sorts the converted keys itself, so its output
-- is still ordered by the key that is not converted (`b.id` here). The runtime-filter pass predicts that order
-- (`predictMergeJoinOutputOrder`) to leave the `sorted_merge` join above it alone. It must not take the conversion
-- in the nested join for a missing order: the join above would get a runtime filter, and planting the filter
-- erases its merge algorithms, turning it into a hash join. See PR #112973 review.

DROP TABLE IF EXISTS fsm_conv_a;
DROP TABLE IF EXISTS fsm_conv_b;
DROP TABLE IF EXISTS fsm_conv_c;

CREATE TABLE fsm_conv_a (id UInt32, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE fsm_conv_b (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE fsm_conv_c (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO fsm_conv_a SELECT number, number FROM numbers(4000);
INSERT INTO fsm_conv_b SELECT number, number FROM numbers(4000);
INSERT INTO fsm_conv_c SELECT number, number FROM numbers(4000);

-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, max_threads = 4, enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

-- Both joins are merge joins, and the join above needs no full sort and gets no runtime filter.
SELECT 'converted_keys', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%RuntimeFilter%') = 0
FROM (EXPLAIN PIPELINE
    SELECT sum(s.v + c.v)
    FROM
    (
        SELECT b.id AS id, a.v AS v
        FROM fsm_conv_a AS a INNER JOIN fsm_conv_b AS b ON a.id = b.id
        SETTINGS join_algorithm = 'full_sorting_merge'
    ) AS s
    INNER JOIN fsm_conv_c AS c ON s.id = c.id
    SETTINGS join_algorithm = 'sorted_merge,hash');

-- The left key `a.id` is the converted one, so a join above on it cannot rely on the order of the nested join:
-- it falls through to `hash` and gets a runtime filter.
SELECT 'converted_key_used_above', countIf(explain LIKE '%MergeJoinTransform%') = 1, countIf(explain LIKE '%RuntimeFilter%') > 0
FROM (EXPLAIN PIPELINE
    SELECT sum(s.v + c.v)
    FROM
    (
        SELECT a.id AS id, a.v AS v
        FROM fsm_conv_a AS a INNER JOIN fsm_conv_b AS b ON a.id = b.id
        SETTINGS join_algorithm = 'full_sorting_merge'
    ) AS s
    INNER JOIN fsm_conv_a AS c ON s.id = c.id
    SETTINGS join_algorithm = 'sorted_merge,hash');

-- The result matches a plain `hash` join.
SELECT 'converted_keys_result',
    (SELECT sum(s.v + c.v) FROM (SELECT b.id AS id, a.v AS v FROM fsm_conv_a AS a INNER JOIN fsm_conv_b AS b ON a.id = b.id SETTINGS join_algorithm = 'full_sorting_merge') AS s
        INNER JOIN fsm_conv_c AS c ON s.id = c.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT sum(s.v + c.v) FROM (SELECT b.id AS id, a.v AS v FROM fsm_conv_a AS a INNER JOIN fsm_conv_b AS b ON a.id = b.id SETTINGS join_algorithm = 'hash') AS s
        INNER JOIN fsm_conv_c AS c ON s.id = c.id SETTINGS join_algorithm = 'hash');

DROP TABLE fsm_conv_a;
DROP TABLE fsm_conv_b;
DROP TABLE fsm_conv_c;
