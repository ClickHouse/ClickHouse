-- Tags: no-parallel-replicas
-- no-parallel-replicas: reading with parallel replicas disables the split of the join by primary key ranges.

DROP TABLE IF EXISTS fsm_sq_1;
DROP TABLE IF EXISTS fsm_sq_2;
DROP TABLE IF EXISTS fsm_sq_3;
DROP TABLE IF EXISTS fsm_sq_left;
DROP TABLE IF EXISTS fsm_sq_right;
DROP TABLE IF EXISTS fsm_sq_blocks;

CREATE TABLE fsm_sq_1 (id UInt64, v1 UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, max_bytes_to_merge_at_max_space_in_pool = 1;
CREATE TABLE fsm_sq_2 (id UInt64, v2 UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, max_bytes_to_merge_at_max_space_in_pool = 1;
CREATE TABLE fsm_sq_3 (id UInt64, v3 UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, max_bytes_to_merge_at_max_space_in_pool = 1;

-- Parts with different borders in each table.
INSERT INTO fsm_sq_1 SELECT number, number FROM numbers(0, 30000);
INSERT INTO fsm_sq_1 SELECT number, number FROM numbers(30000, 70000);
INSERT INTO fsm_sq_2 SELECT number, number * 2 FROM numbers(0, 55000);
INSERT INTO fsm_sq_2 SELECT number, number * 2 FROM numbers(55000, 45000);
INSERT INTO fsm_sq_3 SELECT number, number * 3 FROM numbers(0, 20000);
INSERT INTO fsm_sq_3 SELECT number, number * 3 FROM numbers(20000, 60000);
INSERT INTO fsm_sq_3 SELECT number, number * 3 FROM numbers(80000, 20000);

-- Both joins are split by primary key ranges, and the output of each shard is squashed.
SELECT countIf(explain ILIKE '%ProgressiveSquashingTransform%'), countIf(explain ILIKE '%MergeJoinTransform ×%')
FROM
(
    EXPLAIN PIPELINE graph = 1
    SELECT count() FROM fsm_sq_1 JOIN fsm_sq_2 USING (id) JOIN fsm_sq_3 USING (id)
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1, optimize_read_in_order = 1,
        optimize_sorting_by_input_stream_properties = 1,
        query_plan_optimize_join_order_limit = 0, query_plan_optimize_join_order_randomize = 0, max_threads = 4,
        min_joined_block_size_rows = 65409, min_joined_block_size_bytes = 524288
);

-- No squashing when both minimum sizes are zero.
SELECT countIf(explain ILIKE '%ProgressiveSquashingTransform%'), countIf(explain ILIKE '%MergeJoinTransform ×%')
FROM
(
    EXPLAIN PIPELINE graph = 1
    SELECT count() FROM fsm_sq_1 JOIN fsm_sq_2 USING (id) JOIN fsm_sq_3 USING (id)
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1, optimize_read_in_order = 1,
        optimize_sorting_by_input_stream_properties = 1,
        query_plan_optimize_join_order_limit = 0, query_plan_optimize_join_order_randomize = 0, max_threads = 4,
        min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0
);

-- The same rows with and without squashing.
SELECT count(), sum(v1), sum(v2), sum(v3) FROM fsm_sq_1 JOIN fsm_sq_2 USING (id) JOIN fsm_sq_3 USING (id)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1, optimize_read_in_order = 1,
    optimize_sorting_by_input_stream_properties = 1,
    query_plan_optimize_join_order_limit = 0, query_plan_optimize_join_order_randomize = 0, max_threads = 4,
    min_joined_block_size_rows = 65409, min_joined_block_size_bytes = 524288;

SELECT count(), sum(v1), sum(v2), sum(v3) FROM fsm_sq_1 JOIN fsm_sq_2 USING (id) JOIN fsm_sq_3 USING (id)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1, optimize_read_in_order = 1,
    optimize_sorting_by_input_stream_properties = 1,
    query_plan_optimize_join_order_limit = 0, query_plan_optimize_join_order_randomize = 0, max_threads = 4,
    min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

-- The joins return fewer, larger blocks with squashing.
CREATE TABLE fsm_sq_blocks (squash UInt8, blocks UInt64) ENGINE = Memory;

INSERT INTO fsm_sq_blocks SELECT 1, round(sum(1 / blockSize())) FROM fsm_sq_1 JOIN fsm_sq_2 USING (id) JOIN fsm_sq_3 USING (id)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1, optimize_read_in_order = 1,
    optimize_sorting_by_input_stream_properties = 1,
    query_plan_optimize_join_order_limit = 0, query_plan_optimize_join_order_randomize = 0, max_threads = 4,
    max_block_size = 1000, min_joined_block_size_rows = 65409, min_joined_block_size_bytes = 524288;

INSERT INTO fsm_sq_blocks SELECT 0, round(sum(1 / blockSize())) FROM fsm_sq_1 JOIN fsm_sq_2 USING (id) JOIN fsm_sq_3 USING (id)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1, optimize_read_in_order = 1,
    optimize_sorting_by_input_stream_properties = 1,
    query_plan_optimize_join_order_limit = 0, query_plan_optimize_join_order_randomize = 0, max_threads = 4,
    max_block_size = 1000, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

SELECT (SELECT blocks FROM fsm_sq_blocks WHERE squash = 1) < (SELECT blocks FROM fsm_sq_blocks WHERE squash = 0);

-- A LIMIT above the join gets the first rows without waiting for the rest of the left table.
CREATE TABLE fsm_sq_left (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
CREATE TABLE fsm_sq_right (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO fsm_sq_left SELECT number FROM numbers(1000000);
INSERT INTO fsm_sq_right SELECT number * 10000 FROM numbers(100);

SELECT id FROM fsm_sq_left JOIN fsm_sq_right USING (id) LIMIT 1 FORMAT Null
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1, optimize_read_in_order = 1,
    optimize_sorting_by_input_stream_properties = 1,
    query_plan_optimize_join_order_randomize = 0, max_threads = 4, max_block_size = 8192,
    min_joined_block_size_rows = 65409, min_joined_block_size_bytes = 524288, log_comment = '05870_full_sorting_merge_join_limit';

SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 500000 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05870_full_sorting_merge_join_limit' AND type = 'QueryFinish';

DROP TABLE fsm_sq_1;
DROP TABLE fsm_sq_2;
DROP TABLE fsm_sq_3;
DROP TABLE fsm_sq_left;
DROP TABLE fsm_sq_right;
DROP TABLE fsm_sq_blocks;
