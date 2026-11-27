-- The block inserted into the source table of a materialized view has a known row count,
-- so a join of the block with a larger table builds the hash table from the block.
-- The runtime filter built from the block then skips the granules of the larger table.

SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_parallel_replicas = 0;
SET use_hash_table_stats_for_join_reordering = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dim;

CREATE TABLE dim (id UInt64, name String) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
INSERT INTO dim SELECT number, 'name' || toString(number) FROM numbers(1000);

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst (id UInt64, name String) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv TO dst AS
SELECT e.id AS id, d.name AS name FROM src AS e LEFT JOIN dim AS d ON e.id = d.id;

-- The block is larger than `dim`: the sides are kept.
INSERT INTO src SELECT number FROM numbers(5000) SETTINGS log_comment = 'block_larger_than_dim';
-- The block is smaller than `dim`: half of its keys have no match, the other half is in the last of 10 granules.
INSERT INTO src SELECT 990 + number FROM numbers(20) SETTINGS log_comment = 'block_smaller_than_dim';

SELECT count(), countIf(name = '') FROM dst;

-- A larger block after a smaller one: the hash table size of the smaller block is not reused.
INSERT INTO src SELECT number FROM numbers(5000) SETTINGS log_comment = 'block_larger_than_dim_again';
-- Without the index analysis the runtime filter skips no granules.
INSERT INTO src SELECT 990 + number FROM numbers(20)
SETTINGS log_comment = 'block_smaller_than_dim_no_index_analysis', enable_join_runtime_filters_index_analysis = 0;

SYSTEM FLUSH LOGS query_log;

SELECT DISTINCT log_comment, ProfileEvents['JoinBuildTableRowCount'], ProfileEvents['SelectedMarks']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment LIKE 'block_%_than_dim%'
ORDER BY log_comment;

DROP TABLE mv;
DROP TABLE src;
DROP TABLE dst;
DROP TABLE dim;
