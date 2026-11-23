-- The query condition cache key of a read is built from its `filter_actions_dag`, collected at index analysis, while
-- `updateQueryConditionCache` may attach it again later to the `FilterStep` above the read. With
-- `query_plan_filter_push_down = 0` the outer `WHERE s.b = 2` stays above the join during the collection, so the key
-- describes only `a = 1`. Join runtime filters can then push the filter down regardless of the setting and merge
-- `s.b = 2` into the filter right above the read. That filter must not be tagged with the key of `a = 1`: otherwise
-- granules in which every `a = 1` row has `b = 3` get recorded as not matching `a = 1`, and the last-but-one query
-- returns 0. The correct counts below do not depend on the plan, so they hold whether or not the merge happens.
-- Taken from https://github.com/ClickHouse/ClickHouse/pull/122271.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_filter_push_down = 0;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS dim;

-- `a = 1` holds for a tenth of the rows of every granule, `b = 2` only in the first half of the table.
CREATE TABLE tab (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab SELECT number % 10, if(number < 5000, 2, 3), number FROM numbers(10000);

CREATE TABLE dim (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dim SELECT number FROM numbers(10);

-- Small blocks, so that granules where every `a = 1` row has `b = 3` reach the filter as chunks of their own.
SELECT count() FROM dim AS d JOIN (SELECT a, b FROM tab WHERE a = 1) AS s ON d.x = s.a WHERE s.b = 2
SETTINGS max_block_size = 100;

-- Print both counts rather than a boolean, so a reference diff shows which way it broke.
SELECT count() FROM dim AS d JOIN (SELECT a, b FROM tab WHERE a = 1) AS s ON d.x = s.a WHERE s.b = 3
SETTINGS use_query_condition_cache = 1;
SELECT count() FROM dim AS d JOIN (SELECT a, b FROM tab WHERE a = 1) AS s ON d.x = s.a WHERE s.b = 3
SETTINGS use_query_condition_cache = 0;

DROP TABLE dim;
DROP TABLE tab;
