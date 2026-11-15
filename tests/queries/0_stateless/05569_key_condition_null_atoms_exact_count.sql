-- Tags: no-random-merge-tree-settings
-- Random MergeTree settings change the granules that the row limits depend on.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET enable_parallel_replicas = 0, use_query_condition_cache = 0;
SET merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;

-- `has` is never NULL, so its atoms on `ifNull(x, 0)` and `tuple(x)` find the ranges without a 5.
DROP TABLE IF EXISTS t_hidden_null_has;
CREATE TABLE t_hidden_null_has (x Nullable(UInt32)) ENGINE = MergeTree ORDER BY (ifNull(x, 0), tuple(x))
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', allow_nullable_key = 1;
INSERT INTO t_hidden_null_has SELECT if(number % 20 = 0, NULL, number) FROM numbers(100);

SELECT count() FROM t_hidden_null_has WHERE NOT has([5], x) SETTINGS max_rows_to_read = 20;

-- `assumeNotNull(x)` returns the value stored under a NULL `x`, and this INSERT stores the row number there. So
-- `nullIf(assumeNotNull(x), 0)` is NULL for the first row only, and an ordinary value for the other rows with a NULL `x`.
DROP TABLE IF EXISTS t_hidden_null_value;
CREATE TABLE t_hidden_null_value (x Nullable(Int32)) ENGINE = MergeTree ORDER BY (nullIf(assumeNotNull(x), 0), x)
SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_hidden_null_value SELECT nullIf(toInt32(number), if(number % 7 = 0, toInt32(number), -1)) FROM numbers(100);

SELECT count() FROM t_hidden_null_value WHERE x != 5;

-- The primary key keeps only `ifNull(x, 0)` in memory, which stores a NULL `x` as 0. `x != 5` is NULL on those rows,
-- and no range shows it.
DROP TABLE IF EXISTS t_unloaded_hidden_null;
CREATE TABLE t_unloaded_hidden_null (x Nullable(Int32)) ENGINE = MergeTree ORDER BY (ifNull(x, 0), x)
SETTINGS index_granularity = 1, allow_nullable_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9;
INSERT INTO t_unloaded_hidden_null SELECT if(number % 20 = 0, NULL, number) FROM numbers(100);

SELECT count() FROM t_unloaded_hidden_null WHERE x != 5 SETTINGS use_lightweight_primary_key_index_analysis = 1;

DROP TABLE t_hidden_null_has;
DROP TABLE t_hidden_null_value;
DROP TABLE t_unloaded_hidden_null;
