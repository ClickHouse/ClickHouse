-- `getCustomKeyFilterForParallelReplica` turns `parallel_replicas_custom_key_range_lower/_upper` into a
-- `[lower, upper)` filter over the custom key. `parallel_replicas_custom_key_range_upper` is documented in
-- `src/Core/Settings.cpp` as "Allows the filter type `range` to split the work evenly between replicas based
-- on the custom range `[0, parallel_replicas_custom_key_range_upper]`. A value of 0 disables the upper bound,
-- setting it the max value of the custom key expression." A bound that does not fit into the type of the key,
-- a range that is empty, or a range that cannot be split between the replicas therefore have to be reported
-- instead of silently handing a replica a slice that does not hold the rows it is supposed to read.
--
-- `parallel_replicas_count` and `parallel_replica_offset` are what an initiator sends to a replica, so a single
-- server builds the filter for its own slice and no cluster is needed (same as
-- 05356_projection_calculation_ignores_parallel_replicas_custom_key).

DROP TABLE IF EXISTS t_custom_key_range;

CREATE TABLE t_custom_key_range (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_custom_key_range SELECT number FROM numbers(100);

-- `parallel_replicas_for_non_replicated_merge_tree = 0` keeps the read of this plain `MergeTree` local. With it
-- enabled the local plan is replaced by a read of `cluster_for_parallel_replicas` at a stage which returns one
-- unmerged row per replica, so the last query would print a row per replica instead of a single count.
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, automatic_parallel_replicas_mode = 0,
    parallel_replicas_for_non_replicated_merge_tree = 0,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_count = 2, parallel_replica_offset = 0,
    parallel_replicas_custom_key_range_lower = 0;

-- An upper bound above the largest value the type of the key can hold. Every unsigned type has its own check,
-- because the bound it is compared against is the max value of that type.
SELECT count() FROM t_custom_key_range
SETTINGS parallel_replicas_custom_key = 'toUInt32(k)',
    parallel_replicas_custom_key_range_upper = 4294967296; -- { serverError INVALID_SETTING_VALUE }

SELECT count() FROM t_custom_key_range
SETTINGS parallel_replicas_custom_key = 'toUInt16(k)',
    parallel_replicas_custom_key_range_upper = 65536; -- { serverError INVALID_SETTING_VALUE }

SELECT count() FROM t_custom_key_range
SETTINGS parallel_replicas_custom_key = 'toUInt8(k)',
    parallel_replicas_custom_key_range_upper = 256; -- { serverError INVALID_SETTING_VALUE }

-- A signed key is not an unsigned integer type, so no upper bound can be derived from it at all.
SELECT count() FROM t_custom_key_range
SETTINGS parallel_replicas_custom_key = 'toInt32(k)',
    parallel_replicas_custom_key_range_upper = 50; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

-- The range is empty: its upper bound is below its lower bound.
SELECT count() FROM t_custom_key_range
SETTINGS parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 100,
    parallel_replicas_custom_key_range_upper = 10; -- { serverError INVALID_SETTING_VALUE }

-- `[0, 1]` holds 2 values, which is not more than the 2 replicas, so a replica would get an empty slice.
SELECT count() FROM t_custom_key_range
SETTINGS parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_upper = 1; -- { serverError INVALID_SETTING_VALUE }

-- A bound that fits: replica 0 of 2 gets the lower half of `[0, 99]`, so 50 of the 100 rows.
SELECT count() FROM t_custom_key_range
SETTINGS parallel_replicas_custom_key = 'toUInt32(k)',
    parallel_replicas_custom_key_range_upper = 99;

DROP TABLE t_custom_key_range;
