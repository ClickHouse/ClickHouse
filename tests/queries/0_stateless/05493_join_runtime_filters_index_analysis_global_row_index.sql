-- Tests for enable_join_runtime_filters_index_analysis on the global row index `_part_starting_offset + _part_offset`:
-- a self-join on it prunes the probe side to the granules that contain the rows found by the build side.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
-- Pinned (randomized in CI): an exact set of join keys is what index analysis prunes by; a bloom
-- filter (tiny limit or byte budget) prunes nothing.
SET join_runtime_filter_exact_values_limit = 10000, join_runtime_bloom_filter_bytes = 524288;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_join_swap_table = 'false';
SET use_query_condition_cache = 0;
-- The asserted per-query counters would depend on the coordinator's assignment under parallel replicas.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_rf_global_row_index;
CREATE TABLE t_rf_global_row_index (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_rf_global_row_index;
-- Two parts of 1600 rows = 100 granules each.
INSERT INTO t_rf_global_row_index SELECT number, number * 10 FROM numbers(1600);
INSERT INTO t_rf_global_row_index SELECT number + 1600, number * 10 FROM numbers(1600);

-- Rows in both parts: ids 5 (first part, offset 5) and 1700 (second part, offset 100). Only the granule of each row
-- is kept: granule 0 of the first part and granule 6 of the second part.
SELECT a.id, a.v
FROM t_rf_global_row_index AS a,
    (SELECT _part_starting_offset + _part_offset AS g FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
ORDER BY a.id
SETTINGS log_comment = '05493_global_row_index';

-- The operands in the other order are recognized too.
SELECT a.id
FROM t_rf_global_row_index AS a,
    (SELECT _part_offset + _part_starting_offset AS g FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part_offset + a._part_starting_offset = b.g
ORDER BY a.id
SETTINGS log_comment = '05493_global_row_index_swapped';

-- Another key next to the global row index does not stop it from pruning.
SELECT a.id
FROM t_rf_global_row_index AS a,
    (SELECT _part_starting_offset + _part_offset AS g, _part_offset AS o FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part_starting_offset + a._part_offset = b.g AND a._part_offset = b.o
ORDER BY a.id
SETTINGS log_comment = '05493_global_row_index_and_offset';

-- A join on `_part` and `_part_offset` is not pruned by them.
SELECT a.id
FROM t_rf_global_row_index AS a, (SELECT _part, _part_offset FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part = b._part AND a._part_offset = b._part_offset
ORDER BY a.id
SETTINGS log_comment = '05493_part_and_offset';

-- The same with the pruning disabled returns the same rows.
SELECT a.id, a.v
FROM t_rf_global_row_index AS a,
    (SELECT _part_starting_offset + _part_offset AS g FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
ORDER BY a.id
SETTINGS enable_join_runtime_filters_index_analysis = 0;

-- A filter on the probe side that prunes a part keeps the global row indexes of the other parts: the starting offsets
-- are assigned before the part pruning.
SELECT a.id
FROM t_rf_global_row_index AS a,
    (SELECT _part_starting_offset + _part_offset AS g FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part_starting_offset + a._part_offset = b.g AND a._part = 'all_2_2_0'
ORDER BY a.id;

-- More rows than `join_runtime_filter_exact_values_limit`: the filter keeps only the range [5, 1700] of the global row
-- indexes, which covers all 100 granules of the first part (rows 5 to 1599) and granules 0 to 6 of the second part
-- (rows 1600 to 1700).
SELECT a.id
FROM t_rf_global_row_index AS a,
    (SELECT _part_starting_offset + _part_offset AS g FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
ORDER BY a.id
SETTINGS join_runtime_filter_exact_values_limit = 1, log_comment = '05493_global_row_index_range';

-- A physical column named `_part_offset` shadows the virtual one, so its values are not row positions and the join is
-- not pruned by them. The values are chosen so that pruning by row positions would lose id 5: it has the value 1595 but
-- is in granule 0.
DROP TABLE IF EXISTS t_rf_global_row_index_shadowed;
CREATE TABLE t_rf_global_row_index_shadowed (id UInt64, _part_offset UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_rf_global_row_index_shadowed;
INSERT INTO t_rf_global_row_index_shadowed SELECT number, 1600 - number FROM numbers(1600);
INSERT INTO t_rf_global_row_index_shadowed SELECT number + 1600, number FROM numbers(1600);

SELECT a.id
FROM t_rf_global_row_index_shadowed AS a,
    (SELECT _part_starting_offset + _part_offset AS g FROM t_rf_global_row_index_shadowed WHERE id IN (5, 1700)) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
ORDER BY a.id
SETTINGS log_comment = '05493_global_row_index_shadowed';

-- With parallel replicas, only the result is checked: the counters would depend on the coordinator's assignment.
-- Once with the initiator reading its own share, once with only the followers reading.
SELECT a.id
FROM t_rf_global_row_index AS a,
    (SELECT _part_starting_offset + _part_offset AS g FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
ORDER BY a.id
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1;

SELECT a.id
FROM t_rf_global_row_index AS a,
    (SELECT _part_starting_offset + _part_offset AS g FROM t_rf_global_row_index WHERE id IN (5, 1700)) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
ORDER BY a.id
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 0;

-- A column named like the global row index expression, which `CREATE TABLE ... AS SELECT` creates without an alias, or
-- like that expression cast to the join key type, is not taken for the global row index: the primary key and skip
-- indexes do not see the global row index predicate. The join is still pruned by the global row index.
DROP TABLE IF EXISTS t_rf_global_row_index_name_pk;
DROP TABLE IF EXISTS t_rf_global_row_index_name_skip;
CREATE TABLE t_rf_global_row_index_name_pk (id UInt64, `plus(_part_offset, _part_starting_offset)` UInt64)
ENGINE = MergeTree ORDER BY `plus(_part_offset, _part_starting_offset)` SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
CREATE TABLE t_rf_global_row_index_name_skip
(
    id UInt64,
    `plus(_part_offset, _part_starting_offset)` UInt64,
    INDEX idx `plus(_part_offset, _part_starting_offset)` TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
INSERT INTO t_rf_global_row_index_name_pk SELECT number, number + 10000 FROM numbers(1600);
DROP TABLE IF EXISTS t_rf_global_row_index_cast_name_pk;
DROP TABLE IF EXISTS t_rf_global_row_index_cast_name_skip;
CREATE TABLE t_rf_global_row_index_cast_name_pk (id UInt64, `_CAST(plus(_part_offset, _part_starting_offset), 'Int128')` Int128)
ENGINE = MergeTree ORDER BY `_CAST(plus(_part_offset, _part_starting_offset), 'Int128')`
SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
CREATE TABLE t_rf_global_row_index_cast_name_skip
(
    id UInt64,
    `_CAST(plus(_part_offset, _part_starting_offset), 'Int128')` Int128,
    INDEX idx (id, `_CAST(plus(_part_offset, _part_starting_offset), 'Int128')`) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
INSERT INTO t_rf_global_row_index_cast_name_pk SELECT number, number + 10000 FROM numbers(1600);
INSERT INTO t_rf_global_row_index_cast_name_skip SELECT number, number + 10000 FROM numbers(1600);
INSERT INTO t_rf_global_row_index_name_skip SELECT number, number + 10000 FROM numbers(1600);

SELECT a.id FROM t_rf_global_row_index_name_pk AS a, (SELECT toUInt64(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS log_comment = '05493_global_row_index_name_collision_pk';

SELECT a.id FROM t_rf_global_row_index_name_pk AS a, (SELECT toUInt64(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS join_runtime_filter_exact_values_limit = 0, log_comment = '05493_global_row_index_name_collision_pk_range';

SELECT a.id FROM t_rf_global_row_index_name_skip AS a, (SELECT toUInt64(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS log_comment = '05493_global_row_index_name_collision_skip';

SELECT a.id FROM t_rf_global_row_index_cast_name_pk AS a, (SELECT toInt128(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS log_comment = '05493_global_row_index_cast_name_collision_pk';

SELECT a.id FROM t_rf_global_row_index_cast_name_pk AS a, (SELECT toInt128(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS join_runtime_filter_exact_values_limit = 0, log_comment = '05493_global_row_index_cast_name_collision_pk_range';

SELECT a.id FROM t_rf_global_row_index_cast_name_skip AS a, (SELECT toInt128(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS log_comment = '05493_global_row_index_cast_name_collision_skip';

-- Seal-gated reading prunes by primary key columns only: a primary key column named like the display name of the global
-- row index key, `_part_starting_offset + _part_offset`, is not taken for it, with or without index analysis.
DROP TABLE IF EXISTS t_rf_global_row_index_seal;
CREATE TABLE t_rf_global_row_index_seal
(
    id UInt64,
    `_part_starting_offset + _part_offset` UInt64,
    `plus(_part_offset, _part_starting_offset)` UInt64
)
ENGINE = MergeTree ORDER BY (`_part_starting_offset + _part_offset`, `plus(_part_offset, _part_starting_offset)`)
SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
INSERT INTO t_rf_global_row_index_seal SELECT number, 0, number + 10000 FROM numbers(1600);

SELECT 'seal_gated', a.id FROM t_rf_global_row_index_seal AS a, (SELECT toUInt64(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS enable_join_seal_gated_reading = 1;

SELECT 'seal_gated_no_index_analysis', a.id FROM t_rf_global_row_index_seal AS a, (SELECT toUInt64(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS enable_join_seal_gated_reading = 1, enable_join_runtime_filters_index_analysis = 0;

SELECT 'not_seal_gated', a.id FROM t_rf_global_row_index_seal AS a, (SELECT toUInt64(5) AS g) AS b
WHERE a._part_starting_offset + a._part_offset = b.g
SETTINGS enable_join_seal_gated_reading = 0;

SYSTEM FLUSH LOGS query_log;

-- A part can be analyzed more than once by a query (when its index read result is rebuilt), which adds its granules
-- to the counters again, so exact counts are not checked. With exact values, each part keeps 1 of its 100 granules,
-- so the kept share is 1% however many times a part is analyzed.
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterIndexAnalysisReads'], event_time_microseconds) > 0,
    intDivOrZero(
        (argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time_microseconds)
            - argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds)) * 100,
        argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time_microseconds))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05493_global_row_index', '05493_global_row_index_swapped', '05493_global_row_index_and_offset',
        '05493_part_and_offset', '05493_global_row_index_shadowed', '05493_global_row_index_name_collision_pk',
        '05493_global_row_index_name_collision_pk_range', '05493_global_row_index_name_collision_skip',
        '05493_global_row_index_cast_name_collision_pk', '05493_global_row_index_cast_name_collision_skip')
GROUP BY log_comment
ORDER BY log_comment;

-- The range keeps a different share of each part, so only check that it drops some granules and keeps some.
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterIndexAnalysisReads'], event_time_microseconds) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds)
        < argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time_microseconds)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = '05493_global_row_index_range'
GROUP BY log_comment;

DROP TABLE t_rf_global_row_index;
DROP TABLE t_rf_global_row_index_shadowed;
DROP TABLE t_rf_global_row_index_name_pk;
DROP TABLE t_rf_global_row_index_name_skip;
DROP TABLE t_rf_global_row_index_cast_name_pk;
DROP TABLE t_rf_global_row_index_cast_name_skip;
DROP TABLE t_rf_global_row_index_seal;
