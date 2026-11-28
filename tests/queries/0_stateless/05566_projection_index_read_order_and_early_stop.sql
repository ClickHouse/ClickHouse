-- Projection indexes are read starting from the one with the fewest rows, the reading stops once the
-- intersection is empty, and they are not read at all when the skip indexes already exclude the part.

-- Use the projections as indexes, for any table size and any projection size.
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;
SET max_projection_rows_to_use_projection_index = 1000000;
-- The skip index has to be evaluated at read time, before the projection indexes. A throwing max_rows_to_read
-- (the CI profile sets one) would evaluate it at planning time instead.
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET max_rows_to_read = 0;
-- A cached condition would skip the reads this test counts.
SET use_query_condition_cache = 0;
-- The skip index is not evaluated at read time with parallel replicas.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_proj_idx;

-- by_a is the least selective projection; it is defined between the selective ones.
CREATE TABLE t_proj_idx
(
    a UInt64,
    b UInt64,
    c UInt64,
    d UInt64,
    payload String,
    INDEX c_idx c TYPE minmax GRANULARITY 1,
    PROJECTION by_b (SELECT _part_offset ORDER BY b),
    PROJECTION by_a (SELECT _part_offset ORDER BY a),
    PROJECTION by_d (SELECT _part_offset ORDER BY d)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_proj_idx SELECT number % 10, number * 2, number, number, toString(number) FROM numbers(100000);
OPTIMIZE TABLE t_proj_idx FINAL;

-- by_b selects one granule and no row: by_a must not be read.
SELECT count(), sum(length(payload)) FROM t_proj_idx WHERE a = 1 AND b = 3 SETTINGS log_comment = '05566_empty_first';
SELECT count(), sum(length(payload)) FROM t_proj_idx WHERE a = 1 AND b = 3 SETTINGS optimize_use_projection_filtering = 0;

-- by_b and by_d have disjoint offsets: by_a must not be read.
SELECT count(), sum(length(payload)) FROM t_proj_idx WHERE a = 1 AND b < 200 AND d BETWEEN 200 AND 300 SETTINGS log_comment = '05566_empty_intersection';
SELECT count(), sum(length(payload)) FROM t_proj_idx WHERE a = 1 AND b < 200 AND d BETWEEN 200 AND 300 SETTINGS optimize_use_projection_filtering = 0;

-- The skip index excludes every granule: no projection must be read.
-- Without use_statistics_for_part_pruning = 0 the column statistics exclude the part before reading.
SELECT count(), sum(length(payload)) FROM t_proj_idx WHERE a = 1 AND b < 2000 AND c = 200000 SETTINGS use_statistics_for_part_pruning = 0, log_comment = '05566_excluded_by_skip_index';
SELECT count(), sum(length(payload)) FROM t_proj_idx WHERE a = 1 AND b < 2000 AND c = 200000 SETTINGS use_statistics_for_part_pruning = 0, optimize_use_projection_filtering = 0;

-- Non-empty intersection: both projections are read.
SELECT count(), sum(length(payload)) FROM t_proj_idx WHERE a = 1 AND b < 2000 SETTINGS log_comment = '05566_non_empty';
SELECT count(), sum(length(payload)) FROM t_proj_idx WHERE a = 1 AND b < 2000 SETTINGS optimize_use_projection_filtering = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, multiIf(
        log_comment = '05566_empty_first', rows_read <= 1000,
        log_comment = '05566_empty_intersection', rows_read <= 2000,
        log_comment = '05566_excluded_by_skip_index', rows_read = 0 AND selected_parts >= 1,
        rows_read >= 10000)
FROM
(
    SELECT log_comment, ProfileEvents['RowsReadByPrewhereReaders'] + ProfileEvents['RowsReadByMainReader'] AS rows_read,
        ProfileEvents['SelectedParts'] AS selected_parts
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('05566_empty_first', '05566_empty_intersection', '05566_excluded_by_skip_index', '05566_non_empty')
)
ORDER BY log_comment;

DROP TABLE t_proj_idx;
