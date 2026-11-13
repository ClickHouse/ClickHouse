-- A table column named `_part_index` shadows the virtual column that a text-index merge used to map rows.

DROP TABLE IF EXISTS t_horizontal;
DROP TABLE IF EXISTS t_vertical;

CREATE TABLE t_horizontal (id UInt64, _part_index UInt64, text String, INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, materialize_skip_indexes_on_merge = 1, enable_vertical_merge_algorithm = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE t_vertical (id UInt64, _part_index UInt64, text String, INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, materialize_skip_indexes_on_merge = 1, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_horizontal SELECT number, 1, 'alpha' FROM numbers(1000);
INSERT INTO t_horizontal SELECT number + 1000, 0, 'beta' FROM numbers(1000);
INSERT INTO t_vertical SELECT number, 1, 'alpha' FROM numbers(1000);
INSERT INTO t_vertical SELECT number + 1000, 0, 'beta' FROM numbers(1000);

OPTIMIZE TABLE t_horizontal FINAL;
OPTIMIZE TABLE t_vertical FINAL;

SYSTEM FLUSH LOGS part_log;
SELECT table, merge_algorithm FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase() AND event_type = 'MergeParts' ORDER BY table;

SELECT 'horizontal', count(), sum(_part_index) FROM t_horizontal;
SELECT 'horizontal', count(), countIf(text = 'beta'), min(id), max(id) FROM t_horizontal WHERE hasToken(text, 'beta')
SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_text';
SELECT 'vertical', count(), sum(_part_index) FROM t_vertical;
SELECT 'vertical', count(), countIf(text = 'alpha'), min(id), max(id) FROM t_vertical WHERE hasToken(text, 'alpha')
SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_text';
SELECT 'vertical', count(), countIf(text = 'beta'), min(id), max(id) FROM t_vertical WHERE hasToken(text, 'beta')
SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_text';

DROP TABLE t_horizontal;
DROP TABLE t_vertical;
