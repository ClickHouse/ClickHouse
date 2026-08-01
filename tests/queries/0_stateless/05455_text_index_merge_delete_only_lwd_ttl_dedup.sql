-- Merges applying lightweight deletes, TTL DELETE and OPTIMIZE ... DEDUPLICATE only drop rows,
-- so text indexes are merged with remapping of row ids instead of being rebuilt.

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET lightweight_deletes_sync = 2;

SELECT 'lightweight delete, horizontal merge';

DROP TABLE IF EXISTS t_text_lwd;

CREATE TABLE t_text_lwd
(
    id UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1, enable_vertical_merge_algorithm = 0;

INSERT INTO t_text_lwd SELECT number, 'first bucket row' || toString(number) FROM numbers(5000);
INSERT INTO t_text_lwd SELECT number, 'second bucket row' || toString(number) FROM numbers(5000, 5000);

DELETE FROM t_text_lwd WHERE id % 5 = 0;

OPTIMIZE TABLE t_text_lwd FINAL;

SELECT count() FROM t_text_lwd;
SELECT count() FROM t_text_lwd WHERE hasToken(text, 'first') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_lwd WHERE hasToken(text, 'second') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_lwd WHERE hasToken(text, 'row9105') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_lwd WHERE hasToken(text, 'row9104') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'], merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_lwd' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_lwd;

SELECT 'lightweight delete, vertical merge';

DROP TABLE IF EXISTS t_text_lwd_vertical;

CREATE TABLE t_text_lwd_vertical
(
    id UInt64,
    v UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_optimize_lightweight_delete = 1;

INSERT INTO t_text_lwd_vertical SELECT number, number, 'first bucket row' || toString(number) FROM numbers(5000);
INSERT INTO t_text_lwd_vertical SELECT number, number, 'second bucket row' || toString(number) FROM numbers(5000, 5000);

DELETE FROM t_text_lwd_vertical WHERE id % 5 = 0;

OPTIMIZE TABLE t_text_lwd_vertical FINAL;

SELECT count() FROM t_text_lwd_vertical;
SELECT count() FROM t_text_lwd_vertical WHERE hasToken(text, 'first') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_lwd_vertical WHERE hasToken(text, 'row9105') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_lwd_vertical WHERE hasToken(text, 'row9104') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'], merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_lwd_vertical' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_lwd_vertical;

SELECT 'lightweight delete on a part without materialized index falls back to rebuild';

DROP TABLE IF EXISTS t_text_lwd_missing;

CREATE TABLE t_text_lwd_missing
(
    id UInt64,
    text String
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1, enable_vertical_merge_algorithm = 0;

INSERT INTO t_text_lwd_missing SELECT number, 'first bucket row' || toString(number) FROM numbers(5000);

ALTER TABLE t_text_lwd_missing ADD INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;

INSERT INTO t_text_lwd_missing SELECT number, 'second bucket row' || toString(number) FROM numbers(5000, 5000);

DELETE FROM t_text_lwd_missing WHERE id % 5 = 0;

OPTIMIZE TABLE t_text_lwd_missing FINAL;

SELECT count() FROM t_text_lwd_missing;
SELECT count() FROM t_text_lwd_missing WHERE hasToken(text, 'first') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_lwd_missing WHERE hasToken(text, 'second') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_lwd_missing' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_lwd_missing;

SELECT 'TTL DELETE';

DROP TABLE IF EXISTS t_text_ttl;

CREATE TABLE t_text_ttl
(
    id UInt64,
    d DateTime,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_text_ttl SELECT number, if(number % 3 = 0, toDateTime('2001-01-01 00:00:00'), toDateTime('2101-01-01 00:00:00')), 'payload row' || toString(number) FROM numbers(3000);
INSERT INTO t_text_ttl SELECT number, if(number % 3 = 0, toDateTime('2001-01-01 00:00:00'), toDateTime('2101-01-01 00:00:00')), 'payload row' || toString(number) FROM numbers(3000, 3000);

ALTER TABLE t_text_ttl MODIFY TTL d SETTINGS materialize_ttl_after_modify = 0;

OPTIMIZE TABLE t_text_ttl FINAL;

SELECT count() FROM t_text_ttl;
SELECT count() FROM t_text_ttl WHERE hasToken(text, 'payload') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_ttl WHERE hasToken(text, 'row3333') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_ttl WHERE hasToken(text, 'row3334') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_ttl' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_ttl;

SELECT 'column TTL falls back to rebuild';

DROP TABLE IF EXISTS t_text_column_ttl;

CREATE TABLE t_text_column_ttl
(
    id UInt64,
    d DateTime,
    junk String TTL d + INTERVAL 100 YEAR,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_text_column_ttl SELECT number, if(number % 3 = 0, toDateTime('2001-01-01 00:00:00'), toDateTime('2101-01-01 00:00:00')), 'junk', 'payload row' || toString(number) FROM numbers(3000);
INSERT INTO t_text_column_ttl SELECT number, if(number % 3 = 0, toDateTime('2001-01-01 00:00:00'), toDateTime('2101-01-01 00:00:00')), 'junk', 'payload row' || toString(number) FROM numbers(3000, 3000);

ALTER TABLE t_text_column_ttl MODIFY TTL d SETTINGS materialize_ttl_after_modify = 0;

OPTIMIZE TABLE t_text_column_ttl FINAL;

SELECT count() FROM t_text_column_ttl;
SELECT count() FROM t_text_column_ttl WHERE hasToken(text, 'payload') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_column_ttl' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_column_ttl;

SELECT 'OPTIMIZE DEDUPLICATE';

DROP TABLE IF EXISTS t_text_dedup;

CREATE TABLE t_text_dedup
(
    id UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_text_dedup SELECT number, 'payload row' || toString(number) FROM numbers(3000);
INSERT INTO t_text_dedup SELECT number, 'payload row' || toString(number) FROM numbers(0, 3000, 2);

OPTIMIZE TABLE t_text_dedup FINAL DEDUPLICATE;

SELECT count() FROM t_text_dedup;
SELECT count() FROM t_text_dedup WHERE hasToken(text, 'payload') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_dedup WHERE hasToken(text, 'row2222') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_dedup' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_dedup;
