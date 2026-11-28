-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs the sz3 library
-- no-random-settings: a randomized `max_compress_block_size` that is not a multiple of the float width
-- is rejected by the SZ3 codec at write time
-- no-random-merge-tree-settings: the test is about the default of `add_minmax_index_for_numeric_columns`

-- An index is calculated from the values before compression. A lossy codec (`SZ3`) returns other values,
-- so an implicit min-max index over such a column would prune rows that match. Such columns, and the
-- `ALIAS` columns that read them, get no implicit index.

SET enable_sz3_codec = 1;
SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0, use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_lossy;
DROP TABLE IF EXISTS t_lossy_alter;
DROP TABLE IF EXISTS t_lossy_replicated;

CREATE TABLE t_lossy
(
    k UInt64,
    orig Float64,
    x Float64 CODEC(SZ3('ALGO_LORENZO_REG', 'ABS', 0.01)),
    y Float64 ALIAS x * 2,
    z Float64 ALIAS orig * 2
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_lossy SELECT number, number * 0.5 + sin(number * 0.1), number * 0.5 + sin(number * 0.1) FROM numbers(100000);

SELECT 'create';
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_lossy' ORDER BY name;

-- The codec has changed some values, and a row is found by the value it returns.
SELECT count() > 0 FROM t_lossy WHERE x != orig SETTINGS use_skip_indexes = 0;

WITH (SELECT tuple(k, x) FROM t_lossy WHERE x != orig ORDER BY k LIMIT 1 SETTINGS use_skip_indexes = 0) AS w
SELECT count() FROM t_lossy WHERE k = w.1 AND x = w.2;

WITH (SELECT tuple(k, y) FROM t_lossy WHERE x != orig ORDER BY k LIMIT 1 SETTINGS use_skip_indexes = 0) AS w
SELECT count() FROM t_lossy WHERE k = w.1 AND y = w.2;

SELECT 'reload';
DETACH TABLE t_lossy;
ATTACH TABLE t_lossy;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_lossy' ORDER BY name;

SELECT 'add column';
ALTER TABLE t_lossy ADD COLUMN v Float64 CODEC(SZ3('ALGO_LORENZO_REG', 'ABS', 0.01)), ADD COLUMN w Float64;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_lossy' ORDER BY name;

-- A codec that becomes lossy removes the implicit index of the column and of the aliases that read it.
CREATE TABLE t_lossy_alter (k UInt64, orig Float64, x Float64, y Float64 ALIAS x * 2)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';

SELECT 'alter, before';
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_lossy_alter' ORDER BY name;

ALTER TABLE t_lossy_alter MODIFY COLUMN x CODEC(SZ3('ALGO_LORENZO_REG', 'ABS', 0.01));

SELECT 'alter, after';
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_lossy_alter' ORDER BY name;

INSERT INTO t_lossy_alter SELECT number, number * 0.5 + sin(number * 0.1), number * 0.5 + sin(number * 0.1) FROM numbers(100000);

WITH (SELECT tuple(k, x) FROM t_lossy_alter WHERE x != orig ORDER BY k LIMIT 1 SETTINGS use_skip_indexes = 0) AS w
SELECT count() FROM t_lossy_alter WHERE k = w.1 AND x = w.2;

CREATE TABLE t_lossy_replicated (k UInt64, orig Float64, x Float64, y Float64 ALIAS x * 2)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lossy_replicated', 'r1') ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';

ALTER TABLE t_lossy_replicated MODIFY COLUMN x CODEC(SZ3('ALGO_LORENZO_REG', 'ABS', 0.01)) SETTINGS alter_sync = 2;

SELECT 'replicated';
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_lossy_replicated' ORDER BY name;

INSERT INTO t_lossy_replicated SELECT number, number * 0.5 + sin(number * 0.1), number * 0.5 + sin(number * 0.1) FROM numbers(100000);

WITH (SELECT tuple(k, x) FROM t_lossy_replicated WHERE x != orig ORDER BY k LIMIT 1 SETTINGS use_skip_indexes = 0) AS w
SELECT count() FROM t_lossy_replicated WHERE k = w.1 AND x = w.2;

DROP TABLE t_lossy;
DROP TABLE t_lossy_alter;
DROP TABLE t_lossy_replicated;
