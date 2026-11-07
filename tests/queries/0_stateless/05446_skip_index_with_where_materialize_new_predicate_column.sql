-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- MATERIALIZE INDEX on parts written before the column used only by the index's WHERE was added
-- must keep that column in the part, otherwise the freshly built partial index is refused.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;

SELECT 'Wide part';
CREATE TABLE t_wide (id UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_wide VALUES (1), (2), (3);
ALTER TABLE t_wide ADD COLUMN p UInt8 DEFAULT 1;
ALTER TABLE t_wide ADD INDEX i id TYPE minmax GRANULARITY 1 WHERE p = 1;
ALTER TABLE t_wide MATERIALIZE INDEX i SETTINGS mutations_sync = 2;
SELECT count() FROM t_wide WHERE p = 1 AND id = 1
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i', max_rows_to_read = 1;

SELECT 'Compact part';
CREATE TABLE t_compact (id UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000, index_granularity = 1;
INSERT INTO t_compact VALUES (1), (2), (3);
ALTER TABLE t_compact ADD COLUMN p UInt8 DEFAULT 1;
ALTER TABLE t_compact ADD INDEX i id TYPE minmax GRANULARITY 1 WHERE p = 1;
ALTER TABLE t_compact MATERIALIZE INDEX i SETTINGS mutations_sync = 2;
SELECT count() FROM t_compact WHERE p = 1 AND id = 1
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i', max_rows_to_read = 1;

DROP TABLE t_wide;
DROP TABLE t_compact;
