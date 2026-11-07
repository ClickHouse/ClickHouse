-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- Materializing a sibling index must not write a column that only the WHERE of a kept partial index reads,
-- or the kept index, built when the row did not match, becomes usable and drops the row.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    id UInt64,
    d DateTime,
    p UInt8 TTL d + INTERVAL 1 DAY,
    INDEX i id TYPE minmax GRANULARITY 1 WHERE p = 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;

INSERT INTO t VALUES (1, '2000-01-01 00:00:00', 0);

ALTER TABLE t MODIFY COLUMN p UInt8 DEFAULT 1 TTL d + INTERVAL 1 DAY;
ALTER TABLE t ADD INDEX j p TYPE minmax GRANULARITY 1;
ALTER TABLE t MATERIALIZE INDEX j SETTINGS mutations_sync = 2;

SELECT count() FROM t WHERE p = 1 AND id = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t WHERE p = 1 AND id = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t WHERE p = 1 AND id = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

DROP TABLE t;
