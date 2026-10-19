-- `max_partitions_to_read` is checked before the read, so it must count only the parts left after skip indexes,
-- even with `use_skip_indexes_on_data_read = 1` and a cold query condition cache.

SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS t_skip_idx_partitions;

CREATE TABLE t_skip_idx_partitions
(
    d Date,
    k UInt64,
    s String,
    INDEX idx_s s TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree PARTITION BY d ORDER BY k
SETTINGS index_granularity = 128, max_partitions_to_read = 2;

-- 5 partitions; 'needle' exists only in the last one, every 'hay-N' in all of them.
INSERT INTO t_skip_idx_partitions SELECT toDate('2026-01-01') + number % 5, number, if(number % 5 = 4 AND number % 100 = 4, 'needle', 'hay-' || toString(number % 7)) FROM numbers(10000);

SELECT count() FROM t_skip_idx_partitions WHERE s = 'needle';
SELECT count() FROM t_skip_idx_partitions WHERE s = 'needle' SETTINGS max_partitions_to_read = 0;
SELECT count() FROM t_skip_idx_partitions WHERE s = 'hay-1'; -- { serverError TOO_MANY_PARTITIONS }

-- The query-level setting takes precedence over the table one.
ALTER TABLE t_skip_idx_partitions MODIFY SETTING max_partitions_to_read = 0;
SELECT count() FROM t_skip_idx_partitions WHERE s = 'needle' SETTINGS max_partitions_to_read = 1;
SELECT count() FROM t_skip_idx_partitions WHERE s = 'hay-1' SETTINGS max_partitions_to_read = 1; -- { serverError TOO_MANY_PARTITIONS }

DROP TABLE t_skip_idx_partitions;
