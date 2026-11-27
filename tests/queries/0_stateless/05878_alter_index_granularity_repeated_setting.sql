-- A definition may state a setting more than once, and the last occurrence is in effect. An ALTER is
-- compared with that one, so a value equal only to an earlier occurrence is still a change and is checked.

DROP TABLE IF EXISTS t_granularity_repeated;

CREATE TABLE t_granularity_repeated (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, index_granularity_bytes = 10485760;

-- The table is adaptive, so switching it to the non-adaptive mark format is rejected.
ALTER TABLE t_granularity_repeated MODIFY SETTING index_granularity_bytes = 0; -- { serverError READONLY_SETTING }
SELECT 'bytes_after_rejected', extractAll(create_table_query, 'index_granularity_bytes = \\d+')
FROM system.tables WHERE database = currentDatabase() AND name = 't_granularity_repeated';

-- The effective value itself is a no-op and is allowed.
ALTER TABLE t_granularity_repeated MODIFY SETTING index_granularity_bytes = 10485760;

INSERT INTO t_granularity_repeated SELECT number FROM numbers(1000);
SELECT 'total', count(), sum(a) FROM t_granularity_repeated;

DROP TABLE t_granularity_repeated;
