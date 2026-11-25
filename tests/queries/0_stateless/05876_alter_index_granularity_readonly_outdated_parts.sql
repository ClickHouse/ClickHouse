-- A read-only table does not load its outdated parts. One of them may have non-adaptive granularity, which reads its
-- granule size from index_granularity, so the setting cannot change until they are loaded.

DROP TABLE IF EXISTS t_granularity_outdated_fixed;
DROP TABLE IF EXISTS t_granularity_outdated;

CREATE TABLE t_granularity_outdated_fixed (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = 0;

CREATE TABLE t_granularity_outdated (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, old_parts_lifetime = 1000000;

INSERT INTO t_granularity_outdated_fixed SELECT number FROM numbers(20000);
ALTER TABLE t_granularity_outdated ATTACH PARTITION ID 'all' FROM t_granularity_outdated_fixed;
INSERT INTO t_granularity_outdated SELECT number FROM numbers(20000, 10);

-- The merge leaves the non-adaptive part outdated.
OPTIMIZE TABLE t_granularity_outdated FINAL;
SELECT 'outdated_parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_granularity_outdated' AND NOT active;

-- After a reload the read-only table keeps its outdated parts unloaded.
ALTER TABLE t_granularity_outdated MODIFY SETTING table_readonly = 1;
DETACH TABLE t_granularity_outdated;
ATTACH TABLE t_granularity_outdated;
ALTER TABLE t_granularity_outdated MODIFY SETTING index_granularity = 1024; -- { serverError READONLY_SETTING }
ALTER TABLE t_granularity_outdated MODIFY SETTING table_readonly = 0, index_granularity = 1024; -- { serverError READONLY_SETTING }

-- Once the table is writable, its outdated parts are loaded, and the non-adaptive one is found.
ALTER TABLE t_granularity_outdated MODIFY SETTING table_readonly = 0;
ALTER TABLE t_granularity_outdated MODIFY SETTING index_granularity = 1024; -- { serverError READONLY_SETTING }

SELECT 'total', count(), sum(a) FROM t_granularity_outdated;

DROP TABLE t_granularity_outdated;
DROP TABLE t_granularity_outdated_fixed;
