-- Tags: no-random-settings, no-random-merge-tree-settings

-- A `Nullable` primary key column without NULLs may be read or written without a null map. The primary index
-- is shared with the readers and weighed by `PrimaryIndexCache`, so its null maps must be created when the index
-- is set or loaded, and not lazily by a query reading the index. Each query reading the null map of the index
-- is run twice: if the first one created the null map of the shared index column, it counts one more
-- `NullableNullMapMaterializations` than the second one.

SET max_threads = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_nullable_pk;
DROP TABLE IF EXISTS t_nullable_pk_cache;

CREATE TABLE t_nullable_pk (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_nullable_pk SELECT number % 2 FROM numbers(100);

-- The index is set by the writer.
SELECT sum(isZeroOrNull(k)) FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk') SETTINGS log_comment = '05473_set';
SELECT sum(isZeroOrNull(k)) FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk') SETTINGS log_comment = '05473_set';

-- The index is loaded.
DETACH TABLE t_nullable_pk;
ATTACH TABLE t_nullable_pk;
SELECT count() FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk');
SELECT sum(isZeroOrNull(k)) FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk') SETTINGS log_comment = '05473_load';
SELECT sum(isZeroOrNull(k)) FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk') SETTINGS log_comment = '05473_load';

-- The index is prewarmed into the primary index cache.
CREATE TABLE t_nullable_pk_cache (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 1, use_primary_key_cache = 1, prewarm_primary_key_cache = 1;
INSERT INTO t_nullable_pk_cache SELECT number % 2 FROM numbers(100);
SELECT sum(isZeroOrNull(k)) FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk_cache') SETTINGS log_comment = '05473_cache';
SELECT sum(isZeroOrNull(k)) FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk_cache') SETTINGS log_comment = '05473_cache';

-- The index is loaded into the primary index cache.
DETACH TABLE t_nullable_pk_cache;
ATTACH TABLE t_nullable_pk_cache;
SELECT count() FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk_cache');
SELECT sum(isZeroOrNull(k)) FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk_cache') SETTINGS log_comment = '05473_cache_load';
SELECT sum(isZeroOrNull(k)) FROM mergeTreeIndex(currentDatabase(), 't_nullable_pk_cache') SETTINGS log_comment = '05473_cache_load';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, count(), min(ProfileEvents['NullableNullMapMaterializations']) = max(ProfileEvents['NullableNullMapMaterializations'])
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05473_%' AND event_date >= yesterday()
GROUP BY log_comment
ORDER BY min(event_time_microseconds);

DROP TABLE t_nullable_pk;
DROP TABLE t_nullable_pk_cache;
