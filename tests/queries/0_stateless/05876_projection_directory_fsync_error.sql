-- Tags: no-parallel, no-fasttest, no-object-storage
-- no-parallel: the failpoint is server-wide and fails the next directory sync of any query.
-- no-fasttest: failpoints are not available in the fast test build.
-- no-object-storage: object storage does not fsync directories.

-- A failure to fsync the directory of a merged projection must fail the merge instead of being
-- logged and ignored, otherwise the merge completes without the projection being durable.
-- The projection is merged in place inside the new part, so its directory sync is the first
-- directory sync of the merge, and that is the one the `ONCE` failpoint fails.
-- `min_bytes_for_full_part_storage = 0` keeps the parts in `Full` storage (the setting is randomized).

DROP TABLE IF EXISTS t_proj_dir_fsync;

CREATE TABLE t_proj_dir_fsync (id UInt64, key String, v UInt64,
                               PROJECTION pr (SELECT key, sum(v) GROUP BY key))
ENGINE = MergeTree ORDER BY id
SETTINGS fsync_part_directory = 1, min_rows_to_fsync_after_merge = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_proj_dir_fsync SELECT number, toString(number % 10), number FROM numbers(1000);
INSERT INTO t_proj_dir_fsync SELECT number, toString(number % 10), number FROM numbers(1000, 1000);

SYSTEM ENABLE FAILPOINT local_directory_sync_fail;
OPTIMIZE TABLE t_proj_dir_fsync FINAL; -- { serverError CANNOT_FSYNC }
SYSTEM DISABLE FAILPOINT local_directory_sync_fail;

SELECT 'active parts after failed merge', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_proj_dir_fsync' AND active;

-- The failed merge left the source parts intact, and the merge succeeds when retried.
OPTIMIZE TABLE t_proj_dir_fsync FINAL;

SELECT 'active parts after retried merge', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_proj_dir_fsync' AND active;

SELECT 'active projection parts', count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_dir_fsync' AND active;

SELECT key, sum(v) FROM t_proj_dir_fsync GROUP BY key ORDER BY key
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_dir_fsync;
