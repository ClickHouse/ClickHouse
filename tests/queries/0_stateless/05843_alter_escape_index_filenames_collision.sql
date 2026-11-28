-- Changing `escape_index_filenames` changes the file name of every skip index whose name needs
-- escaping. The new file name of one index must not be the name under which the files of another
-- index are still stored in the parts, otherwise those files would be read as granules of the wrong
-- index and queries would return wrong results.

DROP TABLE IF EXISTS t_policy_collision;

-- With escaping on, `a/b` is stored as `skp_idx_a%2Fb` and `a%2Fb` as `skp_idx_a%252Fb`.
-- With escaping off, `a%2Fb` resolves to `skp_idx_a%2Fb`: the files of `a/b`.
CREATE TABLE t_policy_collision
(
    k UInt64,
    v UInt64,
    w UInt64,
    INDEX `a/b` v TYPE minmax GRANULARITY 1,
    INDEX `a%2Fb` w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 1, index_granularity = 8,
         min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_policy_collision SELECT number, 0, 1 FROM numbers(100)
SETTINGS materialize_skip_indexes_on_insert = 1;

-- Drop `a/b`, but keep its mutation pending, so its files are still in the part.
SYSTEM STOP MERGES t_policy_collision;
ALTER TABLE t_policy_collision DROP INDEX `a/b` SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT 'pending drop';
ALTER TABLE t_policy_collision MODIFY SETTING escape_index_filenames = 0; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM t_policy_collision WHERE w = 1 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_policy_collision WHERE w = 1 SETTINGS use_skip_indexes = 0;

-- Once the drop mutation has removed the files, the name is free and the change is allowed.
SYSTEM START MERGES t_policy_collision;
ALTER TABLE t_policy_collision UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_policy_collision' AND NOT is_done;

SELECT 'finished drop';
ALTER TABLE t_policy_collision MODIFY SETTING escape_index_filenames = 0;

SELECT count() FROM t_policy_collision WHERE w = 1 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_policy_collision WHERE w = 1 SETTINGS use_skip_indexes = 0;

DROP TABLE t_policy_collision;

-- The same in the other direction, between two indices the table has: with escaping off, `a b` is
-- stored as `skp_idx_a b` and `a%20b` as `skp_idx_a%20b`. With escaping on, `a b` resolves to
-- `skp_idx_a%20b`: the files of `a%20b`.
DROP TABLE IF EXISTS t_policy_collision_enable;

CREATE TABLE t_policy_collision_enable
(
    k UInt64,
    v UInt64,
    w UInt64,
    INDEX `a%20b` v TYPE minmax GRANULARITY 1,
    INDEX `a b` w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0, index_granularity = 8,
         min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_policy_collision_enable SELECT number, 0, 1 FROM numbers(100)
SETTINGS materialize_skip_indexes_on_insert = 1;

SELECT 'existing indices';
ALTER TABLE t_policy_collision_enable MODIFY SETTING escape_index_filenames = 1; -- { serverError BAD_ARGUMENTS }

-- Dropping the other index in the same batch does not remove its files yet.
ALTER TABLE t_policy_collision_enable DROP INDEX `a%20b`, MODIFY SETTING escape_index_filenames = 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM t_policy_collision_enable WHERE w = 1 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_policy_collision_enable WHERE w = 1 SETTINGS use_skip_indexes = 0;

ALTER TABLE t_policy_collision_enable DROP INDEX `a%20b` SETTINGS mutations_sync = 2;

SELECT 'dropped index';
ALTER TABLE t_policy_collision_enable MODIFY SETTING escape_index_filenames = 1;

SELECT count() FROM t_policy_collision_enable WHERE w = 1 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_policy_collision_enable WHERE w = 1 SETTINGS use_skip_indexes = 0;

DROP TABLE t_policy_collision_enable;
