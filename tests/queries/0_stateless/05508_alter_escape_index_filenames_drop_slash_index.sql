-- A `DROP INDEX` mutation names the files to remove with the `escape_index_filenames` policy of the
-- metadata it runs against. For an index named with '/', that name can only be produced with
-- escaping on, so disabling escaping must be rejected while such an index is still being dropped:
-- both in the same ALTER batch and while an earlier drop mutation is still pending.

DROP TABLE IF EXISTS t_drop_slash_index;

CREATE TABLE t_drop_slash_index
(
    k UInt64,
    v UInt64,
    INDEX `idx/v` v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 1, index_granularity = 8, min_bytes_for_wide_part = 0;

INSERT INTO t_drop_slash_index SELECT number, number FROM numbers(100);

-- The same batch drops the index and disables escaping.
ALTER TABLE t_drop_slash_index DROP INDEX `idx/v`, MODIFY SETTING escape_index_filenames = 0 SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }

-- The rejected ALTER left the table untouched.
SELECT secondary_indices_materialized FROM system.parts
WHERE database = currentDatabase() AND table = 't_drop_slash_index' AND active;

-- The drop mutation is still pending.
SYSTEM STOP MERGES t_drop_slash_index;
ALTER TABLE t_drop_slash_index DROP INDEX `idx/v` SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_drop_slash_index MODIFY SETTING escape_index_filenames = 0; -- { serverError BAD_ARGUMENTS }

-- Once the mutation has finished, escaping can be disabled.
SYSTEM START MERGES t_drop_slash_index;
ALTER TABLE t_drop_slash_index UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_drop_slash_index' AND NOT is_done;

ALTER TABLE t_drop_slash_index MODIFY SETTING escape_index_filenames = 0;

SELECT secondary_indices_materialized FROM system.parts
WHERE database = currentDatabase() AND table = 't_drop_slash_index' AND active;

SELECT count() FROM t_drop_slash_index WHERE v = 42;

DROP TABLE t_drop_slash_index;
