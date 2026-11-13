-- A `DROP INDEX` mutation removes the files of an index that is no longer in the metadata, so the
-- current `escape_index_filenames` policy may differ from the one the index was written with. An
-- index named with '/' can only be written with escaping on, so its drop mutation must still find
-- (and remove) its files after escaping was disabled while the mutation was pending.

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

SELECT secondary_indices_materialized FROM system.parts
WHERE database = currentDatabase() AND table = 't_drop_slash_index' AND active;

-- Disabling escaping is rejected while the index exists.
ALTER TABLE t_drop_slash_index MODIFY SETTING escape_index_filenames = 0; -- { serverError BAD_ARGUMENTS }

-- Drop the index, but keep its mutation pending while escaping is disabled.
SYSTEM STOP MERGES t_drop_slash_index;
ALTER TABLE t_drop_slash_index DROP INDEX `idx/v` SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_drop_slash_index MODIFY SETTING escape_index_filenames = 0;

-- The drop mutation finishes under the new policy instead of failing on the index file name.
SYSTEM START MERGES t_drop_slash_index;
ALTER TABLE t_drop_slash_index UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_drop_slash_index' AND NOT is_done;

SELECT secondary_indices_materialized FROM system.parts
WHERE database = currentDatabase() AND table = 't_drop_slash_index' AND active;

SELECT count() FROM t_drop_slash_index WHERE v = 42;

DROP TABLE t_drop_slash_index;
