-- Tags: no-replicated-database
-- no-replicated-database: the batch mixes replicated `DROP INDEX` with non-replicated
-- `MODIFY SETTING`, which a `Replicated` database prohibits (`QUERY_IS_PROHIBITED`).

-- A single `ALTER` drops an index named with '/' and disables `escape_index_filenames`. The index
-- is gone from the new metadata, so the change is allowed, and the `DROP INDEX` mutation, which
-- runs against the metadata with escaping disabled, must still resolve the index files.

DROP TABLE IF EXISTS t_drop_slash_index_same_batch;

CREATE TABLE t_drop_slash_index_same_batch
(
    k UInt64,
    v UInt64,
    INDEX `idx/v` v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 1, index_granularity = 8, min_bytes_for_wide_part = 0;

INSERT INTO t_drop_slash_index_same_batch SELECT number, number FROM numbers(100);

ALTER TABLE t_drop_slash_index_same_batch DROP INDEX `idx/v`, MODIFY SETTING escape_index_filenames = 0 SETTINGS mutations_sync = 2;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_drop_slash_index_same_batch' AND NOT is_done;

SELECT secondary_indices_materialized FROM system.parts
WHERE database = currentDatabase() AND table = 't_drop_slash_index_same_batch' AND active;

SELECT count() FROM t_drop_slash_index_same_batch WHERE v = 42;

DROP TABLE t_drop_slash_index_same_batch;
