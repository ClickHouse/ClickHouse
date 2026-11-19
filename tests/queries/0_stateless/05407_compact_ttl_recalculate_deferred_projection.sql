SET mutations_sync = 2;
SET max_execution_time = 120;
SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS compact_ttl_deferred_projection SYNC;

CREATE TABLE compact_ttl_deferred_projection
(
    k UInt64,
    ts DateTime('UTC'),
    payload String,
    PROJECTION p
    (
        SELECT *, _block_number, _block_offset
        ORDER BY _block_number, _block_offset
    )
)
ENGINE = MergeTree ORDER BY k
TTL ts + INTERVAL 1 YEAR
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
         min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
         enable_block_number_column = 1, enable_block_offset_column = 1,
         allow_commit_order_projection = 1, materialize_projections_on_insert = 1,
         materialize_ttl_recalculate_only = 1, max_number_of_merges_with_ttl_in_pool = 0;

INSERT INTO compact_ttl_deferred_projection VALUES
    (3, '2000-01-01', 'c'), (1, '2000-01-01', 'a'), (2, '2000-01-01', 'b');

-- The commit-order projection is deferred during insertion.
SELECT part_type, part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table = 'compact_ttl_deferred_projection' AND active;
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'compact_ttl_deferred_projection' AND active;

-- Recalculation must still execute the projection rebuild scheduled by the interpreter.
ALTER TABLE compact_ttl_deferred_projection MATERIALIZE TTL;
SELECT count(), sum(k) FROM compact_ttl_deferred_projection;
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'compact_ttl_deferred_projection' AND name = 'p' AND active;
SELECT k, payload FROM mergeTreeProjection(currentDatabase(), 'compact_ttl_deferred_projection', 'p') ORDER BY k;
SELECT toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC')
FROM system.parts
WHERE database = currentDatabase() AND table = 'compact_ttl_deferred_projection' AND active;
CHECK TABLE compact_ttl_deferred_projection;

-- Once built, the projection can be carried over by the writer-free path.
ALTER TABLE compact_ttl_deferred_projection MATERIALIZE TTL;
DETACH TABLE compact_ttl_deferred_projection SYNC;
ATTACH TABLE compact_ttl_deferred_projection;
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'compact_ttl_deferred_projection' AND name = 'p' AND active;
SELECT k, payload FROM mergeTreeProjection(currentDatabase(), 'compact_ttl_deferred_projection', 'p') ORDER BY k;
CHECK TABLE compact_ttl_deferred_projection;

DROP TABLE compact_ttl_deferred_projection SYNC;
