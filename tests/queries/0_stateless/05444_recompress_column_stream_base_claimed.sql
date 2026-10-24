-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the test pins the settings that decide whether the column and the
-- skip index share a stream base (part type, packing of the skip index substreams).

-- `RECOMPRESS COLUMN` rewrites the streams of the column in place under the same file names, so
-- they must still claim their stream bases: an index materialized by the same mutation must not
-- write its files over the marks of the column. The mutation is rejected, as it is without the
-- recompression, and the column stays readable.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_recompress_stream_base;

CREATE TABLE t_recompress_stream_base (k UInt64, `skp_idx_a` UInt64 CODEC(NONE), s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
         packed_skip_index_max_bytes = 0, index_granularity = 100;
INSERT INTO t_recompress_stream_base SELECT number, number, toString(number) FROM numbers(20000);

ALTER TABLE t_recompress_stream_base ADD INDEX a(s) TYPE set(100) GRANULARITY 1;
ALTER TABLE t_recompress_stream_base MODIFY COLUMN `skp_idx_a` UInt64 CODEC(ZSTD);
ALTER TABLE t_recompress_stream_base RECOMPRESS COLUMN `skp_idx_a`, MATERIALIZE INDEX a; -- { serverError UNFINISHED }

SELECT count() > 0, countIf(latest_fail_reason LIKE '%INCORRECT_FILE_NAME%') = count()
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_recompress_stream_base' AND NOT is_done;

KILL MUTATION WHERE database = currentDatabase() AND table = 't_recompress_stream_base' SYNC FORMAT Null;

SELECT sum(`skp_idx_a`) FROM t_recompress_stream_base SETTINGS use_skip_indexes = 0;

DROP TABLE t_recompress_stream_base;
