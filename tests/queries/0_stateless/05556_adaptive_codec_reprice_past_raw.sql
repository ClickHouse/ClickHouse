-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the blocks must hold 192 UInt64 values each, three `T64` matrices.

DROP TABLE IF EXISTS t_reprice_past_raw;

CREATE TABLE t_reprice_past_raw (x UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 192, index_granularity_bytes = 0, min_compress_block_size = 1,
         enable_adaptive_codec_selection = 1, default_compression_codec = 'ZSTD(1)', adaptive_codec_selection_period = 8;

-- Block 0 is priced. Only `T64` beats the raw block there, so the raw block is the runner-up.
-- Block 1 reuses `T64`, which grows past the raw block while the default codec shrinks it. The winner's own size must trigger the re-pricing.
INSERT INTO t_reprice_past_raw
SELECT multiIf(number = 0, 0, number = 1, 0x7FFFFFFFFFFFFFFF, number < 192, bitAnd(cityHash64(number), 0x7FFFFFFFFFFFFFFF), number = 192, 0xFFFFFFFFFFFFFFFF, 1)
FROM numbers(384);

OPTIMIZE TABLE t_reprice_past_raw FINAL;

SELECT codec_block_counts FROM mergeTreeCodecBlockCounts(currentDatabase(), t_reprice_past_raw);
SELECT count(), sum(x) FROM t_reprice_past_raw;

SYSTEM FLUSH LOGS part_log;
SELECT ProfileEvents['AdaptiveCodecBlocksPriced'], ProfileEvents['AdaptiveCodecBlocksRepriced'], ProfileEvents['AdaptiveCodecBlocksReused']
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND database = currentDatabase() AND table = 't_reprice_past_raw' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_reprice_past_raw;
