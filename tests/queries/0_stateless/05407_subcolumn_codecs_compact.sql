SET enable_subcolumn_codecs = 1;

DROP TABLE IF EXISTS t_subcolumn_codecs_compact;

-- A Compact part writes all streams into one file, each stream with its own codec. `mergeTreeCodecBlockCounts` reads
-- the file of a stream, which a Compact part doesn't have, so the codecs are checked by writing and reading the data.
CREATE TABLE t_subcolumn_codecs_compact
(
    id UInt64,
    payload Tuple(ts DateTime64(3), value Float64, note Nullable(String)),
    attrs Map(UInt64, Float64),
    CODEC payload.ts (DoubleDelta, ZSTD(1)),
    CODEC payload.note (ZSTD(3)),
    CODEC attrs.values (Gorilla, LZ4HC(4))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, default_compression_codec = 'LZ4',
    ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_subcolumn_codecs_compact SELECT number, (toDateTime64('2026-01-01 00:00:00', 3) + number, number / 7, if(number % 2 = 0, NULL, toString(number))), map(number, number / 3) FROM numbers(1000);
INSERT INTO t_subcolumn_codecs_compact SELECT number, (toDateTime64('2026-01-01 00:00:00', 3) + number, number / 7, if(number % 2 = 0, NULL, toString(number))), map(number, number / 3) FROM numbers(1000, 1000);

SELECT 'compact parts';
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_subcolumn_codecs_compact' AND active;
SELECT count(), round(sum(payload.value)), countIf(payload.note IS NULL), max(payload.ts), round(sum(attrs[id])) FROM t_subcolumn_codecs_compact;

-- A merge writes the streams with the same codecs.
OPTIMIZE TABLE t_subcolumn_codecs_compact FINAL;
SELECT 'after merge';
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_subcolumn_codecs_compact' AND active;
SELECT count(), round(sum(payload.value)), countIf(payload.note IS NULL), max(payload.ts), round(sum(attrs[id])) FROM t_subcolumn_codecs_compact;

DROP TABLE t_subcolumn_codecs_compact;
