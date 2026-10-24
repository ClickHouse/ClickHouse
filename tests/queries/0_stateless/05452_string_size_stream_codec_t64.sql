-- `string_size_stream_codec = 'T64'` puts `T64` before the column's general-purpose codecs on the `.size` substream of `String` columns.

DROP TABLE IF EXISTS t_size_codec;

CREATE TABLE t_size_codec
(
    lz4 String CODEC(LZ4),
    zstd String CODEC(ZSTD(3)),
    none String CODEC(NONE),
    tup Tuple(a String) CODEC(LZ4)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
    string_size_stream_codec = 'T64', ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_size_codec;

INSERT INTO t_size_codec SELECT repeat('x', number % 100) AS s, s, s, tuple(s) FROM numbers(100000);

ALTER TABLE t_size_codec MODIFY SETTING string_size_stream_codec = 'NONE';

INSERT INTO t_size_codec SELECT repeat('x', number % 100) AS s, s, s, tuple(s) FROM numbers(100000);

-- First part: `T64` on `.size` unless the column is not compressed. Second part: column codecs only.
SELECT part_name, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_size_codec)
ORDER BY part_name, substream;

SELECT sum(lz4.size), sum(length(zstd)), sum(none.size), sum(tup.a.size), sum(length(tup.a)) FROM t_size_codec;

ALTER TABLE t_size_codec MODIFY SETTING string_size_stream_codec = 'T64';
SYSTEM START MERGES t_size_codec;
OPTIMIZE TABLE t_size_codec FINAL;

-- After merge: one part with the current setting.
SELECT substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_size_codec)
WHERE substream LIKE '%.size'
ORDER BY substream;

SELECT sum(lz4.size), sum(length(zstd)), sum(none.size), sum(tup.a.size), sum(length(tup.a)) FROM t_size_codec;

DROP TABLE t_size_codec;

-- Compact parts share one stream per codec.
CREATE TABLE t_size_codec (s String CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000, serialization_info_version = 'with_types',
    string_serialization_version = 'with_size_stream', string_size_stream_codec = 'T64';

INSERT INTO t_size_codec SELECT repeat('x', number % 100) FROM numbers(100000);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_size_codec' AND active;
SELECT sum(s.size), sum(length(s)), countIf(s = repeat('x', 42)) FROM t_size_codec;

DROP TABLE t_size_codec;
