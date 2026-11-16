-- `string_size_stream_codec = 'ADAPTIVE'` compresses each block of the `.size` substream of `String` columns with the smallest
-- of the column codec, `T64` and `T64` followed by the column codec. A column with `CODEC(NONE)` keeps `NONE`.

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
    string_size_stream_codec = 'ADAPTIVE', ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_size_codec;

INSERT INTO t_size_codec SELECT repeat('x', intHash32(number) % 100) AS s, s, s, tuple(s) FROM numbers(100000);

ALTER TABLE t_size_codec MODIFY SETTING string_size_stream_codec = 'NONE';

INSERT INTO t_size_codec SELECT repeat('x', intHash32(number) % 100) AS s, s, s, tuple(s) FROM numbers(100000);

-- Whether some block of the substream starts with `T64`: only for `.size` of compressed columns in the first part.
SELECT part_name, substream, arrayExists(k -> startsWith(k, 'T64'), mapKeys(codec_block_counts))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_size_codec)
ORDER BY part_name, substream;

-- The second part uses the column codecs only.
SELECT substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_size_codec)
WHERE part_name = 'all_2_2_0' AND substream LIKE '%.size'
ORDER BY substream;

SELECT sum(lz4.size), sum(length(zstd)), sum(none.size), sum(tup.a.size), sum(length(tup.a)) FROM t_size_codec;

ALTER TABLE t_size_codec MODIFY SETTING string_size_stream_codec = 'ADAPTIVE';
SYSTEM START MERGES t_size_codec;
OPTIMIZE TABLE t_size_codec FINAL;

-- After merge: one part with the current setting.
SELECT substream, arrayExists(k -> startsWith(k, 'T64'), mapKeys(codec_block_counts))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_size_codec)
WHERE substream LIKE '%.size'
ORDER BY substream;

SELECT sum(lz4.size), sum(length(zstd)), sum(none.size), sum(tup.a.size), sum(length(tup.a)) FROM t_size_codec;

DROP TABLE t_size_codec;

-- Compact parts share one stream per codec.
CREATE TABLE t_size_codec (s String CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000, serialization_info_version = 'with_types',
    string_serialization_version = 'with_size_stream', string_size_stream_codec = 'ADAPTIVE';

INSERT INTO t_size_codec SELECT repeat('x', intHash32(number) % 100) FROM numbers(100000);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_size_codec' AND active;
SELECT sum(s.size), sum(length(s)), countIf(s = repeat('x', 42)) FROM t_size_codec;

DROP TABLE t_size_codec;
