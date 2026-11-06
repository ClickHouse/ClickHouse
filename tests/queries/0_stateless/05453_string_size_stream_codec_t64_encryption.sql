-- Tags: no-fasttest
-- no-fasttest: AES_128_GCM_SIV depends on OpenSSL.

-- With `string_size_stream_codec = 'T64'`, the `.size` substream of an encrypted `String` column stays encrypted.
-- `T64` is added in front of a compressing chain, and not added when the column is only encrypted.

DROP TABLE IF EXISTS t_size_codec_enc;

CREATE TABLE t_size_codec_enc
(
    enc String CODEC(AES_128_GCM_SIV),
    lz4_enc String CODEC(LZ4, AES_128_GCM_SIV),
    zstd_enc String CODEC(ZSTD(3), AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
    string_size_stream_codec = 'T64', ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_size_codec_enc SELECT repeat('x', number % 100) AS s, s, s FROM numbers(100000);

SELECT substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_size_codec_enc)
WHERE column IN ('enc', 'lz4_enc', 'zstd_enc') -- skip the randomized `_block_number` / `_block_offset` columns
ORDER BY substream;

SELECT sum(enc.size), sum(lz4_enc.size), sum(length(zstd_enc)), countIf(zstd_enc = repeat('x', 42)) FROM t_size_codec_enc;
CHECK TABLE t_size_codec_enc SETTINGS check_query_single_value_result = 1;

DROP TABLE t_size_codec_enc;
