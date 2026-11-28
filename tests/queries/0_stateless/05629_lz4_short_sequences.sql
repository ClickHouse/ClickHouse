SET use_lz4_library_decompression = 0, use_uncompressed_cache = 0, use_columns_cache = 0;

DROP TABLE IF EXISTS lz4_short_sequences;

CREATE TABLE lz4_short_sequences
(
    fast String CODEC(LZ4),
    hc String CODEC(LZ4HC(3))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_compress_block_size = 16384, max_compress_block_size = 16384;

INSERT INTO lz4_short_sequences
SELECT value, value
FROM
(
    SELECT arrayStringConcat(arrayMap(i -> char(65 + cityHash64(number, toUInt64(i)) % 8), range(64))) AS value
    FROM numbers(100000)
) AS source;

SELECT sum(cityHash64(fast)) = expected AND sum(cityHash64(hc)) = expected
FROM lz4_short_sequences
CROSS JOIN
(
    SELECT sum(cityHash64(arrayStringConcat(arrayMap(i -> char(65 + cityHash64(number, toUInt64(i)) % 8), range(64))))) AS expected
    FROM numbers(100000)
) AS oracle
GROUP BY expected;

DROP TABLE lz4_short_sequences;
