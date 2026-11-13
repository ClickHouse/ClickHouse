SET use_uncompressed_cache = 0, use_columns_cache = 0;

SELECT value, default FROM system.settings WHERE name = 'use_lz4_library_decompression';

DROP TABLE IF EXISTS lz4_library_wide;
DROP TABLE IF EXISTS lz4_library_compact;

CREATE TABLE lz4_library_wide
(
    id UInt64 CODEC(LZ4),
    delta UInt64 CODEC(Delta, LZ4),
    text String CODEC(LZ4HC),
    category LowCardinality(String) CODEC(LZ4),
    optional Nullable(UInt64) CODEC(LZ4)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE lz4_library_compact AS lz4_library_wide
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000;

INSERT INTO lz4_library_wide
SELECT number, number * 3, concat(repeat('abcdef', 20), toString(number)),
       toString(number % 5), if(number % 7 = 0, NULL, number)
FROM numbers(100000)
SETTINGS use_lz4_library_decompression = 0;

INSERT INTO lz4_library_compact SELECT * FROM lz4_library_wide
SETTINGS use_lz4_library_decompression = 1;

-- Hash the values, including strings, to exercise decompression rather than metadata-only reads.
CREATE TEMPORARY TABLE lz4_library_expected ENGINE = Memory AS
SELECT groupBitXor(cityHash64(number, number * 3, concat(repeat('abcdef', 20), toString(number)),
                              toString(number % 5), if(number % 7 = 0, 0, number))) AS hash
FROM numbers(100000);

SELECT groupBitXor(cityHash64(id, delta, text, category, ifNull(optional, 0))) =
       (SELECT hash FROM lz4_library_expected)
FROM lz4_library_wide
SETTINGS use_lz4_library_decompression = 0, max_threads = 4;

SELECT groupBitXor(cityHash64(id, delta, text, category, ifNull(optional, 0))) =
       (SELECT hash FROM lz4_library_expected)
FROM lz4_library_wide
SETTINGS use_lz4_library_decompression = 1, max_threads = 4;

SELECT groupBitXor(cityHash64(id, delta, text, category, ifNull(optional, 0))) =
       (SELECT hash FROM lz4_library_expected)
FROM lz4_library_compact
SETTINGS use_lz4_library_decompression = 1, max_threads = 4;

SELECT groupBitXor(cityHash64(id, delta, text, category, ifNull(optional, 0))) =
       (SELECT hash FROM lz4_library_expected)
FROM lz4_library_compact
SETTINGS use_lz4_library_decompression = 0, max_threads = 4;

SET compatibility = '26.10';
SELECT value FROM system.settings WHERE name = 'use_lz4_library_decompression';
SET use_lz4_library_decompression = 1;
SELECT value FROM system.settings WHERE name = 'use_lz4_library_decompression';

DROP TABLE lz4_library_wide;
DROP TABLE lz4_library_compact;
