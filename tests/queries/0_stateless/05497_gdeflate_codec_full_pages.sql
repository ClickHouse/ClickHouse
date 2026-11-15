-- Pages of 64 KiB that are nearly incompressible compress to more than the bound `libdeflate_gdeflate_compress_bound`
-- used to return, so the `INSERT` failed with `CANNOT_COMPRESS`, or the page was written past its buffer.
-- Each chunk of 65536 rows of `x` is one such page: a short compressible prefix, then random bytes.

SET enable_gdeflate_codec = 1;

DROP TABLE IF EXISTS t_gdeflate_full_pages;

CREATE TABLE t_gdeflate_full_pages
(
    k UInt64,
    x1 UInt8 CODEC(GDeflate(1)),
    x6 UInt8 CODEC(GDeflate(6)),
    x12 UInt8 CODEC(GDeflate(12))
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 65536;

INSERT INTO t_gdeflate_full_pages
SELECT number, x, x, x
FROM
(
    SELECT number, toUInt8(if(number % 65536 < 100 + intDiv(number, 65536), number % 7, cityHash64(number) % 256)) AS x
    FROM numbers(65536 * 64)
);

SELECT count(), sum(x1), sum(x1) = sum(x6) AND sum(x6) = sum(x12),
    sum(cityHash64(x1)) = (SELECT sum(cityHash64(toUInt8(if(number % 65536 < 100 + intDiv(number, 65536), number % 7, cityHash64(number) % 256)))) FROM numbers(65536 * 64))
FROM t_gdeflate_full_pages;

DROP TABLE t_gdeflate_full_pages;
