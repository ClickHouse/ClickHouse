-- Checks `pack_string_sizes`: the `.size` stream of a `String` column written with a separate size stream is compressed
-- with `BitPacking` in front of the column's generic codecs, and the strings and their sizes read back as written.

DROP TABLE IF EXISTS packed_sizes;
DROP TABLE IF EXISTS plain_sizes;

CREATE TABLE packed_sizes (k UInt32, s String, z String CODEC(ZSTD(3)), e String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream', pack_string_sizes = 1;

CREATE TABLE plain_sizes (k UInt32, s String, z String CODEC(ZSTD(3)), e String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream', pack_string_sizes = 0;

-- Strings of sizes from 0 to 70000, which take from 0 to 17 bits a size, and a block of them that ends in the middle
-- of a chunk of the packing.
INSERT INTO packed_sizes SELECT
    number,
    repeat('x', number % 7),
    if(number % 1000 = 0, repeat('y', 70000), toString(number)),
    ''
FROM numbers(300007);
INSERT INTO plain_sizes SELECT * FROM packed_sizes;

SELECT count(), sum(length(s)), sum(length(z)), sum(length(e)) FROM packed_sizes;
SELECT (SELECT sum(cityHash64(*)) FROM packed_sizes) = (SELECT sum(cityHash64(*)) FROM plain_sizes);
SELECT (SELECT sum(cityHash64(s.size, z.size, e.size)) FROM packed_sizes) = (SELECT sum(cityHash64(s.size, z.size, e.size)) FROM plain_sizes);
SELECT k, s, length(z), s.size, z.size FROM packed_sizes WHERE k IN (0, 6, 1000, 300006) ORDER BY k;

-- The sizes of `s` lie in 0..6: packed in 3 bits each, they take much less than the 8 bytes each the generic codec expands.
SELECT name, data_compressed_bytes < 300007 * 2 FROM system.columns WHERE database = currentDatabase() AND table = 'packed_sizes' AND name = 's';

DROP TABLE packed_sizes;
DROP TABLE plain_sizes;
