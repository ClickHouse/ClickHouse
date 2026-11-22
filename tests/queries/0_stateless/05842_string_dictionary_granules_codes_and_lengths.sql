-- Dictionary granules with codes of 0, 1, 3, 9 and 17 bits read back equal to a `with_size_stream` twin, also
-- through blocks that end inside granules, and so do their lengths, read alone and with the values. The
-- granules of 131072 rows hold 1, 2, 8, 300 and 70000 distinct values; the last granule of `wide` has no
-- repeated value and stays plain. A granule of a single value stores no codes.

DROP TABLE IF EXISTS t_codes_plain;
DROP TABLE IF EXISTS t_codes_dict;

CREATE TABLE t_codes_plain (id UInt64, one String, two String, eight String, many String, wide String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 131072, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

CREATE TABLE t_codes_dict (id UInt64, one String, two String, eight String, many String, wide String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 131072, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries';

INSERT INTO t_codes_plain SELECT number, 'constant', toString(number % 2), 'v' || toString(number % 8), 'w' || toString(number % 300), toString(number % 70000) FROM numbers(140000);
INSERT INTO t_codes_dict SELECT number, 'constant', toString(number % 2), 'v' || toString(number % 8), 'w' || toString(number % 300), toString(number % 70000) FROM numbers(140000);

SELECT 'values', (SELECT sum(cityHash64(id, one, two, eight, many, wide)) FROM t_codes_plain)
    = (SELECT sum(cityHash64(id, one, two, eight, many, wide)) FROM t_codes_dict);
SELECT 'values in small blocks', (SELECT sum(cityHash64(id, one, two, eight, many, wide)) FROM t_codes_plain SETTINGS max_block_size = 1000)
    = (SELECT sum(cityHash64(id, one, two, eight, many, wide)) FROM t_codes_dict SETTINGS max_block_size = 1000);
SELECT 'lengths alone', (SELECT sum(length(one) + 2 * length(two) + 3 * length(eight) + 5 * length(many) + 7 * length(wide)) FROM t_codes_plain SETTINGS max_block_size = 1000)
    = (SELECT sum(length(one) + 2 * length(two) + 3 * length(eight) + 5 * length(many) + 7 * length(wide)) FROM t_codes_dict SETTINGS max_block_size = 1000);
SELECT 'lengths with values', (SELECT sum(cityHash64(id, wide, length(wide), empty(many))) FROM t_codes_plain SETTINGS max_block_size = 1000)
    = (SELECT sum(cityHash64(id, wide, length(wide), empty(many))) FROM t_codes_dict SETTINGS max_block_size = 1000);
SELECT 'single value', max(column_data_uncompressed_bytes) < 100 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_codes_dict' AND active AND column = 'one';

DROP TABLE t_codes_plain;
DROP TABLE t_codes_dict;
