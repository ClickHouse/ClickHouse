-- The `FSST` encoder splits a block into lanes at the boundaries of the values. With a few long values, the splits repeat
-- and some lanes are empty, which must not read past the boundaries of the values.

SET enable_fsst_codec = 1;
SET use_columns_cache = 0;
SET use_uncompressed_cache = 0;
SET apply_string_filters_during_scan = 1;

DROP TABLE IF EXISTS t_fsst_empty_lanes;

CREATE TABLE t_fsst_empty_lanes (id UInt32, s String CODEC(FSST), z String CODEC(FSST, LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0,
    string_serialization_version = 'with_size_stream', serialization_info_version = 'with_types';

-- Two values of 4000 bytes: the second and the fourth lanes are empty.
INSERT INTO t_fsst_empty_lanes SELECT number, repeat(toString(number), 4000), repeat(toString(number), 4000) FROM numbers(2);
-- A long value between two short ones: two empty lanes in the middle.
INSERT INTO t_fsst_empty_lanes SELECT 10 + number, if(number = 1, repeat('abc', 2600), repeat('x', 100)), if(number = 1, repeat('abc', 2600), repeat('x', 100)) FROM numbers(3);
-- Many short values and one long value at the end.
INSERT INTO t_fsst_empty_lanes SELECT 20 + number, if(number = 50, repeat('long', 3000), 'short'), if(number = 50, repeat('long', 3000), 'short') FROM numbers(51);

SELECT id, length(s), cityHash64(s), cityHash64(z) FROM t_fsst_empty_lanes ORDER BY id LIMIT 6;
SELECT count(), sum(length(s)), sum(cityHash64(s)) = sum(cityHash64(z)) FROM t_fsst_empty_lanes;
SELECT id FROM t_fsst_empty_lanes PREWHERE s LIKE '%abcabc%' ORDER BY id;
SELECT id FROM t_fsst_empty_lanes PREWHERE z LIKE 'longlong%' ORDER BY id;
SELECT count() FROM t_fsst_empty_lanes PREWHERE s = 'short';

DROP TABLE t_fsst_empty_lanes;
