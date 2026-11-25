-- Tags: no-fasttest
-- no-fasttest: Parquet support is not built in fast-test images
-- Random settings limits: input_format_parquet_memory_high_watermark=(67108864, None); output_format_parquet_max_dictionary_size=(1048576, None)

-- An out-of-range compression codec of a column chunk makes the dictionary filter skip that column; reading the
-- column reports the corrupted codec.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05770.parquet')
SELECT toString(number % 3) AS a, toString(number % 3) AS bad_codec FROM numbers(100)
SETTINGS output_format_parquet_compression_method = 'zstd', output_format_parquet_write_bloom_filter = 0;

-- codec ZSTD -> -1 (compact protocol: `path_in_schema` list header and name, then the `codec` field header and
-- zigzag value). The writer stores the column chunk twice, after its data and in the footer, so patch both.
INSERT INTO FUNCTION file(currentDatabase() || '_05770_bad_codec.parquet', RawBLOB)
SELECT replaceAll(raw, unhex('191809') || 'bad_codec' || unhex('150C'), unhex('191809') || 'bad_codec' || unhex('1501'))
FROM file(currentDatabase() || '_05770.parquet', RawBLOB, 'raw String');

-- Both copies were patched.
SELECT countSubstrings(raw, unhex('191809') || 'bad_codec' || unhex('150C')), countSubstrings(raw, unhex('191809') || 'bad_codec' || unhex('1501'))
FROM file(currentDatabase() || '_05770_bad_codec.parquet', RawBLOB, 'raw String');

SELECT count() FROM file(currentDatabase() || '_05770.parquet') WHERE bad_codec = '1' SETTINGS input_format_parquet_dictionary_filter_push_down = 1048576;
SELECT count() FROM file(currentDatabase() || '_05770_bad_codec.parquet') WHERE bad_codec = '1' SETTINGS input_format_parquet_dictionary_filter_push_down = 1048576; -- { serverError INCORRECT_DATA }
-- '15' is inside the min/max range of `a` but not in its dictionary. Without PREWHERE, `bad_codec` is read unless the
-- dictionary filter skips the row group.
SELECT count() FROM file(currentDatabase() || '_05770_bad_codec.parquet') WHERE a = '15' AND bad_codec = '1' SETTINGS input_format_parquet_dictionary_filter_push_down = 1048576, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
SELECT count() FROM file(currentDatabase() || '_05770_bad_codec.parquet') WHERE a = '15' AND bad_codec = '1' SETTINGS input_format_parquet_dictionary_filter_push_down = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0; -- { serverError INCORRECT_DATA }
