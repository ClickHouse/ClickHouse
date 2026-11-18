-- Tags: no-fasttest
-- no-fasttest: Parquet support is not built in fast-test images

SET enable_nullable_tuple_type = 1, engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05835.parquet', Parquet)
SELECT if(number % 2 = 0, NULL, tuple((number + 0) % 251, (number + 1) % 251, (number + 2) % 251, (number + 3) % 251, (number + 4) % 251, (number + 5) % 251, (number + 6) % 251, (number + 7) % 251, (number + 8) % 251, (number + 9) % 251, (number + 10) % 251, (number + 11) % 251, (number + 12) % 251, (number + 13) % 251, (number + 14) % 251, (number + 15) % 251))::Nullable(Tuple(a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8, h UInt8, i UInt8, j UInt8, k UInt8, l UInt8, m UInt8, n UInt8, o UInt8, p UInt8)) AS t
FROM numbers(3000000) SETTINGS output_format_parquet_row_group_size = 65536;

-- Each element of the tuple is decoded with its own copy of the null map of the tuple. The reader must free the
-- copies when it returns a row subgroup, not when it finishes the file: here they would take 45 MB.
SELECT count(), countIf(t IS NULL), sum(cityHash64(t))
FROM file(currentDatabase() || '_05835.parquet', Parquet, 't Nullable(Tuple(a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8, h UInt8, i UInt8, j UInt8, k UInt8, l UInt8, m UInt8, n UInt8, o UInt8, p UInt8))')
SETTINGS max_threads = 1, max_parsing_threads = 1, input_format_parquet_memory_low_watermark = '4Mi',
    input_format_parquet_memory_high_watermark = '8Mi', max_memory_usage = '16Mi';
