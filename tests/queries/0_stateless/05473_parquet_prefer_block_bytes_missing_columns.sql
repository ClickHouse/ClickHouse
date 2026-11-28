-- Tags: no-fasttest
-- The block size that `input_format_parquet_prefer_block_bytes` asks for counts the columns that are not in the file
-- and are filled with default values: here a `FixedString(256)` takes most of the memory of a row.

INSERT INTO FUNCTION file(currentDatabase() || '_05473.parquet') SELECT number AS id FROM numbers(200000) SETTINGS engine_file_truncate_on_insert = 1;

SET input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_allow_missing_columns = 1, max_threads = 1;

SELECT max(blockSize()) < 10000, count(), any(length(missing))
FROM file(currentDatabase() || '_05473.parquet', Parquet, 'id UInt64, missing FixedString(256)')
WHERE id >= 0
SETTINGS input_format_parquet_prefer_block_bytes = 1000000;
