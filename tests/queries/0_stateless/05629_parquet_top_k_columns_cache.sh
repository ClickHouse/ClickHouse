-- Tags: no-fasttest
-- no-fasttest: Parquet support is not built in fast-test images

-- ORDER BY ... LIMIT over a Parquet file with many row groups, with the row groups served from the columns cache:
-- the top-K threshold is checked again when a row group is delivered, which must not lose rows of the result.

INSERT INTO FUNCTION file(currentDatabase() || '_05629.parquet', Parquet)
SELECT intDiv(sipHash64(number) % 1000000, 7) AS v, toString(number) AS s
FROM numbers(1000000)
SETTINGS output_format_parquet_row_group_size = 10000, max_block_size = 10000, engine_file_truncate_on_insert = 1;

SET max_threads = 16, max_parsing_threads = 16;

SELECT 'without the cache';
SELECT v, s FROM file(currentDatabase() || '_05629.parquet') ORDER BY v, s LIMIT 5 SETTINGS use_columns_cache = 0;
SELECT v, s FROM file(currentDatabase() || '_05629.parquet') ORDER BY v DESC, s LIMIT 5 SETTINGS use_columns_cache = 0;

SELECT 'with the cache, three times';
SELECT v, s FROM file(currentDatabase() || '_05629.parquet') ORDER BY v, s LIMIT 5 SETTINGS use_columns_cache = 1;
SELECT v, s FROM file(currentDatabase() || '_05629.parquet') ORDER BY v, s LIMIT 5 SETTINGS use_columns_cache = 1;
SELECT v, s FROM file(currentDatabase() || '_05629.parquet') ORDER BY v, s LIMIT 5 SETTINGS use_columns_cache = 1;
SELECT v, s FROM file(currentDatabase() || '_05629.parquet') ORDER BY v DESC, s LIMIT 5 SETTINGS use_columns_cache = 1;
SELECT v, s FROM file(currentDatabase() || '_05629.parquet') ORDER BY v DESC, s LIMIT 5 SETTINGS use_columns_cache = 1;
SELECT v, s FROM file(currentDatabase() || '_05629.parquet') ORDER BY v DESC, s LIMIT 5 SETTINGS use_columns_cache = 1;
