-- Tags: no-fasttest
-- Tag no-fasttest: ORC is not built in fasttest.

-- With `input_format_null_as_default` a NULL of a non-Nullable column is read as the default, which can match
-- under `NOT`. `count()` under `indexHint` is the number of rows read, 10000 per row group.

SET output_format_orc_row_index_stride = 10000;
SET input_format_orc_filter_push_down = 1;
SET input_format_null_as_default = 1;
SET engine_file_truncate_on_insert = 1;
SET max_insert_threads = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05839.orc', ORC)
SELECT
    if(number < 30000 AND number % 10000 = 5, NULL, 0.6 + (number % 100) / 1000)::Nullable(Float64) AS f,
    if(number < 30000 AND number % 10000 = 5, NULL, concat('abc', toString(number)))::Nullable(String) AS s,
    if(number < 30000 AND number % 10000 = 6, NULL,
        tuple(if(number < 30000 AND number % 10000 = 7, NULL, 0.6 + (number % 100) / 1000)))::Nullable(Tuple(x Nullable(Float64))) AS t
FROM numbers(40000);

SELECT 'f rows read', count() FROM file(currentDatabase() || '_05839.orc', ORC, 'f Float64') WHERE indexHint(NOT (f > 0.5));
SELECT 'f', count() FROM file(currentDatabase() || '_05839.orc', ORC, 'f Float64') WHERE NOT (f > 0.5);
SELECT 'f', count() FROM file(currentDatabase() || '_05839.orc', ORC, 'f Float64') WHERE NOT (f > 0.5)
    SETTINGS input_format_orc_filter_push_down = 0;

SELECT 's', count() FROM file(currentDatabase() || '_05839.orc', ORC, 's String') WHERE NOT startsWith(s, 'abc');
SELECT 's', count() FROM file(currentDatabase() || '_05839.orc', ORC, 's String') WHERE NOT startsWith(s, 'abc')
    SETTINGS input_format_orc_filter_push_down = 0;

SELECT 's lc', count() FROM file(currentDatabase() || '_05839.orc', ORC, 's LowCardinality(String)') WHERE NOT startsWith(s, 'abc');
SELECT 's lc', count() FROM file(currentDatabase() || '_05839.orc', ORC, 's LowCardinality(String)') WHERE NOT startsWith(s, 'abc')
    SETTINGS input_format_orc_filter_push_down = 0;

SELECT 't.x', count() FROM file(currentDatabase() || '_05839.orc', ORC, 't Tuple(x Float64)') WHERE NOT (t.x > 0.5);
SELECT 't.x', count() FROM file(currentDatabase() || '_05839.orc', ORC, 't Tuple(x Float64)') WHERE NOT (t.x > 0.5)
    SETTINGS input_format_orc_filter_push_down = 0;
