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

SELECT 't.x', count() FROM file(currentDatabase() || '_05839.orc', ORC, 't Tuple(x Float64)') WHERE NOT (t.x > 0.5) SETTINGS log_comment = '05839_tx';
SELECT 't.x', count() FROM file(currentDatabase() || '_05839.orc', ORC, 't Tuple(x Float64)') WHERE NOT (t.x > 0.5)
    SETTINGS input_format_orc_filter_push_down = 0;

-- Under `NOT` a struct element prunes no row group, so all 40000 rows are read.
SYSTEM FLUSH LOGS query_log;
SELECT 't.x rows read', read_rows FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05839_tx';

-- `x <=> c` is false, not NULL, for a NULL, so its negation is true and the row groups with NULLs must be read.
-- The first row group holds only NULLs, the second only `5` (or `true`) and NULLs.
INSERT INTO FUNCTION file(currentDatabase() || '_05839_null_safe.orc', ORC)
SELECT
    multiIf(number < 10000, NULL, number < 20000, if(number % 2 = 0, NULL, 5), number)::Nullable(Int64) AS x,
    multiIf(number < 10000, NULL, number < 20000, if(number % 2 = 0, NULL, true), false)::Nullable(Bool) AS b,
    tuple(x)::Tuple(y Nullable(Int64)) AS t
FROM numbers(30000);

SELECT 'x <=> 5', count(), countIf(x IS NULL) FROM file(currentDatabase() || '_05839_null_safe.orc', ORC, 'x Nullable(Int64)') WHERE x <=> 5;
SELECT 'not x <=> 5', count(), countIf(x IS NULL) FROM file(currentDatabase() || '_05839_null_safe.orc', ORC, 'x Nullable(Int64)') WHERE NOT (x <=> 5);
SELECT 'not x <=> 5', count(), countIf(x IS NULL) FROM file(currentDatabase() || '_05839_null_safe.orc', ORC, 'x Nullable(Int64)') WHERE NOT (x <=> 5)
    SETTINGS input_format_orc_filter_push_down = 0;
SELECT 'not x is not distinct from 5', count(), countIf(x IS NULL) FROM file(currentDatabase() || '_05839_null_safe.orc', ORC, 'x Nullable(Int64)')
    WHERE NOT (x IS NOT DISTINCT FROM 5);
SELECT 'not x <=> 5 inferred', count(), countIf(x IS NULL) FROM file(currentDatabase() || '_05839_null_safe.orc', ORC) WHERE NOT (x <=> 5);
SELECT 'not b is true', count(), countIf(b IS NULL) FROM file(currentDatabase() || '_05839_null_safe.orc', ORC, 'b Nullable(Bool)') WHERE NOT (b IS TRUE);
SELECT 'not b is true', count(), countIf(b IS NULL) FROM file(currentDatabase() || '_05839_null_safe.orc', ORC, 'b Nullable(Bool)') WHERE NOT (b IS TRUE)
    SETTINGS input_format_orc_filter_push_down = 0;
SELECT 'not t.y <=> 5', count(), countIf(t.y IS NULL) FROM file(currentDatabase() || '_05839_null_safe.orc', ORC, 't Tuple(y Nullable(Int64))') WHERE NOT (t.y <=> 5);
