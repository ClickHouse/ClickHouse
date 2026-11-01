-- Tags: no-fasttest
-- Tag no-fasttest: ORC is not built in fasttest.

-- ORC filter pushdown through a monotonic function chain over a column.
-- `count()` under `indexHint` is the number of rows read, each row group has 10000 rows.

SET output_format_orc_row_index_stride = 10000;
SET input_format_orc_filter_push_down = 1;
SET engine_file_truncate_on_insert = 1;
SET max_insert_threads = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05495.orc', ORC) SELECT number AS id FROM numbers(1000000);

SELECT 'rows', toTypeName(id), count() FROM file(currentDatabase() || '_05495.orc', ORC) GROUP BY toTypeName(id);

SELECT 'bare', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id = 555555);
SELECT 'between', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id BETWEEN 555555 AND 555555);
SELECT 'toUInt64', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(toUInt64(id) = 555555);
SELECT 'toInt64', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(toInt64(id) = 555555);
SELECT 'toFloat64', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(toFloat64(id) = 555555.0);
SELECT 'plus0', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id + 0 = 555555);
SELECT 'minus0', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id - 0 = 555555);
SELECT 'mul1', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id * 1 = 555555);
SELECT 'plus1', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id + 1 = 555556);
SELECT 'mul2', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id * 2 = 1111110);
SELECT 'intDiv1', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(intDiv(id, 1) = 555555);
SELECT 'intDiv10', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(intDiv(id, 10) = 55555);
SELECT 'neg', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(-id = -555555);
SELECT 'in', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id + 1 IN (1, 500001, 999999));
SELECT 'mixed', count() FROM file(currentDatabase() || '_05495.orc', ORC) WHERE indexHint(id + 1 = 555556 OR id = 5);

-- The same results with and without the pushdown.
SELECT 'intDiv10', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE intDiv(id, 10) = 55555;
SELECT 'intDiv10', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE intDiv(id, 10) = 55555
    SETTINGS input_format_orc_filter_push_down = 0;
SELECT 'neg', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE -id = -555555;
SELECT 'neg', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE -id = -555555
    SETTINGS input_format_orc_filter_push_down = 0;
SELECT 'in 8192', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE id + 1 IN (1, 500001, 999999)
    SETTINGS input_format_orc_row_batch_size = 8192;
SELECT 'in 8192', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE id + 1 IN (1, 500001, 999999)
    SETTINGS input_format_orc_row_batch_size = 8192, input_format_orc_filter_push_down = 0;
SELECT 'in 10000', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE id + 1 IN (1, 500001, 999999)
    SETTINGS input_format_orc_row_batch_size = 10000;
SELECT 'in 10000', count(), min(id), max(id), sum(id) FROM file(currentDatabase() || '_05495.orc', ORC) WHERE id + 1 IN (1, 500001, 999999)
    SETTINGS input_format_orc_row_batch_size = 10000, input_format_orc_filter_push_down = 0;

-- A NULL satisfies `IS NULL` in a row group whose other values do not satisfy the chain.
INSERT INTO FUNCTION file(currentDatabase() || '_05495_null.orc', ORC)
    SELECT if(number = 195000, NULL, 1000000 + number)::Nullable(Int64) AS id FROM numbers(200000);
SELECT 'null or', count() FROM file(currentDatabase() || '_05495_null.orc', ORC) WHERE id + 1 = 999 OR id IS NULL;

-- With `input_format_null_as_default` a NULL is read as the default of a non-Nullable column.
INSERT INTO FUNCTION file(currentDatabase() || '_05495_default.orc', ORC)
    SELECT if(number % 7 = 0, NULL, number + 1)::Nullable(Int64) AS id FROM numbers(200000);
SELECT 'default', count() FROM file(currentDatabase() || '_05495_default.orc', ORC, 'id Int64') WHERE id + 1 = 1
    SETTINGS input_format_null_as_default = 1;
