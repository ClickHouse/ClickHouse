-- Tags: no-fasttest
-- no-fasttest: the fast test build has no Parquet support

-- A Parquet NULL read into a non-Nullable column with a `DEFAULT` expression takes the expression's value, so a filter
-- on that value must return these rows. The filter used to reach the Parquet reader, which pruned by the type default.

SET engine_file_truncate_on_insert = 1;
SET input_format_null_as_default = 1;
SET input_format_defaults_for_omitted_fields = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_null_default.parquet', Parquet)
SELECT if(number % 2 = 0, NULL, toInt64(number)) AS k FROM numbers(10);

SELECT count() FROM file(currentDatabase() || '_null_default.parquet', Parquet, 'k Int64 DEFAULT -1') WHERE k = -1;
SELECT countIf(k = -1) FROM file(currentDatabase() || '_null_default.parquet', Parquet, 'k Int64 DEFAULT -1');
SELECT count() FROM file(currentDatabase() || '_null_default.parquet', Parquet, 'k Int64 DEFAULT -1') WHERE k < 0;
SELECT count() FROM file(currentDatabase() || '_null_default.parquet', Parquet, 'k Int64 DEFAULT -1') WHERE k IN (-1, 3);
SELECT k FROM file(currentDatabase() || '_null_default.parquet', Parquet, 'k Int64 DEFAULT -1') WHERE k > 4 ORDER BY k;
