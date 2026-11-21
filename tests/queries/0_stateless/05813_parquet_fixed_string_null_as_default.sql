-- A NULL of a nullable Parquet column decoded as FixedString is read as the default with input_format_null_as_default.
-- Read as `String`, it is the default `FixedString` value converted to `String`, which keeps its zero bytes.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_fixed_string_null.parquet', Parquet,
    'w Nullable(FixedString(2)), i Nullable(Int32), a Array(Nullable(FixedString(2)))')
VALUES ('aa', 1, ['aa', NULL, 'bb']), (NULL, NULL, []), ('bb', 2, []);

SET input_format_null_as_default = 1;

SELECT hex(w) FROM file(currentDatabase() || '_fixed_string_null.parquet', Parquet, 'w FixedString(2)');
SELECT hex(w), length(w) FROM file(currentDatabase() || '_fixed_string_null.parquet', Parquet, 'w String');
SELECT hex(i) FROM file(currentDatabase() || '_fixed_string_null.parquet', Parquet, 'i FixedString(4)');
SELECT arrayMap(x -> hex(x), a) FROM file(currentDatabase() || '_fixed_string_null.parquet', Parquet, 'a Array(FixedString(2))');
