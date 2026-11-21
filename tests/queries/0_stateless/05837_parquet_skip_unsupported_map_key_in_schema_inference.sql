-- With `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference`, a map whose key has an
-- unsupported type is skipped as a whole, and the columns after it are still inferred and read.

SET engine_file_truncate_on_insert = 1;

-- T1: Int256 is written as DECIMAL(77, 0), wider than any ClickHouse `Decimal`.
INSERT INTO FUNCTION file(currentDatabase() || '_05837_key.parquet')
SELECT map(toInt256(number + 1), number + 10) AS m, number + 100 AS x FROM numbers(2)
SETTINGS output_format_parquet_wide_integer_as_decimal = 1;

-- T2: the same map inside a tuple, inside an array, as the key of another map; and an unsupported value.
INSERT INTO FUNCTION file(currentDatabase() || '_05837_nested.parquet')
SELECT
    CAST(tuple(map(toInt256(number + 1), number + 10), number + 20), 'Tuple(m Map(Int256, UInt64), y UInt64)') AS t,
    [map(toInt256(number + 1), number + 30)] AS a,
    map(map(toInt256(number + 1), number + 40), number + 50) AS mm,
    map(number + 60, toInt256(number + 1)) AS v,
    number + 100 AS x
FROM numbers(2)
SETTINGS output_format_parquet_wide_integer_as_decimal = 1;

SET input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

DESCRIBE file(currentDatabase() || '_05837_key.parquet');
SELECT * FROM file(currentDatabase() || '_05837_key.parquet') ORDER BY x;
DESCRIBE file(currentDatabase() || '_05837_nested.parquet');
SELECT * FROM file(currentDatabase() || '_05837_nested.parquet') ORDER BY x;
