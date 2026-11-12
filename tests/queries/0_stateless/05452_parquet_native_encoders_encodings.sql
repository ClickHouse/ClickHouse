-- Tags: no-fasttest
-- no-fasttest: the `ParquetMetadata` format uses the Arrow library.

-- A companion to `05406_parquet_native_encoders`: check, with an independent reader of the file
-- metadata (`parquet-cpp`), that the column chunks written by the native writer really use the
-- encodings under test: dictionary, `PLAIN`, and the fallback from dictionary to `PLAIN`.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_row_group_size = 1000000;
SET output_format_parquet_batch_size = 13;
SET output_format_parquet_data_page_size = 4096;

DROP TABLE IF EXISTS t_05452;
CREATE TABLE t_05452
(
    b Bool,
    i32 Int32,
    i64 Int64,
    f32 Float32,
    f64 Float64,
    s String,
    fs FixedString(3),
    d Decimal128(5),
    n Nullable(Int64),
    a Array(Nullable(String)),
    aa Array(Array(UInt8))
) ENGINE = Memory;

INSERT INTO t_05452
SELECT
    x % 3 = 0,
    x,
    x * 1000000007,
    x / 7,
    -x / 3,
    if(x % 5 = 0, '', repeat('a', x % 17) || toString(x)),
    toFixedString(toString(x % 1000), 3),
    x / 100,
    if(x % 4 = 0, NULL, x),
    arrayMap(i -> if((x + i) % 3 = 0, NULL, toString(x + i)), range(x % 4)),
    arrayMap(i -> range(i % 3), range(x % 3))
FROM
(
    SELECT if(number < 3000, intDiv(number, 100), if(number < 6000, intDiv(number, 3), number)) AS x
    FROM numbers(10000)
);

SELECT 'dictionary';
INSERT INTO FUNCTION file(currentDatabase() || '_05452.parquet') SELECT * FROM t_05452;
SELECT c.path, arraySort(c.encodings)
FROM file(currentDatabase() || '_05452.parquet', ParquetMetadata)
ARRAY JOIN columns AS c;

SELECT 'plain';
INSERT INTO FUNCTION file(currentDatabase() || '_05452.parquet') SELECT * FROM t_05452 SETTINGS output_format_parquet_max_dictionary_size = 0;
SELECT c.path, arraySort(c.encodings)
FROM file(currentDatabase() || '_05452.parquet', ParquetMetadata)
ARRAY JOIN columns AS c;

-- The columns with many distinct values restart without the dictionary, the others keep it.
SELECT 'dictionary too big, fallback to plain';
INSERT INTO FUNCTION file(currentDatabase() || '_05452.parquet') SELECT * FROM t_05452 SETTINGS output_format_parquet_max_dictionary_size = 1000;
SELECT c.path, arraySort(c.encodings)
FROM file(currentDatabase() || '_05452.parquet', ParquetMetadata)
ARRAY JOIN columns AS c;

DROP TABLE t_05452;
