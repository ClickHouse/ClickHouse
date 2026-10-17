-- Round trip through the Parquet writer and reader, covering the PLAIN and dictionary encodings of
-- every physical type, the fallback from dictionary to PLAIN, and the RLE / bit-packed hybrid
-- encoding of levels and dictionary indexes: long runs, short runs, and distinct values.

SET engine_file_truncate_on_insert = 1;
SET output_format_parquet_row_group_size = 1000000;
-- Not a multiple of 8, so that bit-packed booleans and groups of RLE values span batches.
SET output_format_parquet_batch_size = 13;
-- Many pages.
SET output_format_parquet_data_page_size = 4096;

DROP TABLE IF EXISTS t_05406;
CREATE TABLE t_05406
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

-- `x` has long runs of equal values, then short runs, then only distinct values.
INSERT INTO t_05406
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
INSERT INTO FUNCTION file(currentDatabase() || '_05406.parquet') SELECT * FROM t_05406;
SELECT count(), sum(cityHash64(*)) = (SELECT sum(cityHash64(*)) FROM t_05406)
FROM file(currentDatabase() || '_05406.parquet', Parquet, 'b Bool, i32 Int32, i64 Int64, f32 Float32, f64 Float64, s String, fs FixedString(3), d Decimal128(5), n Nullable(Int64), a Array(Nullable(String)), aa Array(Array(UInt8))');

SELECT 'plain';
INSERT INTO FUNCTION file(currentDatabase() || '_05406.parquet') SELECT * FROM t_05406 SETTINGS output_format_parquet_max_dictionary_size = 0;
SELECT count(), sum(cityHash64(*)) = (SELECT sum(cityHash64(*)) FROM t_05406)
FROM file(currentDatabase() || '_05406.parquet', Parquet, 'b Bool, i32 Int32, i64 Int64, f32 Float32, f64 Float64, s String, fs FixedString(3), d Decimal128(5), n Nullable(Int64), a Array(Nullable(String)), aa Array(Array(UInt8))');

SELECT 'dictionary too big, fallback to plain';
INSERT INTO FUNCTION file(currentDatabase() || '_05406.parquet') SELECT * FROM t_05406 SETTINGS output_format_parquet_max_dictionary_size = 1000;
SELECT count(), sum(cityHash64(*)) = (SELECT sum(cityHash64(*)) FROM t_05406)
FROM file(currentDatabase() || '_05406.parquet', Parquet, 'b Bool, i32 Int32, i64 Int64, f32 Float32, f64 Float64, s String, fs FixedString(3), d Decimal128(5), n Nullable(Int64), a Array(Nullable(String)), aa Array(Array(UInt8))');

-- The dictionary keeps floats with different bits apart: zero and negative zero, and NaNs with different payloads.
SELECT 'floats';
INSERT INTO FUNCTION file(currentDatabase() || '_05406.parquet')
SELECT reinterpretAsFloat64(bits.1) AS f64, reinterpretAsFloat32(bits.2) AS f32
FROM
(
    SELECT arrayJoin([
        (0::UInt64, 0::UInt32),
        (0x8000000000000000, 0x80000000),
        (0x7FF8000000000000, 0x7FC00000),
        (0x7FF8000000000001, 0x7FC00001),
        (0xFFF8000000000000, 0xFFC00000),
        (0, 0),
        (0x8000000000000000, 0x80000000)]) AS bits
);
SELECT reinterpretAsUInt64(f64), reinterpretAsUInt32(f32) FROM file(currentDatabase() || '_05406.parquet', Parquet, 'f64 Float64, f32 Float32');

DROP TABLE t_05406;
