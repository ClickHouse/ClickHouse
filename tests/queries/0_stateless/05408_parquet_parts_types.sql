-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- Values of all data types are read back from data parts in Parquet format without changes.
-- The types without a lossless representation in Parquet are stored in the ClickHouse binary format.

SET allow_experimental_time_time64_type = 1, allow_suspicious_low_cardinality_types = 1;
SET enable_json_type = 1, allow_experimental_variant_type = 1, allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS t_types_reference;
DROP TABLE IF EXISTS t_types_parquet;

CREATE TABLE t_types_reference
(
    id UInt64,
    u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64, u128 UInt128, u256 UInt256,
    i8 Int8, i16 Int16, i32 Int32, i64 Int64, i128 Int128, i256 Int256,
    f32 Float32, f64 Float64, bf16 BFloat16, b Bool,
    s String, fs FixedString(5),
    d Date, d32 Date32, dt DateTime, dt_tz DateTime('Asia/Tokyo'), dt64 DateTime64(3), dt64_1 DateTime64(1, 'UTC'), dt64_9 DateTime64(9),
    t Time, t64 Time64(3),
    dec32 Decimal32(2), dec64 Decimal64(4), dec128 Decimal128(10), dec256 Decimal256(20),
    uuid UUID, ip4 IPv4, ip6 IPv6,
    e8 Enum8('a' = 1, 'b' = -2), e16 Enum16('x' = 1000, 'y' = -1000),
    lc LowCardinality(String), lcn LowCardinality(Nullable(String)), lcfs LowCardinality(FixedString(3)),
    ni Nullable(Int32), ns Nullable(String), nd Nullable(Decimal64(3)),
    arr Array(Int32), arr_ns Array(Nullable(String)), arr2 Array(Array(Int8)), arr_lc Array(LowCardinality(String)),
    tup Tuple(a Int32, b String), tup_unnamed Tuple(Int32, String), tup_nested Tuple(a Tuple(b Tuple(c Int8))),
    ntup Nullable(Tuple(a Int32, b String)), ntup_complex Nullable(Tuple(a Nullable(Int32), b Array(String))),
    arr_tup Array(Tuple(a Int32, b Array(String))),
    map Map(String, UInt64), map_complex Map(LowCardinality(String), Tuple(Nullable(Decimal(9, 2)), Array(UUID))),
    point Point, ring Ring, polygon Polygon, multipolygon MultiPolygon, linestring LineString,
    var Variant(Int32, String, Array(UInt8)), dyn Dynamic, json JSON,
    arr_var Array(Variant(UInt64, String)),
    agg AggregateFunction(sum, UInt64), sagg SimpleAggregateFunction(max, UInt64)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_types_reference SELECT * REPLACE (arrayReduce('sumState', [agg]) AS agg) FROM generateRandom(
    'id UInt64,
    u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64, u128 UInt128, u256 UInt256,
    i8 Int8, i16 Int16, i32 Int32, i64 Int64, i128 Int128, i256 Int256,
    f32 Float32, f64 Float64, bf16 BFloat16, b Bool,
    s String, fs FixedString(5),
    d Date, d32 Date32, dt DateTime, dt_tz DateTime(\'Asia/Tokyo\'), dt64 DateTime64(3), dt64_1 DateTime64(1, \'UTC\'), dt64_9 DateTime64(9),
    t Time, t64 Time64(3),
    dec32 Decimal32(2), dec64 Decimal64(4), dec128 Decimal128(10), dec256 Decimal256(20),
    uuid UUID, ip4 IPv4, ip6 IPv6,
    e8 Enum8(\'a\' = 1, \'b\' = -2), e16 Enum16(\'x\' = 1000, \'y\' = -1000),
    lc LowCardinality(String), lcn LowCardinality(Nullable(String)), lcfs LowCardinality(FixedString(3)),
    ni Nullable(Int32), ns Nullable(String), nd Nullable(Decimal64(3)),
    arr Array(Int32), arr_ns Array(Nullable(String)), arr2 Array(Array(Int8)), arr_lc Array(LowCardinality(String)),
    tup Tuple(a Int32, b String), tup_unnamed Tuple(Int32, String), tup_nested Tuple(a Tuple(b Tuple(c Int8))),
    ntup Nullable(Tuple(a Int32, b String)), ntup_complex Nullable(Tuple(a Nullable(Int32), b Array(String))),
    arr_tup Array(Tuple(a Int32, b Array(String))),
    map Map(String, UInt64), map_complex Map(LowCardinality(String), Tuple(Nullable(Decimal(9, 2)), Array(UUID))),
    point Point, ring Ring, polygon Polygon, multipolygon MultiPolygon, linestring LineString,
    var Variant(Int32, String, Array(UInt8)), dyn Dynamic, json JSON,
    arr_var Array(Variant(UInt64, String)),
    agg UInt64, sagg UInt64', 42, 10, 3)
LIMIT 1000
SETTINGS input_format_parallel_parsing = 0;

CREATE TABLE t_types_parquet AS t_types_reference
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 100;

INSERT INTO t_types_parquet SELECT * FROM t_types_reference;

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_types_parquet' AND active;

SELECT 'difference';
SELECT count() FROM
(
    SELECT id, formatRow('TSV', * EXCEPT id) FROM t_types_parquet
    EXCEPT
    SELECT id, formatRow('TSV', * EXCEPT id) FROM t_types_reference
);
SELECT count() FROM t_types_parquet;

SELECT 'subcolumns';
SELECT count() FROM
(
    SELECT id, ni.null, arr.size0, tup.a, ntup.b, map.keys, var.String, dyn.Int64, json.a.b, arr_var.UInt64 FROM t_types_parquet
    EXCEPT
    SELECT id, ni.null, arr.size0, tup.a, ntup.b, map.keys, var.String, dyn.Int64, json.a.b, arr_var.UInt64 FROM t_types_reference
);

SELECT 'aggregate function';
SELECT sumMerge(agg) = (SELECT sumMerge(agg) FROM t_types_reference) FROM t_types_parquet;

DROP TABLE t_types_reference;
DROP TABLE t_types_parquet;
