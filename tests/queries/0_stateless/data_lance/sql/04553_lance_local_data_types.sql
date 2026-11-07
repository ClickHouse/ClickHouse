DROP TABLE IF EXISTS lance_local_rich_types;
DROP TABLE IF EXISTS lance_local_map;
DROP TABLE IF EXISTS lance_local_nullable_array_without_nulls;
DROP TABLE IF EXISTS lance_local_nullable_containers;
DROP TABLE IF EXISTS lance_local_nullable_struct;

SET print_pretty_type_names = 0;

CREATE TABLE lance_local_rich_types
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/rich_types.lance');

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 'lance_local_rich_types'
ORDER BY position
FORMAT Null;

SELECT
    throwIf(NOT (
        count(bool_value) = 2
        AND count(int8_value) = 2
        AND count(int16_value) = 2
        AND count(int32_value) = 2
        AND count(int64_value) = 2
        AND count(uint8_value) = 2
        AND count(uint16_value) = 2
        AND count(uint32_value) = 2
        AND count(uint64_value) = 2
        AND count(float32_value) = 2
        AND count(float64_value) = 2
        AND count(string_value) = 2
        AND count(large_string_value) = 2
        AND count(binary_value) = 2
        AND count(large_binary_value) = 2
        AND count(decimal_value) = 2
        AND count(date_value) = 2
        AND count(timestamp_s) = 2
        AND count(timestamp_ms) = 2
        AND count(timestamp_us) = 2
        AND count(timestamp_ns) = 2
        AND count(time_s) = 2
        AND count(time_ms) = 2
        AND count(time_us) = 2
        AND count(time_ns) = 2
        AND count(duration_us) = 2
        AND count(array_value) = 3
        AND count(fixed_array_value) = 3
        AND count(struct_value) = 3))
FROM lance_local_rich_types
FORMAT Null;

SELECT throwIf(
    toTypeName(bool_value) != 'Nullable(Bool)'
    OR toTypeName(decimal_value) != 'Nullable(Decimal(18, 4))'
    OR toTypeName(timestamp_ms) != 'Nullable(DateTime64(3, ''UTC''))'
    OR toTypeName(time_us) != 'Nullable(Time64(6))'
    OR toTypeName(duration_us) != 'Nullable(IntervalMicrosecond)'
    OR toTypeName(array_value) != 'Array(Nullable(Int32))'
    OR toTypeName(fixed_array_value) != 'Array(Nullable(Float32))'
    OR toTypeName(struct_value) != 'Tuple(code Int32, label Nullable(String))')
FROM lance_local_rich_types
LIMIT 1
FORMAT Null;

SELECT
    bool_value,
    int8_value,
    uint64_value,
    float64_value,
    string_value,
    binary_value,
    decimal_value,
    date_value,
    timestamp_ms,
    time_us,
    duration_us,
    array_value,
    fixed_array_value,
    struct_value
FROM lance_local_rich_types
ORDER BY int32_value NULLS LAST
FORMAT Null;

CREATE TABLE lance_local_map
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/map.lance');

SELECT
    throwIf(toTypeName(m) != 'Map(String, Nullable(Int32))'),
    mapKeys(m),
    mapValues(m)
FROM lance_local_map
FORMAT Null;

CREATE TABLE lance_local_nullable_array_without_nulls
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/nullable_array_without_nulls.lance');

SELECT
    throwIf(count() != 3 OR count(nullable_array) != 3)
FROM lance_local_nullable_array_without_nulls
FORMAT Null;

SELECT
    throwIf(toTypeName(nullable_array) != 'Array(Nullable(Int32))'),
    nullable_array
FROM lance_local_nullable_array_without_nulls
FORMAT Null;

SET enable_nullable_tuple_type = 0;

CREATE TABLE lance_local_nullable_containers
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/nullable_containers.lance');

SELECT nullable_array FROM lance_local_nullable_containers; -- { serverError BAD_ARGUMENTS }
SELECT throwIf(toTypeName(nullable_struct) != 'Tuple(value Int32)')
FROM lance_local_nullable_containers
LIMIT 1
FORMAT Null;

SELECT throwIf(count(nullable_struct) != 3)
FROM lance_local_nullable_containers
FORMAT Null;

SET enable_nullable_tuple_type = 1;

CREATE TABLE lance_local_nullable_struct
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/nullable_containers.lance');

SELECT throwIf(count() != 3 OR count(nullable_struct) != 3)
FROM lance_local_nullable_struct
FORMAT Null;

SELECT
    throwIf(toTypeName(nullable_struct) != 'Nullable(Tuple(value Int32))'),
    nullable_struct
FROM lance_local_nullable_struct
FORMAT Null;

DROP TABLE lance_local_nullable_struct;
DROP TABLE lance_local_nullable_containers;
DROP TABLE lance_local_nullable_array_without_nulls;
DROP TABLE lance_local_map;
DROP TABLE lance_local_rich_types;

SET enable_nullable_tuple_type = 0;
