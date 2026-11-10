-- Tags: no-fasttest, no-replicated-database
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`
-- Requires Parquet and `IcebergLocal` (USE_AVRO).
-- A Parquet `LIST` or `MAP` used as a `MAP` key is read as `Array` or `Map`: https://github.com/ClickHouse/ClickHouse/issues/130000

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_map_keys.parquet')
SELECT number AS id,
    map([1, 2], 'a')::Map(Array(Nullable(Float32)), String) AS issue,
    map(range(number), toString(number), [100, NULL], 'x')::Map(Array(Nullable(UInt64)), String) AS list_key,
    map(map('k', number), number)::Map(Map(String, UInt64), UInt64) AS map_key,
    map([[number], []], number)::Map(Array(Array(UInt64)), UInt64) AS list_of_lists_key,
    map([map('a', number)], number)::Map(Array(Map(String, UInt64)), UInt64) AS list_of_maps_key,
    map(number, map([number], 'v'))::Map(UInt64, Map(Array(UInt64), String)) AS list_key_in_value,
    [map([number], 'e')]::Array(Map(Array(UInt64), String)) AS list_key_in_array,
    map(tuple([number]), 't')::Map(Tuple(a Array(UInt64)), String) AS list_in_struct_key
FROM numbers(3);

DESCRIBE file(currentDatabase() || '_map_keys.parquet') SETTINGS print_pretty_type_names = 0;
SELECT * FROM file(currentDatabase() || '_map_keys.parquet') ORDER BY id;

-- Explicit types, as in a table definition or an Iceberg schema.
SELECT id, issue, list_key, map_key FROM file(currentDatabase() || '_map_keys.parquet', Parquet,
    'id UInt64, issue Map(Array(Float32), String), list_key Map(Array(Nullable(UInt64)), String), map_key Map(Map(String, UInt64), UInt64)') ORDER BY id;

SELECT id, list_key.keys, list_key.values, mapContains(list_key, [0]) FROM file(currentDatabase() || '_map_keys.parquet') ORDER BY id;
SELECT id FROM file(currentDatabase() || '_map_keys.parquet') WHERE has(mapKeys(list_key), [0]) ORDER BY id;

-- The literal tuple type of a `LIST` key still reads.
SELECT id, issue FROM file(currentDatabase() || '_map_keys.parquet', Parquet,
    'id UInt64, issue Map(Tuple(list Array(Tuple(element Nullable(Float32)))), String)') ORDER BY id;

-- A `JSON` key: a `LIST` key is read as the tuple of its children, a `MAP` key as the map.
SELECT id, issue, map_key FROM file(currentDatabase() || '_map_keys.parquet', Parquet,
    'id UInt64, issue Map(JSON, String), map_key Map(JSON, UInt64)') ORDER BY id;

SET allow_insert_into_iceberg = 1;
SET use_iceberg_metadata_files_cache = 0;
SET iceberg_delete_data_on_drop = 1;
SET async_insert = 0;

-- Resolve relative user-files paths against the server's data directory.
CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05569_parquet_list_and_map_as_map_key/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t_iceberg (id Int64, m Map(Array(Float32), String), n Map(Map(String, Int64), String), o Map(Array(Tuple(a Int64, b String)), String))
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');

INSERT INTO t_iceberg VALUES (1, map([1, 2], 'a'), map(map('k', 1), 'b'), map([(1, 'x'), (2, 'y')], 'c')), (2, map([], 'd', [3], 'e'), map(map(), 'f'), map([], 'g'));

SELECT * FROM t_iceberg ORDER BY id;
SELECT mapKeys(m), mapKeys(n), o.keys FROM t_iceberg ORDER BY id;
SELECT id FROM t_iceberg WHERE mapContains(m, [3]) ORDER BY id;

DROP TABLE t_iceberg;
