-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: the test pins the lookup of a `Map` key in its bucket of a data part, which `SharedMergeTree` does not do the same way.

-- `-0.` and `0.` are equal, but they are different constants: `1 / -0.` is `-inf`.
-- A long constant is named by the hash of its value, and the name identifies the constant in the actions DAG,
-- so the two zeros must not get the same name.
SELECT 1 / if(number % 2 = 0, arrayWithConstant(128, -0.), arrayWithConstant(128, 0.))[1] FROM numbers(4) SETTINGS compile_expressions = 0;
SELECT if(number % 2 = 0, -0., 0.) AS k, 1 / k FROM numbers(4) SETTINGS optimize_const_name_size = 0, compile_expressions = 0;

-- The bucket of a `Map` key is a part of the format of a data part, and it is chosen by the raw bits
-- of floating point values, also inside a `Variant`. A lookup of a key reads only its bucket, so `m[0.]`
-- does not find the key `-0.`, which is in another bucket (as it was in older versions).
DROP TABLE IF EXISTS t_map_variant_signed_zero;
CREATE TABLE t_map_variant_signed_zero (m Map(Variant(Float64, String), String), f Map(Float64, String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', max_buckets_in_map = 32, map_buckets_min_avg_size = 0, min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_types';

INSERT INTO t_map_variant_signed_zero SELECT
    mapFromArrays(arrayMap(x -> CAST(x, 'Variant(Float64, String)'), [-0., 1.5, 2.5, 3.5]), ['neg', 'a', 'b', 'c']),
    mapFromArrays([-0., 1.5, 2.5, 3.5], ['neg', 'a', 'b', 'c']);

SET optimize_functions_to_subcolumns = 1;
SELECT m[CAST(-0., 'Variant(Float64, String)')], m[CAST(0., 'Variant(Float64, String)')], m[CAST(2.5, 'Variant(Float64, String)')] FROM t_map_variant_signed_zero;
SELECT f[-0.], f[0.], f[2.5] FROM t_map_variant_signed_zero;
SELECT m, f FROM t_map_variant_signed_zero;

DROP TABLE t_map_variant_signed_zero;

-- The same inside `JSON`.
DROP TABLE IF EXISTS t_map_json_signed_zero;
CREATE TABLE t_map_json_signed_zero (m Map(JSON(a Float64), String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', max_buckets_in_map = 32, map_buckets_min_avg_size = 0, min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_types';

INSERT INTO t_map_json_signed_zero SELECT
    mapFromArrays(arrayMap(x -> CAST(x, 'JSON(a Float64)'), ['{"a":-0.0}', '{"a":1.5}', '{"a":2.5}', '{"a":3.5,"b":-0.0}']), ['neg', 'a', 'b', 'c']);

SELECT `m.key_{"a":-0.0}`, `m.key_{"a":0.0}`, `m.key_{"a":1.5}`, `m.key_{"a":3.5,"b":-0.0}` FROM t_map_json_signed_zero;
SELECT m FROM t_map_json_signed_zero;

DROP TABLE t_map_json_signed_zero;
