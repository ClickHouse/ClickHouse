-- The streams that only the deserialized prefixes list - of the dynamic paths of JSON, also inside Nullable, of the
-- variants of Dynamic and of the regions of SHARED paths - are opened once the prefixes are known, before a read, also
-- by the readers that take the prefixes from the cache of another reader of the part. A debug build checks that no read
-- opens a stream later. Results are compared with the same data with typed paths that are not SHARED.

SET enable_reads_from_columns_cache = 0, use_query_condition_cache = 0, enable_parallel_replicas = 0;
SET merge_tree_use_deserialization_prefixes_cache = 1, load_marks_asynchronously = 1;
SET max_threads = 4, merge_tree_min_rows_for_concurrent_read = 100, merge_tree_min_bytes_for_concurrent_read = 1;

DROP TABLE IF EXISTS t_streams_discovered;
DROP TABLE IF EXISTS t_streams_discovered_typed;

CREATE TABLE t_streams_discovered
(
    x UInt64,
    json JSON(SHARED a.k0 UInt64, SHARED a.k1 String, SHARED a.k2 Array(UInt32), SHARED b UInt64),
    d Dynamic,
    nj Nullable(JSON(SHARED a UInt64))
)
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100, object_typed_shared_data_serialization_version = 'v2';

CREATE TABLE t_streams_discovered_typed
(
    x UInt64,
    json JSON(a.k0 UInt64, a.k1 String, a.k2 Array(UInt32), b UInt64),
    d Dynamic,
    nj Nullable(JSON(a UInt64))
)
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100;

INSERT INTO t_streams_discovered_typed SELECT
    number,
    concat('{"a":{"k0":', toString(number), ',"k1":"s', toString(number % 10), '","k2":', toString(range(number % 3)), '},"b":', toString(number * 2),
           if(number % 2 = 0, concat(',"c":', toString(number)), ''), if(number % 3 = 0, concat(',"e":{"f":"f', toString(number), '"}'), ''), '}'),
    if(number % 2 = 0, number::Dynamic, [number]::Dynamic),
    if(number % 5 = 0, NULL, concat('{"a":', toString(number), ',"c":', toString(number % 7), if(number % 2 = 0, ',"s":"v"', ''), '}'))
FROM numbers(10000);
INSERT INTO t_streams_discovered SELECT * FROM t_streams_discovered_typed;

SELECT 'regions', (SELECT sum(cityHash64(x, json.a.k0, json.a.k1, json.a.k2, json.b)) FROM t_streams_discovered)
    = (SELECT sum(cityHash64(x, json.a.k0, json.a.k1, json.a.k2, json.b)) FROM t_streams_discovered_typed);

SELECT 'sub-object', (SELECT sum(cityHash64(x, toString(json.^a))) FROM t_streams_discovered
                      SETTINGS object_shared_data_max_sub_object_paths_to_read_separately = 128)
    = (SELECT sum(cityHash64(x, toString(json.^a))) FROM t_streams_discovered_typed);

SELECT 'dynamic paths', (SELECT sum(cityHash64(x, toString(json.c), toString(json.e.f))) FROM t_streams_discovered)
    = (SELECT sum(cityHash64(x, toString(json.c), toString(json.e.f))) FROM t_streams_discovered_typed);

SELECT 'dynamic', (SELECT sum(cityHash64(x, toString(d))) FROM t_streams_discovered)
    = (SELECT sum(cityHash64(x, toString(d))) FROM t_streams_discovered_typed);

SELECT 'whole', (SELECT sum(cityHash64(x, toString(json))) FROM t_streams_discovered)
    = (SELECT sum(cityHash64(x, toString(json))) FROM t_streams_discovered_typed);

SELECT 'nullable dynamic paths', (SELECT sum(cityHash64(x, toString(nj.c), toString(nj.s))) FROM t_streams_discovered)
    = (SELECT sum(cityHash64(x, toString(nj.c), toString(nj.s))) FROM t_streams_discovered_typed);

SELECT 'nullable shared path', (SELECT sum(cityHash64(x, nj.a)) FROM t_streams_discovered)
    = (SELECT sum(cityHash64(x, nj.a)) FROM t_streams_discovered_typed);

SELECT 'nullable whole', (SELECT sum(cityHash64(x, toString(nj))) FROM t_streams_discovered)
    = (SELECT sum(cityHash64(x, toString(nj))) FROM t_streams_discovered_typed);

DROP TABLE t_streams_discovered;
DROP TABLE t_streams_discovered_typed;
