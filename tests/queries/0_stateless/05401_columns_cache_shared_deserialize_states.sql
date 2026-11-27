-- Tags: no-parallel
-- no-parallel: `SYSTEM DROP COLUMNS CACHE` clears the cache of every test, and the reads of the test rely on the
-- admission to the cache, which other tests filling the cache at the same time can make probabilistic.

-- The columns cache serves a subcolumn for some granules and reads it from the part for others, so the reader that decodes
-- the streams shared by the subcolumns of a column changes between calls. The deserialize states of such streams are shared
-- by those readers, so the new decoder continues from where the previous one stopped.

SET use_columns_cache = 1, max_threads = 1, merge_tree_use_deserialization_prefixes_cache = 1;
-- One granule per block: the range reader cuts the filtered tail of a granule only when it is more than half of the block.
SET max_block_size = 8192;

DROP TABLE IF EXISTS t_shared_states;

CREATE TABLE t_shared_states
(
    id UInt64,
    json JSON(max_dynamic_paths = 0),
    v Variant(String, UInt64),
    d Dynamic,
    t Tuple(a LowCardinality(String), b UInt8),
    jlc JSON(p LowCardinality(String), max_dynamic_paths = 0)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0, use_compact_variant_discriminators_serialization = 1,
    object_shared_data_serialization_version = 'advanced', object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 1;

-- The JSON values are parsed once, a thousand of them, and taken by index: parsing locks a mutex per value, and with the
-- thread fuzzer of the flaky check an insert that parses a value per row takes minutes. The values repeat every 1000 rows,
-- which is not a multiple of the distance between the granules a stale decoder confuses.
INSERT INTO t_shared_states
SELECT
    number,
    arrayElement(arrayMap(i -> format('{{"a":"a{0}","b":"b{0}"}}', toString(i))::JSON(max_dynamic_paths = 0), range(1000)), number % 1000 + 1),
    if(number % 3 = 0, concat('s', toString(number))::Dynamic, number::Dynamic)::Variant(String, UInt64),
    if(number % 3 = 0, concat('s', toString(number))::Dynamic, number::Dynamic),
    tuple(toString(number % 100), number % 7),
    arrayElement(arrayMap(i -> format('{{"p":"{0}","a":"a{1}"}}', toString(i % 100), toString(i))::JSON(p LowCardinality(String), max_dynamic_paths = 0), range(1000)), number % 1000 + 1)
FROM numbers(200000)
SETTINGS max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000;

-- Granule g holds ids [8192 * g, 8192 * (g + 1)). The first two reads of each case put granules 7..14 and 17..22 of one
-- subcolumn into the cache (a key range also covers the granule after its upper bound). The third read covers granule 12,
-- a part of 13, then 15 .. a part of 18: the other subcolumn decodes alone while the cached one is served and stops inside
-- granule 13, the cached one decodes at 15, and from 16 the other one decodes again and must continue from 15, not from 13.
-- The cached subcolumn comes first in the read, so that it is the one that decodes when both are read from the part.

-- Dynamic shared data of JSON: the structure of the chunks.
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(json.a) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(json.a) FORMAT Null;
SELECT 'json, a cached', count(), countIf(toString(json.a) != concat('a', toString(id % 1000))), countIf(toString(json.b) != concat('b', toString(id % 1000)))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(json.b) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(json.b) FORMAT Null;
SELECT 'json, b cached', count(), countIf(toString(json.b) != concat('b', toString(id % 1000))), countIf(toString(json.a) != concat('a', toString(id % 1000)))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

-- Variant: the compact discriminators.
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(v.UInt64) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(v.UInt64) FORMAT Null;
SELECT 'v, UInt64 cached', count(),
    countIf((v.UInt64 IS NULL) != (id % 3 = 0)), countIf(v.UInt64 IS NOT NULL AND v.UInt64 != id),
    countIf((v.String IS NULL) != (id % 3 != 0)), countIf(v.String IS NOT NULL AND v.String != concat('s', toString(id)))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(v.String) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(v.String) FORMAT Null;
SELECT 'v, String cached', count(),
    countIf((v.String IS NULL) != (id % 3 != 0)), countIf(v.String IS NOT NULL AND v.String != concat('s', toString(id))),
    countIf((v.UInt64 IS NULL) != (id % 3 = 0)), countIf(v.UInt64 IS NOT NULL AND v.UInt64 != id)
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

-- Dynamic: the discriminators of its Variant.
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(d.UInt64) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(d.UInt64) FORMAT Null;
SELECT 'd, UInt64 cached', count(),
    countIf((d.UInt64 IS NULL) != (id % 3 = 0)), countIf(d.UInt64 IS NOT NULL AND d.UInt64 != id),
    countIf((d.String IS NULL) != (id % 3 != 0)), countIf(d.String IS NOT NULL AND d.String != concat('s', toString(id)))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(d.String) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(d.String) FORMAT Null;
SELECT 'd, String cached', count(),
    countIf((d.String IS NULL) != (id % 3 != 0)), countIf(d.String IS NOT NULL AND d.String != concat('s', toString(id))),
    countIf((d.UInt64 IS NULL) != (id % 3 = 0)), countIf(d.UInt64 IS NOT NULL AND d.UInt64 != id)
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

-- LowCardinality inside Tuple: the dictionary and the index block of the element, read by the column and by the subcolumn.
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(t.a) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(t.a) FORMAT Null;
SELECT 't, a cached', count(), countIf(t.a != toString(id % 100)), countIf(toString(t) != format('(\'{0}\',{1})', toString(id % 100), toString(id % 7)))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(t) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(t) FORMAT Null;
SELECT 't, whole cached', count(), countIf(toString(t) != format('(\'{0}\',{1})', toString(id % 100), toString(id % 7))), countIf(t.a != toString(id % 100))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

-- LowCardinality typed path of JSON.
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(jlc.p) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(jlc.p) FORMAT Null;
SELECT 'jlc, p cached', count(), countIf(jlc.p != toString(id % 100)), countIf(NOT (toString(jlc) LIKE concat('%"p":"', toString(id % 100), '"%')))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(jlc) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(jlc) FORMAT Null;
SELECT 'jlc, whole cached', count(), countIf(NOT (toString(jlc) LIKE concat('%"p":"', toString(id % 100), '"%'))), countIf(jlc.p != toString(id % 100))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

-- The readers of LowCardinality share its state through the deserialize states cache, also without the prefixes cache.
SET merge_tree_use_deserialization_prefixes_cache = 0;

SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(t.a) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(t.a) FORMAT Null;
SELECT 't, a cached, no prefixes cache', count(), countIf(t.a != toString(id % 100)), countIf(toString(t) != format('(\'{0}\',{1})', toString(id % 100), toString(id % 7)))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_states WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(t) FORMAT Null;
SELECT count() FROM t_shared_states WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(t) FORMAT Null;
SELECT 't, whole cached, no prefixes cache', count(), countIf(toString(t) != format('(\'{0}\',{1})', toString(id % 100), toString(id % 7))), countIf(t.a != toString(id % 100))
FROM t_shared_states PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999);

DROP TABLE t_shared_states;
