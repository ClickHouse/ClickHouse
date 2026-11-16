SET enable_subcolumn_codecs = 1;
SET print_pretty_type_names = 0;

DROP TABLE IF EXISTS t_subcolumn_codecs;
DROP TABLE IF EXISTS t_subcolumn_codecs_sparse;

-- The three forms of a name are the same name; a declared subcolumn may be a container (payload.meta)
-- or an element of an unnamed Tuple, whose name needs quotes (`pair`.`2`).
CREATE TABLE t_subcolumn_codecs
(
    id UInt64,
    payload Tuple(ts DateTime64(3), value Float64, meta Tuple(seq UInt64, flags Array(UInt8)), `with.dot` UInt32),
    attrs Map(UInt64, Float64),
    pair Tuple(UInt64, Float64),
    CODEC payload.ts (DoubleDelta, ZSTD(1)),
    CODEC `payload`.`value` (Gorilla, ZSTD(1)),
    CODEC payload.meta (Delta, ZSTD(3)),
    CODEC `payload.with.dot` (T64, LZ4HC(4)),
    CODEC attrs.values (Gorilla, LZ4HC(4)),
    CODEC `pair`.`2` (ZSTD(5)),
    INDEX idx_seq payload.meta.seq TYPE minmax GRANULARITY 1,
    CONSTRAINT c_value CHECK isFinite(payload.value)
)
ENGINE = MergeTree ORDER BY (id, payload.ts)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4',
    replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1,
    map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';

SELECT 'create query';
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs';

SELECT 'ast json round trip';
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (p Tuple(a UInt64), CODEC p.a (ZSTD)) ENGINE = MergeTree ORDER BY tuple()'));

SELECT 'system.columns';
SELECT name, compression_codec, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table = 't_subcolumn_codecs' ORDER BY position;

SELECT 'describe';
DESCRIBE TABLE t_subcolumn_codecs SETTINGS describe_include_subcolumns = 1;

INSERT INTO t_subcolumn_codecs SELECT number, (toDateTime64('2026-01-01 00:00:00', 3) + number, number / 7, (number, [1, 2]), number), map(number, number / 3), (number, number / 5) FROM numbers(1000);
INSERT INTO t_subcolumn_codecs SELECT number, (toDateTime64('2026-01-01 00:00:00', 3) + number, number / 7, (number, [1, 2]), number), map(number, number / 3), (number, number / 5) FROM numbers(1000, 1000);
-- The streams are listed for the part written by the merge.
OPTIMIZE TABLE t_subcolumn_codecs FINAL;

-- Every stream is compressed with the codec declared for its subcolumn or with the default codec of the part.
-- Structural streams (flags.size0, attrs.size0) get only the generic part of the codec.
SELECT 'streams';
SELECT column, substream, mapKeys(codec_block_counts) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_subcolumn_codecs)
WHERE column IN ('payload', 'attrs', 'pair') ORDER BY column, substream;

SELECT 'data';
SELECT count(), sum(payload.meta.seq), round(sum(payload.value)), sum(length(payload.meta.flags)), round(sum(attrs[id])), round(sum(pair.2)) FROM t_subcolumn_codecs;

DROP TABLE t_subcolumn_codecs;

-- A declaration for a column is the same as the inline clause. A column named `codec` is a column, also with a
-- parametrized type: the brackets of a declaration hold codecs, not type arguments.
SELECT 'column form';
CREATE TABLE t_subcolumn_codecs_column (id UInt64, s String, codec Nullable(String), codec_params Array(UInt8) DEFAULT [], CODEC id (ZSTD(3)), CODEC s (LZ4HC(4))) ENGINE = MergeTree ORDER BY id;
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_column';
DROP TABLE t_subcolumn_codecs_column;

-- Flattened Nested: n.a is a column. Not flattened: n.a is a subcolumn of n.
SELECT 'nested';
CREATE TABLE t_subcolumn_codecs_nested (n Nested(a UInt64, b String), CODEC n.a (Delta, LZ4)) ENGINE = MergeTree ORDER BY tuple();
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_nested';
DROP TABLE t_subcolumn_codecs_nested;

SET flatten_nested = 0;
CREATE TABLE t_subcolumn_codecs_nested (n Nested(a UInt64, b String), CODEC n.a (Delta, LZ4)) ENGINE = MergeTree ORDER BY tuple();
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_nested';
DROP TABLE t_subcolumn_codecs_nested;
SET flatten_nested = 1;

-- A column with the same name as a subcolumn of another column wins, as in a query.
SELECT 'column named like a subcolumn';
CREATE TABLE t_subcolumn_codecs_shadow (n Tuple(a UInt64, b String), `n.a` Map(String, UInt64), CODEC n.a (ZSTD(3))) ENGINE = MergeTree ORDER BY tuple();
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_shadow';
DROP TABLE t_subcolumn_codecs_shadow;

-- With buckets the streams of a Map carry the bucket number, which is skipped when a stream is matched against the declarations.
SELECT 'map with buckets';
CREATE TABLE t_subcolumn_codecs_map (m Map(UInt64, Float64), CODEC m.values (Gorilla, ZSTD(1))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4',
    replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 2, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 1, serialization_info_version = 'with_types';
INSERT INTO t_subcolumn_codecs_map SELECT map(number, number / 7, number + 1000, number / 3) FROM numbers(1000);
SELECT substream, mapKeys(codec_block_counts) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_subcolumn_codecs_map) WHERE column = 'm' ORDER BY substream;
SELECT round(sum(m[number])) FROM t_subcolumn_codecs_map ARRAY JOIN range(1000) AS number;
DROP TABLE t_subcolumn_codecs_map;

-- The sparse index of a sparse element and the null map of a Nullable element are structural streams:
-- they follow the declaration of their element with the generic part of its codec.
-- An element name with a dot is one name: `value.b` is a sibling of `value`, not an element inside it.
SELECT 'sparse, null and dotted streams';
CREATE TABLE t_subcolumn_codecs_sparse
(
    id UInt64,
    payload Tuple(value Float64, `value.b` String, note Nullable(String)),
    CODEC payload.value (Gorilla, ZSTD(1)),
    CODEC payload.`value.b` (LZ4HC(4)),
    CODEC payload.note (ZSTD)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4',
    replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 0.9,
    string_serialization_version = 'single_stream', nullable_serialization_version = 'basic';

INSERT INTO t_subcolumn_codecs_sparse SELECT number, (if(number % 100 = 0, number / 7, 0), toString(number), if(number % 2 = 0, NULL, toString(number))) FROM numbers(1000);
SELECT substream, mapKeys(codec_block_counts) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_subcolumn_codecs_sparse) WHERE column = 'payload' ORDER BY substream;
SELECT count(), round(sum(payload.value)), sum(length(payload.`value.b`)), countIf(payload.note IS NULL) FROM t_subcolumn_codecs_sparse;

DROP TABLE t_subcolumn_codecs_sparse;
