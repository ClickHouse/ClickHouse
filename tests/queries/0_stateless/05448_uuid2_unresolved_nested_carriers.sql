-- A `Dynamic` (or a `Variant`) nested in a collection loses the type of its value when the value becomes a `Field`.
-- A nested `Variant` is resolved by its only alternative of the kind of the value; when the type of a nested UUID
-- value cannot be resolved, the conversion to `UUID` / `UUID2` is rejected instead of guessing the layout.
-- The `Field` of a `Dynamic` value keeps its UUID values in the layout of `UUID`, so it is converted by that layout.

SET allow_suspicious_variant_types = 1;

SELECT u FROM values('u Array(UUID1)', [CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Dynamic)]);
SELECT u FROM values('u Array(Tuple(UUID1))', [CAST(tuple(toUUID2('00000000-0000-0000-0000-000000000001')) AS Variant(Tuple(UUID2), String))]);
SELECT u FROM values('u Array(Tuple(UUID2))', [CAST(tuple(toUUID('00000000-0000-0000-0000-000000000001')) AS Variant(Tuple(UUID), String))]);

-- `INSERT ... VALUES` evaluates such a value as a column, which keeps the type.
DROP TABLE IF EXISTS t_uuid2_nested_dynamic;
CREATE TABLE t_uuid2_nested_dynamic (u Array(UUID1)) ENGINE = Memory;
INSERT INTO t_uuid2_nested_dynamic SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES ([CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Dynamic)]);
SELECT u FROM t_uuid2_nested_dynamic;
DROP TABLE t_uuid2_nested_dynamic;

-- A partition is selected by the identifier, not by the layout of the value.
DROP TABLE IF EXISTS t_uuid2_partition_dynamic;
CREATE TABLE t_uuid2_partition_dynamic (u Array(UUID1)) ENGINE = MergeTree PARTITION BY u ORDER BY tuple();
INSERT INTO t_uuid2_partition_dynamic VALUES (['00000000-0000-0000-0000-000000000001']), (['00000000-0000-0001-0000-000000000000']);
ALTER TABLE t_uuid2_partition_dynamic DROP PARTITION tuple([CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Dynamic)]);
SELECT u FROM t_uuid2_partition_dynamic ORDER BY toString(u);
DROP TABLE t_uuid2_partition_dynamic;

-- The bloom filter on `mapValues` does not skip a granule by a value of unknown layout.
DROP TABLE IF EXISTS t_uuid2_map_values_bloom_filter;
CREATE TABLE t_uuid2_map_values_bloom_filter
(
    m Map(String, UUID2),
    INDEX bf mapValues(m) TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_uuid2_map_values_bloom_filter SELECT map('k', toUUID2('00000000-0000-0000-0000-000000000001'));
SELECT count() FROM t_uuid2_map_values_bloom_filter
WHERE m['k'] = CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, String))
SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_uuid2_map_values_bloom_filter
WHERE m['k'] = CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Dynamic)
SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;
DROP TABLE t_uuid2_map_values_bloom_filter;

-- The `IN` list rewrite for a shard keeps a value of unknown layout.
DROP TABLE IF EXISTS t_uuid2_shard_carriers;
CREATE TABLE t_uuid2_shard_carriers (id UUID2) ENGINE = Memory;
INSERT INTO t_uuid2_shard_carriers VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0001-0000-000000000002');
SELECT countIf(_shard_num != reinterpretAsUInt64(id) % 2 + 1), count()
FROM remote('127.{1,2}', currentDatabase(), t_uuid2_shard_carriers, reinterpretAsUInt64(id))
WHERE id IN (
    CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, String)),
    CAST(toUUID('00000000-0000-0001-0000-000000000002') AS Variant(UUID, String)))
SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1, prefer_localhost_replica = 0, enable_analyzer = 1;
SELECT count()
FROM remote('127.{1,2}', currentDatabase(), t_uuid2_shard_carriers, reinterpretAsUInt64(id))
WHERE id IN (
    CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Dynamic),
    CAST(toUUID('00000000-0000-0001-0000-000000000002') AS Dynamic))
SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1, prefer_localhost_replica = 0, enable_analyzer = 1;
DROP TABLE t_uuid2_shard_carriers;
