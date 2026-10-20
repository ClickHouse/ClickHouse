-- `if` with a non-constant condition and mixed `UUID` / `UUID2` branches casts the selected branch.
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT number, toString(if(number = 0, toUUID(s), toUUID2(s))), toString(if(number = 0, toUUID2(s), toUUID(s)))
FROM numbers(2);

WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT number, arrayMap(x -> toString(x), if(number = 0, [toUUID(s)], [toUUID2(s)]))
FROM numbers(2);

-- `indexOfAssumeSorted` with a constant haystack whose order changes with the cast to the common layout.
WITH
    '00000000-0000-0001-0000-000000000000' AS a,
    '00000000-0000-0000-0000-000000000001' AS b,
    [tuple(toUUID(a)), tuple(toUUID(b))] AS sorted_legacy,
    tuple(toUUID2(a)) AS needle
SELECT
    indexOfAssumeSorted(sorted_legacy, needle),
    indexOfAssumeSorted(materialize(sorted_legacy), needle);

SET allow_suspicious_variant_types = 1;

-- A bloom filter index must not skip granules for a `Variant` / `Dynamic` constant of the other UUID layout.
DROP TABLE IF EXISTS t_uuid2_bf;
CREATE TABLE t_uuid2_bf (u UUID2, INDEX bf u TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_uuid2_bf VALUES ('00000000-0000-0000-0000-000000000001');
SELECT count() FROM t_uuid2_bf
WHERE u = CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, String))
SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_uuid2_bf
WHERE u = CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Dynamic)
SETTINGS use_skip_indexes = 1;
DROP TABLE t_uuid2_bf;

-- A map subscript with a `Variant` / `Dynamic` key gives the same result with and without the subcolumn rewrite.
DROP TABLE IF EXISTS t_uuid2_map;
CREATE TABLE t_uuid2_map (m Map(UUID2, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_uuid2_map SELECT map(toUUID2('00000000-0000-0000-0000-000000000001'), 7);
SELECT m[CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, String))] FROM t_uuid2_map SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m[CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID, String))] FROM t_uuid2_map SETTINGS optimize_functions_to_subcolumns = 0;
SELECT m[CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Dynamic)] FROM t_uuid2_map SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_uuid2_map;

-- A partition value whose whole tuple is carried by a `Variant` uses the layout of the active tuple element.
DROP TABLE IF EXISTS t_uuid2_part;
CREATE TABLE t_uuid2_part (u UUID1) ENGINE = MergeTree PARTITION BY u ORDER BY u;
INSERT INTO t_uuid2_part VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0001-0000-000000000000');
ALTER TABLE t_uuid2_part DROP PARTITION CAST(tuple(toUUID2('00000000-0000-0000-0000-000000000001')) AS Variant(Tuple(UUID2), String));
SELECT toString(u) FROM t_uuid2_part ORDER BY u;
DROP TABLE t_uuid2_part;

DROP TABLE IF EXISTS t_uuid2_part2;
CREATE TABLE t_uuid2_part2 (u UUID1, k UInt8) ENGINE = MergeTree PARTITION BY (u, k) ORDER BY u;
INSERT INTO t_uuid2_part2 VALUES ('00000000-0000-0000-0000-000000000001', 1), ('00000000-0000-0001-0000-000000000000', 1);
ALTER TABLE t_uuid2_part2 DROP PARTITION CAST(tuple(toUUID2('00000000-0000-0000-0000-000000000001'), 1) AS Variant(Tuple(UUID2, UInt8), String));
SELECT toString(u) FROM t_uuid2_part2 ORDER BY u;
DROP TABLE t_uuid2_part2;
