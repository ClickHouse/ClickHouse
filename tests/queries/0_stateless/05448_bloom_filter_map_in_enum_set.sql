-- The probe of a `bloom_filter` index over a map with the default of a missing key must not throw
-- `UNKNOWN_ELEMENT_OF_ENUM` when the set has an `Enum` element type and the map values are `String`.

DROP TABLE IF EXISTS t_bf_map_enum_set;
CREATE TABLE t_bf_map_enum_set
(
    m Map(String, String),
    INDEX bf_keys mapKeys(m) TYPE bloom_filter GRANULARITY 1,
    INDEX bf_values mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_enum_set VALUES (map('k', 'b')), (map('k', 'a'));

DROP TABLE IF EXISTS t_bf_map_enum_set_source;
CREATE TABLE t_bf_map_enum_set_source (e Enum8('a' = 1, 'b' = 2)) ENGINE = Memory;
INSERT INTO t_bf_map_enum_set_source VALUES ('b');

SELECT count() FROM t_bf_map_enum_set WHERE m['k'] IN (SELECT CAST('b' AS Enum8('a' = 1, 'b' = 2)));
SELECT count() FROM t_bf_map_enum_set WHERE m['k'] IN (SELECT CAST('b' AS Enum8('a' = 1, 'b' = 2))) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_enum_set WHERE m['k'] IN (SELECT CAST('b' AS Enum8('a' = 1, 'b' = 2))) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bf_map_enum_set WHERE m['k'] IN t_bf_map_enum_set_source;
SELECT count() FROM t_bf_map_enum_set WHERE m['k'] GLOBAL IN (SELECT e FROM t_bf_map_enum_set_source);

DROP TABLE t_bf_map_enum_set_source;
DROP TABLE t_bf_map_enum_set;

-- An `Enum` set over `Enum` map values of the same type still uses the index.
DROP TABLE IF EXISTS t_bf_map_enum_values;
CREATE TABLE t_bf_map_enum_values (m Map(String, Enum8('a' = 1, 'b' = 2)), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_enum_values VALUES (map('k', 'b')), (map('k', 'b'));
OPTIMIZE TABLE t_bf_map_enum_values FINAL;

SELECT count() FROM t_bf_map_enum_values WHERE m['k'] IN (SELECT CAST('b' AS Enum8('a' = 1, 'b' = 2)));
SELECT count() FROM t_bf_map_enum_values WHERE m['k'] IN (SELECT CAST('b' AS Enum8('a' = 1, 'b' = 2))) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_map_enum_values;
