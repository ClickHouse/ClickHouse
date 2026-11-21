-- A nullable but non-NULL key that is absent from the map yields the default of the map's value type,
-- not NULL, although `arrayElement` makes its result type `Nullable`. The `bloom_filter` index must
-- not prune the rows without the key. Every query is compared with a full scan.

DROP TABLE IF EXISTS t_bf_keys;
DROP TABLE IF EXISTS t_bf_values;
DROP TABLE IF EXISTS t_bf_uint8;
DROP TABLE IF EXISTS t_bf_enum;

CREATE TABLE t_bf_keys (m Map(String, FixedString(3)), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_keys VALUES (map('k', 'abc')), (map());

CREATE TABLE t_bf_values (m Map(String, FixedString(3)), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_values VALUES (map('k', 'abc')), (map());

SELECT 'equals';
SELECT count() FROM t_bf_keys WHERE m[toNullable('absent')] = '\0';
SELECT count() FROM t_bf_keys WHERE m[toNullable('absent')] = '\0' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_keys WHERE m[toNullable('absent')] = '\0' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bf_values WHERE m[toNullable('absent')] = '\0';
SELECT count() FROM t_bf_values WHERE m[toNullable('absent')] = '\0' SETTINGS use_skip_indexes = 0;

SELECT 'in subquery';
SELECT count() FROM t_bf_keys WHERE m[toNullable('absent')] IN (SELECT '\0');
SELECT count() FROM t_bf_keys WHERE m[toNullable('absent')] IN (SELECT '\0') SETTINGS use_skip_indexes = 0;

CREATE TABLE t_bf_uint8 (m Map(String, UInt8), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_uint8 VALUES (map('k', 1)), (map());

SELECT count() FROM t_bf_uint8 WHERE m[toNullable('absent')] IN (SELECT '0');
SELECT count() FROM t_bf_uint8 WHERE m[toNullable('absent')] GLOBAL IN (SELECT '0');
SELECT count() FROM t_bf_uint8 WHERE m[toNullable('absent')] IN (SELECT '0') SETTINGS use_skip_indexes = 0;

SELECT 'enum';
CREATE TABLE t_bf_enum (m Map(String, Enum8('a' = 1, 'b' = 2)), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_enum VALUES (map('k', 'a')), (map());

SELECT count() FROM t_bf_enum WHERE CAST(m[toNullable('absent')], 'Nullable(Int8)') IN (SELECT 0);
SELECT count() FROM t_bf_enum WHERE CAST(m[toNullable('absent')], 'Nullable(Int8)') IN (SELECT 0) SETTINGS use_skip_indexes = 0;

SELECT 'nullable values';
-- A genuinely nullable map value gives NULL for a missing key, which never matches.
DROP TABLE t_bf_keys;
CREATE TABLE t_bf_keys (m Map(String, Nullable(FixedString(3))), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_keys VALUES (map('k', 'abc')), (map());
SELECT count() FROM t_bf_keys WHERE m[toNullable('absent')] = '\0';
SELECT count() FROM t_bf_keys WHERE m[toNullable('absent')] = '\0' SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_keys;
DROP TABLE t_bf_values;
DROP TABLE t_bf_uint8;
DROP TABLE t_bf_enum;
