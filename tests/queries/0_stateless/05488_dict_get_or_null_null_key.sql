-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/66294
-- dictGetOrNull with a NULL key must return NULL (like dictGet and dictHas) instead of throwing.

DROP DICTIONARY IF EXISTS d_66294_flat;
DROP DICTIONARY IF EXISTS d_66294_hashed;
DROP TABLE IF EXISTS t_66294;

CREATE TABLE t_66294 (id UInt64, value Float64, name String) ENGINE = Memory;
-- Key 0 is present on purpose: a NULL key must not be looked up as the nested default 0.
INSERT INTO t_66294 VALUES (0, 0.5, 'zero'), (5, 1.5, 'five');

CREATE DICTIONARY d_66294_flat (id UInt64, value Float64, name String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_66294' DB currentDatabase()))
LAYOUT(FLAT()) LIFETIME(0);

CREATE DICTIONARY d_66294_hashed (id UInt64, value Float64, name String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_66294' DB currentDatabase()))
LAYOUT(HASHED()) LIFETIME(0);

SELECT 'const NULL key';
SELECT dictGetOrNull('d_66294_flat', 'value', NULL), toTypeName(dictGetOrNull('d_66294_flat', 'value', NULL));
SELECT dictGetOrNull('d_66294_flat', 'value', CAST(NULL, 'Nullable(UInt64)'));
SELECT dictGetOrNull('d_66294_hashed', 'name', NULL), toTypeName(dictGetOrNull('d_66294_hashed', 'name', NULL));

SELECT 'materialized NULL key';
SELECT dictGetOrNull('d_66294_flat', 'value', materialize(CAST(NULL, 'Nullable(UInt64)')));
SELECT dictGetOrNull('d_66294_hashed', 'value', materialize(CAST(NULL, 'Nullable(UInt64)')));

SELECT 'Nullable key column, flat';
SELECT k, dictGetOrNull('d_66294_flat', 'value', k) AS v, toTypeName(v), dictGetOrNull('d_66294_flat', 'name', k)
FROM (SELECT arrayJoin([0, 5, NULL, 7]::Array(Nullable(UInt64))) AS k);

SELECT 'Nullable key column, hashed';
SELECT k, dictGetOrNull('d_66294_hashed', 'value', k) AS v, toTypeName(v), dictGetOrNull('d_66294_hashed', 'name', k)
FROM (SELECT arrayJoin([0, 5, NULL, 7]::Array(Nullable(UInt64))) AS k);

SELECT 'Nullable key column without NULLs';
SELECT dictGetOrNull('d_66294_flat', 'value', toNullable(toUInt64(5)));

-- Like dictGet, a Nullable key is supported only for a single attribute.
SELECT dictGetOrNull('d_66294_flat', ('value', 'name'), NULL); -- { serverError UNSUPPORTED_METHOD }

DROP DICTIONARY d_66294_hashed;
DROP DICTIONARY d_66294_flat;
DROP TABLE t_66294;
