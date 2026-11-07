DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_lc;
DROP VIEW IF EXISTS v_enum;
DROP TABLE IF EXISTS t_enum;

CREATE TABLE t (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t VALUES (1, {'k':7}), (2, {});

SELECT m[toNullable('k')], toTypeName(m[toNullable('k')]) FROM (SELECT id, m FROM t)
ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT m[toNullable('k')], toTypeName(m[toNullable('k')]) FROM (SELECT id, m FROM t)
ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT sum(m[toNullable('k')]), toTypeName(sum(m[toNullable('k')])) FROM (SELECT m FROM t)
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(m[toNullable('k')]), toTypeName(sum(m[toNullable('k')])) FROM (SELECT m FROM t)
SETTINGS optimize_functions_to_subcolumns = 1;

CREATE TABLE t_lc (id UInt64, m Map(String, LowCardinality(String))) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_lc VALUES (1, {'k':'value'}), (2, {});

SELECT m['k'], toTypeName(m['k']) FROM (SELECT id, m FROM t_lc)
ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT m['k'], toTypeName(m['k']) FROM (SELECT id, m FROM t_lc)
ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT sum(m[toNullable('k')]) FROM (SELECT m FROM t)
    SETTINGS optimize_functions_to_subcolumns = 1, query_plan_remove_unused_columns = 1
)
WHERE explain ILIKE '%Output: m.key_k%';

ALTER TABLE t MODIFY SETTING map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
SELECT count() = 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT sum(m[toNullable('k')]) FROM (SELECT m FROM t)
    SETTINGS optimize_functions_to_subcolumns = 1
)
WHERE explain ILIKE '%m.key_k%';

DROP TABLE t_lc;
DROP TABLE t;

CREATE TABLE t_enum
(
    id UInt64,
    m8 Map(String, Enum8('first' = 1, 'other' = 2)),
    m16 Map(String, Enum16('first' = 1, 'other' = 2)),
    m_nullable Map(String, Nullable(Enum8('first' = 1, 'other' = 2))),
    m_tuple Map(String, Tuple(Enum8('first' = 1, 'other' = 2), String))
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_enum VALUES (1, map('k', 'other'), map('k', 'other'), map('k', 'other'), map('k', tuple('other', 'value')));
CREATE VIEW v_enum SQL SECURITY INVOKER AS SELECT id, m8, m16, m_nullable, m_tuple FROM t_enum;

SELECT n.number, toInt8(s.m8['k']), toInt16(s.m16['k']),
    toInt8(s.m_nullable['k']), toInt8(tupleElement(s.m_tuple['k'], 1))
FROM numbers(2) AS n
LEFT JOIN (SELECT id, m8, m16, m_nullable, m_tuple FROM t_enum) AS s ON n.number = s.id
ORDER BY n.number SETTINGS join_use_nulls = 0, optimize_functions_to_subcolumns = 0;
SELECT n.number, toInt8(s.m8['k']), toInt16(s.m16['k']),
    toInt8(s.m_nullable['k']), toInt8(tupleElement(s.m_tuple['k'], 1))
FROM numbers(2) AS n
LEFT JOIN (SELECT id, m8, m16, m_nullable, m_tuple FROM t_enum) AS s ON n.number = s.id
ORDER BY n.number SETTINGS join_use_nulls = 0, optimize_functions_to_subcolumns = 1;

SELECT n.number
FROM numbers(2) AS n
LEFT JOIN (SELECT id, m8 FROM t_enum) AS s ON n.number = s.id
WHERE toInt8(s.m8['k']) = 0
ORDER BY n.number SETTINGS join_use_nulls = 0, optimize_functions_to_subcolumns = 0;
SELECT n.number
FROM numbers(2) AS n
LEFT JOIN (SELECT id, m8 FROM t_enum) AS s ON n.number = s.id
WHERE toInt8(s.m8['k']) = 0
ORDER BY n.number SETTINGS join_use_nulls = 0, optimize_functions_to_subcolumns = 1;

SELECT n.number, toInt8(s.m8['k']), toInt16(s.m16['k']),
    toInt8(s.m_nullable['k']), toInt8(tupleElement(s.m_tuple['k'], 1))
FROM numbers(2) AS n LEFT JOIN v_enum AS s ON n.number = s.id
ORDER BY n.number SETTINGS join_use_nulls = 0, optimize_functions_to_subcolumns = 0;
SELECT n.number, toInt8(s.m8['k']), toInt16(s.m16['k']),
    toInt8(s.m_nullable['k']), toInt8(tupleElement(s.m_tuple['k'], 1))
FROM numbers(2) AS n LEFT JOIN v_enum AS s ON n.number = s.id
ORDER BY n.number SETTINGS join_use_nulls = 0, optimize_functions_to_subcolumns = 1;

DROP VIEW v_enum;
DROP TABLE t_enum;
