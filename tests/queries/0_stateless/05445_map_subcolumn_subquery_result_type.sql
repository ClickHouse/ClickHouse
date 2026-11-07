DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_lc;

CREATE TABLE t (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, {'k':7}), (2, {});

SELECT m[toNullable('k')], toTypeName(m[toNullable('k')]) FROM (SELECT id, m FROM t)
ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT m[toNullable('k')], toTypeName(m[toNullable('k')]) FROM (SELECT id, m FROM t)
ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT sum(m[toNullable('k')]), toTypeName(sum(m[toNullable('k')])) FROM (SELECT m FROM t)
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(m[toNullable('k')]), toTypeName(sum(m[toNullable('k')])) FROM (SELECT m FROM t)
SETTINGS optimize_functions_to_subcolumns = 1;

CREATE TABLE t_lc (id UInt64, m Map(String, LowCardinality(String))) ENGINE = MergeTree ORDER BY id;
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

DROP TABLE t_lc;
DROP TABLE t;