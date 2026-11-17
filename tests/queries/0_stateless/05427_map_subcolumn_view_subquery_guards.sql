-- Map key lookups pushed through views and projection subqueries must give the same result
-- as reading the full Map.

DROP VIEW IF EXISTS v_shadow;
DROP VIEW IF EXISTS v_alias;
DROP VIEW IF EXISTS v_alias_where;
DROP VIEW IF EXISTS v_fixed;
DROP TABLE IF EXISTS t_shadow;
DROP TABLE IF EXISTS t_fixed;
DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, m Map(String, String)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t VALUES (1, {'k':'v', 'b':'other'});

SET optimize_functions_to_subcolumns = 1;

SELECT 'physical column with the subcolumn name';
CREATE TABLE t_shadow (m Map(String, String), `m.key_k` String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_shadow VALUES ({'k':'correct'}, 'wrong');
CREATE VIEW v_shadow SQL SECURITY INVOKER AS SELECT m FROM t_shadow;
SELECT m['k'] FROM v_shadow;

SELECT 'outer join with join_use_nulls';
SELECT s.m['k'], toTypeName(s.m['k'])
FROM numbers(1) AS n
LEFT JOIN (SELECT id, m FROM t) AS s ON n.number = s.id
SETTINGS join_use_nulls = 1;

SELECT 'outer aggregation';
SELECT m['k'] FROM (SELECT m FROM t) GROUP BY m;
SELECT m FROM (SELECT m FROM t) GROUP BY m HAVING m['k'] = 'v';
SELECT sum(length(m['k'])) FROM (SELECT m FROM t);

SELECT 'correlated subquery';
SELECT m FROM (SELECT m FROM t) AS s
WHERE EXISTS (SELECT 1 FROM numbers(1) WHERE s.m['k'] = toString(number));

SELECT 'alias used in the view body';
CREATE VIEW v_alias SQL SECURITY INVOKER AS SELECT m AS attrs, attrs['b'] AS extra FROM t;
SELECT attrs['k'], extra FROM v_alias;
CREATE VIEW v_alias_where SQL SECURITY INVOKER AS SELECT m AS attrs FROM t WHERE attrs['b'] = 'other';
SELECT attrs['k'] FROM v_alias_where;

SELECT 'FixedString key shorter than its width';
CREATE TABLE t_fixed (m Map(FixedString(3), String), ml Map(LowCardinality(FixedString(3)), String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_fixed VALUES ({'k':'padded', 'abc':'full'}, {'k':'padded', 'abc':'full'});
CREATE VIEW v_fixed SQL SECURITY INVOKER AS SELECT m, ml FROM t_fixed;
SELECT m['k'], ml['k'], m['abc'], ml['abc'], m[toFixedString('k', 3)] FROM v_fixed;
SELECT m['k'], ml['k'], m['abc'], ml['abc'], m[toFixedString('k', 3)] FROM (SELECT m, ml FROM t_fixed);
SELECT m['k'], ml['k'], m['abc'], ml['abc'], m[toFixedString('k', 3)] FROM t_fixed;
SELECT count() FROM v_fixed WHERE m['k'] = 'padded' OR ml['k'] = 'padded';
SELECT count() FROM (SELECT m, ml FROM t_fixed) WHERE m['k'] = 'padded' OR ml['k'] = 'padded';
SELECT count() > 0
FROM (EXPLAIN PLAN actions = 1 SELECT m['abc'] FROM v_fixed)
WHERE explain ILIKE '%Output: m.key_abc%';
SELECT count()
FROM (EXPLAIN PLAN actions = 1 SELECT m['k'], ml['k'] FROM (SELECT m, ml FROM t_fixed))
WHERE explain ILIKE '%key_k%';

DROP VIEW v_shadow;
DROP VIEW v_alias;
DROP VIEW v_alias_where;
DROP VIEW v_fixed;
DROP TABLE t_shadow;
DROP TABLE t_fixed;
DROP TABLE t;
