SET prefer_outer_table_name_for_correlated_identifier = 0;
SET analyzer_compatibility_prefer_alias_over_subcolumn = 1;

CREATE TABLE squality_matcher_defer_u (x UInt64, t Tuple(z UInt64)) ENGINE = Memory;
INSERT INTO squality_matcher_defer_u VALUES (4, (100)), (5, (200));

-- The opt-in setting must not change the existing tuple-column matcher when disabled.
SELECT x, (SELECT tuple(t.*) FROM numbers(1) LIMIT 1)
FROM squality_matcher_defer_u AS t ORDER BY x;

DROP TABLE squality_matcher_defer_u;

SET prefer_outer_table_name_for_correlated_identifier = 1;
SET allow_correlated_subqueries = 0;

CREATE TABLE squality_matcher_defer_v (v Tuple(x UInt64)) ENGINE = Memory;
INSERT INTO squality_matcher_defer_v VALUES ((1)), ((2)), ((3));

-- The outer tuple is only a qualifier; `REPLACE` removes its correlated field.
SELECT v.v.x, (SELECT v.v.* REPLACE (1 AS x) FROM squality_matcher_defer_v AS i LIMIT 1)
FROM squality_matcher_defer_v AS v ORDER BY v.v.x;

-- `EXCEPT` removes the same field before correlation is registered.
SELECT v.v.x, (SELECT 1, v.v.* EXCEPT x FROM squality_matcher_defer_v AS i LIMIT 1)
FROM squality_matcher_defer_v AS v ORDER BY v.v.x;

SELECT count(), (SELECT v.v.* REPLACE (1 AS x) FROM squality_matcher_defer_v AS i LIMIT 1)
FROM squality_matcher_defer_v AS v;

-- A function alias used as the qualifier must defer correlation in its body too.
SELECT v.v.x, (
    WITH identity(v.v) AS q
    SELECT q.* REPLACE (1 AS x) FROM squality_matcher_defer_v AS i LIMIT 1
)
FROM squality_matcher_defer_v AS v ORDER BY v.v.x;

SELECT count(), (
    WITH identity(v.v) AS q
    SELECT q.* REPLACE (1 AS x) FROM squality_matcher_defer_v AS i LIMIT 1
)
FROM squality_matcher_defer_v AS v;

-- A matcher nested in the alias body must defer its final collector too.
SELECT v.v.x, (
    WITH CAST(tuple(v.v.*), 'Tuple(x UInt64)') AS q
    SELECT q.* REPLACE (1 AS x) FROM squality_matcher_defer_v AS i LIMIT 1
)
FROM squality_matcher_defer_v AS v ORDER BY v.v.x;

SELECT count(), (
    WITH CAST(tuple(v.v.*), 'Tuple(x UInt64)') AS q
    SELECT q.* REPLACE (1 AS x) FROM squality_matcher_defer_v AS i LIMIT 1
)
FROM squality_matcher_defer_v AS v;

SET allow_correlated_subqueries = 1;
SET enable_identifier_resolve_cache = 1;

-- The deferred alias lookup must not cache an unregistered outer reference.
SELECT v.v.x, (
    WITH identity(v.v) AS q
    SELECT tuple(q.* REPLACE (1 AS x), v.v)
    FROM squality_matcher_defer_v AS i LIMIT 1
)
FROM squality_matcher_defer_v AS v ORDER BY v.v.x;

DROP TABLE squality_matcher_defer_v;

SET prefer_outer_table_name_for_correlated_identifier = 0;
CREATE TABLE squality_matcher_defer_scalar (x UInt64) ENGINE = Memory;
INSERT INTO squality_matcher_defer_scalar VALUES (1), (2), (3);

-- A scalar subquery inside the alias keeps its own correlation state.
WITH (SELECT CAST(tuple(s.x AS x), 'Tuple(x UInt64)')) AS q
SELECT x, q.* FROM squality_matcher_defer_scalar AS s ORDER BY x;

WITH (SELECT CAST(tuple(s.x AS x), 'Tuple(x UInt64)')) AS q
SELECT x, q.* REPLACE (1 AS x) FROM squality_matcher_defer_scalar AS s ORDER BY x;

DROP TABLE squality_matcher_defer_scalar;
